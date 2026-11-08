use std::sync::{Arc, LazyLock};
use std::time::Duration;

use opentelemetry::logs::{AnyValue, LogRecord as _};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{InstrumentationScope, KeyValue, global};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{ExporterBuildError, LogExporter, MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::logs::{LogProcessor, SdkLogRecord, SdkLoggerProvider};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use regex::Regex;
use sentry::integrations::tracing::{BreadcrumbLayer, ErrorLayer};
use sentry::protocol::Event;
use serde_json::Value;
use tracing::Level;
use tracing_subscriber::filter::{LevelFilter, filter_fn};
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

const FILTERED: &str = "[Filtered]";

/// Metrics leave for the collector every half minute; an alert looks at a few minutes.
const METRIC_INTERVAL: Duration = Duration::from_secs(30);

/// Targets whose events stay in the container log: the exporters' own, and the HTTP client they send with, would
/// loop through the exporter that is failing.
const LOCAL_ONLY_TARGETS: [&str; 5] = ["opentelemetry", "reqwest", "hyper", "h2", "tower"];

const PII_KEY_WORDS: [&str; 14] = [
    "phone",
    "otp",
    "lat",
    "lng",
    "lon",
    "latitude",
    "longitude",
    "location",
    "coordinates",
    "address",
    "plate",
    "plates",
    "passport",
    "waypoint",
];

const PII_KEYS: [&str; 2] = ["code", "point"];

static PATTERNS: LazyLock<Vec<(Regex, &'static str)>> = LazyLock::new(|| {
    [
        (r"%2[Bb]998\d{9}\b", FILTERED),
        (
            r"\+?\b998[\s-]?\(?\d{2}\)?[\s-]?\d{3}[\s-]?\d{2}[\s-]?\d{2}\b",
            FILTERED,
        ),
        (r"\+\d{10,14}\b", FILTERED),
        (r"-?\d{1,3}\.\d{4,}\s*,\s*-?\d{1,3}\.\d{4,}", FILTERED),
        (
            r#"(?i)\b((?:\w+_)?(?:lat|lng|lon|latitude|longitude)["']?\s*[:=]\s*)-?\d+(?:\.\d+)?"#,
            "${1}[Filtered]",
        ),
    ]
    .into_iter()
    .map(|(pattern, replacement)| (Regex::new(pattern).expect("valid PII pattern"), replacement))
    .collect()
});

/// Keeps error reporting and the OTLP pipelines alive; dropping it at the end of `main` flushes what is left.
pub struct Telemetry {
    _sentry: Option<sentry::ClientInitGuard>,
    otel: Option<Providers>,
}

struct Providers {
    tracer: SdkTracerProvider,
    logger: SdkLoggerProvider,
    meter: SdkMeterProvider,
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        if let Some(otel) = self.otel.take() {
            let _ = otel.tracer.shutdown();
            let _ = otel.logger.shutdown();
            let _ = otel.meter.shutdown();
        }
    }
}

/// Every log record bound for the collector gets the scrubbing an error report gets.
#[derive(Debug)]
struct ScrubLogs;

impl LogProcessor for ScrubLogs {
    fn emit(&self, record: &mut SdkLogRecord, _scope: &InstrumentationScope) {
        if let Some(AnyValue::String(body)) = record.body() {
            let clean = scrub_text(body.as_str());
            if clean != body.as_str() {
                record.set_body(AnyValue::String(clean.into()));
            }
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }
}

/// Logs to stdout always; to GlitchTip with a DSN; traces, logs and metrics over OTLP/HTTP with an endpoint (the
/// collector's base URL). Spans also carry their context across NATS (see `events`), so a request's trace continues
/// in the services that consume what it published.
pub fn init(
    service_name: &str,
    environment: &str,
    glitchtip_dsn: Option<&str>,
    otlp_endpoint: Option<&str>,
) -> Telemetry {
    let otel = otlp_endpoint
        .filter(|endpoint| !endpoint.is_empty())
        .and_then(|endpoint| match providers(service_name, environment, endpoint) {
            Ok(providers) => Some(providers),
            Err(err) => {
                eprintln!("OTLP export disabled: {err}");
                None
            }
        });
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let exported = || {
        filter_fn(|meta| {
            !LOCAL_ONLY_TARGETS
                .iter()
                .any(|target| meta.target().starts_with(target))
        })
    };
    let traces = otel.as_ref().map(|providers| {
        tracing_opentelemetry::layer()
            .with_tracer(providers.tracer.tracer("goway"))
            .with_filter(exported())
    });
    let logs = otel
        .as_ref()
        .map(|providers| OpenTelemetryTracingBridge::new(&providers.logger).with_filter(exported()));
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(BreadcrumbLayer::new().with_filter(filter_fn(|meta| matches!(*meta.level(), Level::WARN | Level::INFO))))
        .with(ErrorLayer::new().with_filter(LevelFilter::ERROR))
        .with(traces)
        .with(logs)
        .init();
    if let Some(providers) = &otel {
        global::set_tracer_provider(providers.tracer.clone());
        global::set_meter_provider(providers.meter.clone());
        global::set_text_map_propagator(TraceContextPropagator::new());
        tracing::info!(service = service_name, "OTLP export enabled");
    }
    Telemetry {
        _sentry: init_glitchtip(service_name, environment, glitchtip_dsn),
        otel,
    }
}

fn providers(service_name: &str, environment: &str, endpoint: &str) -> Result<Providers, ExporterBuildError> {
    let base = endpoint.trim_end_matches('/');
    let resource = Resource::builder()
        .with_service_name(service_name.to_owned())
        .with_attribute(KeyValue::new("deployment.environment.name", environment.to_owned()))
        .build();
    let spans = SpanExporter::builder()
        .with_http()
        .with_endpoint(format!("{base}/v1/traces"))
        .build()?;
    let logs = LogExporter::builder()
        .with_http()
        .with_endpoint(format!("{base}/v1/logs"))
        .build()?;
    let metrics = MetricExporter::builder()
        .with_http()
        .with_endpoint(format!("{base}/v1/metrics"))
        .build()?;
    Ok(Providers {
        tracer: SdkTracerProvider::builder()
            .with_resource(resource.clone())
            .with_batch_exporter(spans)
            .build(),
        logger: SdkLoggerProvider::builder()
            .with_resource(resource.clone())
            .with_log_processor(ScrubLogs)
            .with_batch_exporter(logs)
            .build(),
        meter: SdkMeterProvider::builder()
            .with_resource(resource)
            .with_reader(PeriodicReader::builder(metrics).with_interval(METRIC_INTERVAL).build())
            .build(),
    })
}

fn init_glitchtip(
    service_name: &str,
    environment: &str,
    glitchtip_dsn: Option<&str>,
) -> Option<sentry::ClientInitGuard> {
    let dsn = glitchtip_dsn.filter(|dsn| !dsn.is_empty())?;
    let mut options = sentry::ClientOptions::default();
    options.release = sentry::release_name!();
    options.environment = Some(environment.to_owned().into());
    options.server_name = Some(service_name.to_owned().into());
    options.send_default_pii = false;
    options.before_send = Some(Arc::new(scrub_event));
    let guard = sentry::init((dsn, options));
    tracing::info!(service = service_name, "glitchtip reporting enabled");
    Some(guard)
}

pub fn scrub_event(event: Event<'static>) -> Option<Event<'static>> {
    let value = serde_json::to_value(&event).ok()?;
    serde_json::from_value(scrub_value(value, 0)).ok()
}

fn scrub_value(value: Value, depth: usize) -> Value {
    if depth > 32 {
        return value;
    }
    match value {
        Value::String(text) => Value::String(scrub_text(&text)),
        Value::Array(items) => Value::Array(items.into_iter().map(|item| scrub_value(item, depth + 1)).collect()),
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, item)| {
                    let blank = item.is_null() || item.as_str().is_some_and(str::is_empty);
                    let item = if is_pii_key(&key) && !blank {
                        Value::String(FILTERED.to_owned())
                    } else {
                        scrub_value(item, depth + 1)
                    };
                    (key, item)
                })
                .collect(),
        ),
        other => other,
    }
}

fn scrub_text(text: &str) -> String {
    PATTERNS.iter().fold(text.to_owned(), |text, (pattern, replacement)| {
        pattern.replace_all(&text, *replacement).into_owned()
    })
}

fn is_pii_key(key: &str) -> bool {
    PII_KEYS.iter().any(|name| key.eq_ignore_ascii_case(name))
        || key_words(key).iter().any(|word| PII_KEY_WORDS.contains(&word.as_str()))
}

fn key_words(key: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut after_lower = false;
    for c in key.chars() {
        if matches!(c, '-' | '_' | '.') || c.is_whitespace() {
            words.extend((!word.is_empty()).then(|| std::mem::take(&mut word)));
            after_lower = false;
            continue;
        }
        if c.is_uppercase() && after_lower {
            words.push(std::mem::take(&mut word));
        }
        after_lower = c.is_lowercase() || c.is_ascii_digit();
        word.extend(c.to_lowercase());
    }
    words.extend((!word.is_empty()).then_some(word));
    words
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use sentry::protocol::{Breadcrumb, Event, Exception, Level};
    use serde_json::json;

    use super::*;

    fn event(message: &str, extra: Value) -> Event<'static> {
        let Value::Object(extra) = extra else {
            panic!("extra must be an object")
        };
        Event {
            message: Some(message.to_owned()),
            level: Level::Error,
            extra: extra.into_iter().collect::<BTreeMap<_, _>>(),
            exception: vec![Exception {
                ty: "DispatchError".into(),
                value: Some("driver +998 90 123-45-67 at 41.311081, 69.240562".into()),
                ..Default::default()
            }]
            .into(),
            breadcrumbs: vec![Breadcrumb {
                message: Some("offer to +79161234567".into()),
                ..Default::default()
            }]
            .into(),
            ..Default::default()
        }
    }

    #[test]
    fn phones_and_coordinates_leave_every_text() {
        let scrubbed = scrub_event(event("no driver for +998901234567 near lat=41.31", json!({}))).unwrap();
        let text = serde_json::to_string(&scrubbed).unwrap();
        for leaked in ["998", "7916", "41.31", "69.24"] {
            assert!(!text.contains(leaked), "{leaked} leaked: {text}");
        }
        assert_eq!(scrubbed.level, Level::Error);
        assert_eq!(scrubbed.exception.values[0].ty, "DispatchError");
    }

    #[test]
    fn personal_fields_are_replaced_by_name_and_the_rest_kept() {
        let extra = json!({
            "phone": "+998901234567", "driverPhone": "+998901234567", "from_lat": 41.31, "code": "4821",
            "status_code": 409, "load_code": "L-1042", "platform": "android", "empty_phone": null,
        });
        let scrubbed = scrub_event(event("boom", extra)).unwrap().extra;
        for key in ["phone", "driverPhone", "from_lat", "code"] {
            assert_eq!(scrubbed[key], json!(FILTERED), "{key}");
        }
        assert_eq!(scrubbed["status_code"], json!(409));
        assert_eq!(scrubbed["load_code"], json!("L-1042"));
        assert_eq!(scrubbed["platform"], json!("android"));
        assert_eq!(scrubbed["empty_phone"], Value::Null);
    }

    #[test]
    fn ids_and_amounts_survive() {
        let text = "booking 0199f3a2-7c1e-7d4b-9a10-3f2e5c6b7a81 trace 4bf92f3577b34da6a3ce929d0e0e4736 amount 1500000";
        assert_eq!(scrub_text(text), text);
        assert_eq!(scrub_text("q=%2B998901234567&lang=uz"), "q=[Filtered]&lang=uz");
    }

    #[test]
    fn key_words_split_on_separators_and_camel_case() {
        assert_eq!(key_words("pickupAddress"), ["pickup", "address"]);
        assert_eq!(key_words("X-Driver_phone.number"), ["x", "driver", "phone", "number"]);
        assert_eq!(key_words("platform"), ["platform"]);
    }

    #[test]
    fn a_log_bound_for_the_collector_loses_its_phone_and_coordinates() {
        use opentelemetry::logs::{Logger as _, LoggerProvider as _};
        use opentelemetry_sdk::logs::InMemoryLogExporter;

        let exporter = InMemoryLogExporter::default();
        let provider = SdkLoggerProvider::builder()
            .with_log_processor(ScrubLogs)
            .with_simple_exporter(exporter.clone())
            .build();
        let logger = provider.logger("test");
        let mut record = logger.create_log_record();
        record.set_body(AnyValue::String(
            "driver +998 90 123 45 67 at 41.311081, 69.240562".into(),
        ));
        logger.emit(record);
        let logs = exporter.get_emitted_logs().expect("emitted");
        assert_eq!(
            logs[0].record.body(),
            Some(&AnyValue::String("driver [Filtered] at [Filtered]".into()))
        );
    }
}
