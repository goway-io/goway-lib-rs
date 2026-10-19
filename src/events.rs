use std::collections::{BTreeSet, HashMap};
use std::str::FromStr;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_nats::jetstream::consumer::pull;
use async_nats::jetstream::{Context, stream};
use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::{KeyValue, global};
use serde_json::Value;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub const STREAM: &str = "GOWAY";

/// How long the stream keeps an event: a week, the value goway-lib's Python publisher keeps too, or the two would
/// fight over it. Consumers ack within seconds; the window is for a worker that is down.
pub const STREAM_MAX_AGE: Duration = Duration::from_secs(7 * 86_400);
pub const MAX_DELIVER: i64 = 5;
pub const NAK_DELAY: Duration = Duration::from_secs(5);
pub const ACK_WAIT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

pub type HandlerResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
pub type Handler = Arc<dyn Fn(String, Value) -> BoxFuture<'static, HandlerResult> + Send + Sync>;

pub async fn connect(nats_url: &str, subjects: &[&str]) -> Result<Context, Box<dyn std::error::Error + Send + Sync>> {
    let mut options = async_nats::ConnectOptions::new()
        .connection_timeout(CONNECT_TIMEOUT)
        .retry_on_initial_connect();

    // async-nats builds the CONNECT frame purely from ConnectOptions.auth; it
    // parses user:pass out of the URL into ServerAddr but never applies it. So
    // a URL like nats://user:pass@host:4222 authenticates as anonymous and the
    // server answers "authorization violation". Lift the credentials across
    // explicitly, reusing the client's own parser so percent-encoding is
    // handled the same way.
    if let Ok(addr) = async_nats::ServerAddr::from_str(nats_url)
        && let Some(user) = addr.username()
    {
        options = options.user_and_password(user.to_owned(), addr.password().unwrap_or_default().to_owned());
    }

    let client = options.connect(nats_url).await?;
    let js = async_nats::jetstream::new(client);
    ensure_stream(&js, subjects).await?;
    Ok(js)
}

async fn ensure_stream(js: &Context, subjects: &[&str]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let desired: BTreeSet<String> = subjects.iter().map(|subject| (*subject).to_owned()).collect();
    match js.get_stream(STREAM).await {
        Ok(mut existing) => {
            let mut config = existing.info().await?.config.clone();
            let current: BTreeSet<String> = config.subjects.iter().cloned().collect();
            let merged: BTreeSet<String> = current.union(&desired).cloned().collect();
            if merged != current || config.max_age != STREAM_MAX_AGE {
                config.subjects = merged.into_iter().collect();
                config.max_age = STREAM_MAX_AGE;
                js.update_stream(config.clone()).await?;
                tracing::info!(stream = STREAM, subjects = ?config.subjects, max_age = ?STREAM_MAX_AGE, "stream set");
            }
        }
        Err(_) => {
            js.create_stream(stream::Config {
                name: STREAM.to_owned(),
                subjects: desired.into_iter().collect(),
                max_age: STREAM_MAX_AGE,
                ..Default::default()
            })
            .await?;
        }
    }
    Ok(())
}

/// Time-ordered (UUIDv7) like every Python publisher's, so ids sort by when they were made across both runtimes.
pub fn stamp_event_id(payload: &mut Value) {
    if let Some(object) = payload.as_object_mut()
        && !object.contains_key("event_id")
    {
        object.insert("event_id".into(), Value::String(uuid::Uuid::now_v7().to_string()));
    }
}

/// The event's id rides as `Nats-Msg-Id` too, so JetStream drops a second publish of the same event (a retry after
/// a lost ack) inside the stream's duplicate window instead of storing it twice.
pub async fn publish(
    js: &Context,
    subject: &str,
    payload: &Value,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut message = payload.clone();
    stamp_event_id(&mut message);
    let mut headers = async_nats::HeaderMap::new();
    if let Some(event_id) = message.get("event_id").and_then(Value::as_str) {
        headers.insert("Nats-Msg-Id", event_id);
    }
    let context = tracing::Span::current().context();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut HeaderInjector(&mut headers))
    });
    let ack = js
        .publish_with_headers(subject.to_owned(), headers, message.to_string().into())
        .await?;
    ack.await?;
    Ok(())
}

/// The same measures goway-lib's Python consumers report, under the same names, so one dashboard covers both runtimes.
struct ConsumerMetrics {
    processed: Counter<u64>,
    duration: Histogram<f64>,
    lag: Histogram<f64>,
}

static METRICS: LazyLock<ConsumerMetrics> = LazyLock::new(|| {
    let meter = global::meter("goway.events");
    meter
        .u64_observable_gauge("goway.consumer.pending")
        .with_unit("{message}")
        .with_description("Messages a durable consumer has not been delivered yet")
        .with_callback(|gauge| {
            if let Ok(pending) = PENDING.lock() {
                for (durable, count) in pending.iter() {
                    gauge.observe(*count, &[KeyValue::new("durable", durable.clone())]);
                }
            }
        })
        .build();
    ConsumerMetrics {
        processed: meter
            .u64_counter("goway.consumer.processed")
            .with_unit("{message}")
            .with_description("Deliveries handled, by outcome")
            .build(),
        duration: meter
            .f64_histogram("goway.consumer.duration")
            .with_unit("s")
            .with_description("Time a handler took for one delivery")
            .build(),
        lag: meter
            .f64_histogram("goway.consumer.lag")
            .with_unit("s")
            .with_description("Time an event waited in the stream before this delivery")
            .build(),
    }
});

/// What each durable has not been delivered yet, as JetStream reported with its latest delivery.
static PENDING: LazyLock<Mutex<HashMap<String, u64>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

struct HeaderInjector<'a>(&'a mut async_nats::HeaderMap);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key, value.as_str());
    }
}

struct HeaderExtractor<'a>(&'a async_nats::HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|value| value.as_str())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.iter().map(|(name, _)| name.as_ref()).collect()
    }
}

fn sanitize(subject: &str) -> String {
    subject.replace('.', "-").replace('>', "all").replace('*', "any")
}

pub async fn run_consumer(
    js: Context,
    durable: &str,
    subjects: &[&str],
    handler: Handler,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let stream = js.get_stream(STREAM).await?;
    let dlq_subject = format!("dlq.{durable}");

    for subject in subjects {
        let name = format!("{durable}-{}", sanitize(subject));
        let consumer = stream
            .get_or_create_consumer(
                &name,
                pull::Config {
                    durable_name: Some(name.clone()),
                    filter_subject: (*subject).to_owned(),
                    max_deliver: MAX_DELIVER,
                    ack_wait: ACK_WAIT,
                    ..Default::default()
                },
            )
            .await?;

        let js_for_task = js.clone();
        let durable_label = durable.to_owned();
        let dlq = dlq_subject.clone();
        let task_handler = Arc::clone(&handler);
        tokio::spawn(async move {
            loop {
                let mut messages = match consumer.messages().await {
                    Ok(messages) => messages,
                    Err(err) => {
                        tracing::error!(consumer = name, error = %err, "consumer stream failed, retrying");
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        continue;
                    }
                };
                while let Some(message) = messages.next().await {
                    let message = match message {
                        Ok(message) => message,
                        Err(err) => {
                            tracing::warn!(consumer = name, error = %err, "message pull failed");
                            break;
                        }
                    };
                    let subject = message.subject.to_string();
                    let payload: Value = match serde_json::from_slice(&message.payload) {
                        Ok(payload) => payload,
                        Err(_) => {
                            tracing::warn!(subject, "bad event body");
                            let _ = message.ack().await;
                            continue;
                        }
                    };
                    let labels = [
                        KeyValue::new("durable", durable_label.clone()),
                        KeyValue::new("subject", subject.clone()),
                    ];
                    let delivered = message.info().map(|info| info.delivered).unwrap_or(1);
                    if let Ok(info) = message.info() {
                        if let Ok(mut pending) = PENDING.lock() {
                            pending.insert(name.clone(), info.pending);
                        }
                        if delivered == 1 {
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .map(|d| d.as_nanos() as i128);
                            let waited = now.unwrap_or_default() - info.published.unix_timestamp_nanos();
                            METRICS.lag.record((waited.max(0) as f64) / 1e9, &labels);
                        }
                    }
                    let span = tracing::info_span!("consume", subject = %subject, durable = %durable_label);
                    if let Some(headers) = message.headers.as_ref() {
                        let parent =
                            global::get_text_map_propagator(|propagator| propagator.extract(&HeaderExtractor(headers)));
                        let _ = span.set_parent(parent);
                    }
                    let started = Instant::now();
                    let outcome = task_handler(subject.clone(), payload).instrument(span.clone()).await;
                    METRICS.duration.record(started.elapsed().as_secs_f64(), &labels);
                    match outcome {
                        Ok(()) => {
                            let _ = message.ack().await;
                            METRICS
                                .processed
                                .add(1, &[labels.as_slice(), &[KeyValue::new("outcome", "ok")]].concat());
                        }
                        Err(err) => {
                            span.in_scope(|| tracing::error!(subject, error = %err, "handler failed"));
                            if delivered >= MAX_DELIVER {
                                if let Err(publish_err) =
                                    js_for_task.publish(dlq.clone(), message.payload.clone()).await
                                {
                                    tracing::error!(subject, error = %publish_err, "DLQ publish failed");
                                }
                                let _ = message.ack_with(async_nats::jetstream::AckKind::Term).await;
                                let dead = KeyValue::new("outcome", "dead_lettered");
                                METRICS.processed.add(1, &[labels.as_slice(), &[dead]].concat());
                            } else {
                                let _ = message
                                    .ack_with(async_nats::jetstream::AckKind::Nak(Some(NAK_DELAY)))
                                    .await;
                                METRICS
                                    .processed
                                    .add(1, &[labels.as_slice(), &[KeyValue::new("outcome", "retry")]].concat());
                            }
                        }
                    }
                }
            }
        });
    }
    tracing::info!(durable, subjects = ?subjects, "consumer bound");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn stamps_missing_event_id_only() {
        let mut fresh = json!({ "user_id": "u1" });
        stamp_event_id(&mut fresh);
        let id = uuid::Uuid::parse_str(fresh["event_id"].as_str().unwrap()).unwrap();
        assert_eq!(
            id.get_version_num(),
            7,
            "time-ordered, as the Python publishers stamp them"
        );

        let mut stamped = json!({ "event_id": "keep-me" });
        stamp_event_id(&mut stamped);
        assert_eq!(stamped["event_id"], "keep-me");
    }

    #[test]
    fn a_trace_crosses_nats_in_the_headers() {
        use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};
        use opentelemetry_sdk::propagation::TraceContextPropagator;

        global::set_text_map_propagator(TraceContextPropagator::new());
        let trace_id = TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap();
        let remote = SpanContext::new(
            trace_id,
            SpanId::from_hex("00f067aa0ba902b7").unwrap(),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        );
        let sent = opentelemetry::Context::new().with_remote_span_context(remote);
        let mut headers = async_nats::HeaderMap::new();
        global::get_text_map_propagator(|p| p.inject_context(&sent, &mut HeaderInjector(&mut headers)));
        let received = global::get_text_map_propagator(|p| p.extract(&HeaderExtractor(&headers)));
        assert_eq!(received.span().span_context().trace_id(), trace_id);
    }
}
