use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::consumer::pull;
use async_nats::jetstream::{Context, stream};
use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use serde_json::Value;

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
    let ack = js
        .publish_with_headers(subject.to_owned(), headers, message.to_string().into())
        .await?;
    ack.await?;
    Ok(())
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
                    match task_handler(subject.clone(), payload).await {
                        Ok(()) => {
                            let _ = message.ack().await;
                        }
                        Err(err) => {
                            tracing::error!(subject, error = %err, "handler failed");
                            let delivered = message.info().map(|info| info.delivered).unwrap_or(1);
                            if delivered >= MAX_DELIVER {
                                if let Err(publish_err) =
                                    js_for_task.publish(dlq.clone(), message.payload.clone()).await
                                {
                                    tracing::error!(subject, error = %publish_err, "DLQ publish failed");
                                }
                                let _ = message.ack_with(async_nats::jetstream::AckKind::Term).await;
                            } else {
                                let _ = message
                                    .ack_with(async_nats::jetstream::AckKind::Nak(Some(NAK_DELAY)))
                                    .await;
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
}
