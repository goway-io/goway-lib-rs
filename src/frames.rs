use serde_json::{Value, json};

use crate::timeutils::now_iso;

pub fn envelope(frame_type: &str, data: Value) -> Value {
    json!({ "type": frame_type, "data": data, "ts": now_iso() })
}

pub fn error_frame(code: &str, key: &str) -> Value {
    envelope("error", json!({ "code": code, "key": key }))
}
