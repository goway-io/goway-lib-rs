use chrono::{DateTime, Utc};

pub fn now_utc() -> DateTime<Utc> {
    Utc::now()
}

pub fn now_iso() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

pub fn unix_ts() -> i64 {
    Utc::now().timestamp()
}

pub fn unix_ms() -> i64 {
    Utc::now().timestamp_millis()
}
