use actix_web::http::StatusCode;
use actix_web::{HttpResponse, ResponseError};
use serde_json::{Map, Value, json};

pub const PROBLEM_TYPE_BASE: &str = "https://api.goway.uz/problems/";
pub const CONTENT_TYPE: &str = "application/problem+json";

#[derive(Debug, Clone)]
struct Inner {
    status: u16,
    title: &'static str,
    code: String,
    detail: Option<String>,
    i18n_key: Option<String>,
    params: Option<Map<String, Value>>,
}

#[derive(Debug, Clone)]
pub struct Problem(Box<Inner>);

impl Problem {
    pub fn new(status: u16, title: &'static str, code: impl Into<String>) -> Self {
        Self(Box::new(Inner {
            status,
            title,
            code: code.into(),
            detail: None,
            i18n_key: None,
            params: None,
        }))
    }

    pub fn bad_request(detail: impl Into<String>, code: impl Into<String>) -> Self {
        Self::new(400, "Bad Request", code).detail(detail)
    }

    pub fn unauthorized(detail: impl Into<String>, code: impl Into<String>) -> Self {
        Self::new(401, "Unauthorized", code).detail(detail)
    }

    pub fn forbidden(detail: impl Into<String>, code: impl Into<String>) -> Self {
        Self::new(403, "Forbidden", code).detail(detail)
    }

    pub fn not_found(resource: &str) -> Self {
        Self::new(404, "Not Found", "NOT_FOUND")
            .detail(format!("{resource} not found"))
            .key("common.not_found")
    }

    pub fn conflict(detail: impl Into<String>, code: impl Into<String>) -> Self {
        Self::new(409, "Conflict", code).detail(detail)
    }

    pub fn upstream(detail: impl Into<String>) -> Self {
        Self::new(502, "Bad Gateway", "UPSTREAM_UNAVAILABLE")
            .detail(detail)
            .key("common.upstream_unavailable")
    }

    pub fn internal() -> Self {
        Self::new(500, "Internal Server Error", "INTERNAL").key("common.internal")
    }

    pub fn detail(mut self, detail: impl Into<String>) -> Self {
        self.0.detail = Some(detail.into());
        self
    }

    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.0.i18n_key = Some(key.into());
        self
    }

    pub fn param(mut self, name: &str, value: impl Into<Value>) -> Self {
        self.0
            .params
            .get_or_insert_with(Map::new)
            .insert(name.to_owned(), value.into());
        self
    }

    pub fn status(&self) -> u16 {
        self.0.status
    }

    pub fn code(&self) -> &str {
        &self.0.code
    }

    pub fn i18n_key(&self) -> Option<&str> {
        self.0.i18n_key.as_deref()
    }

    pub fn body(&self) -> Value {
        let inner = &self.0;
        let mut body = json!({
            "type": format!("{PROBLEM_TYPE_BASE}{}", inner.code.to_lowercase().replace('_', "-")),
            "title": inner.title,
            "status": inner.status,
            "code": inner.code,
        });
        let object = body.as_object_mut().expect("problem body is an object");
        if let Some(detail) = &inner.detail {
            object.insert("detail".into(), json!(detail));
        }
        if let Some(key) = &inner.i18n_key {
            object.insert("key".into(), json!(key));
        }
        if let Some(params) = &inner.params {
            object.insert("params".into(), Value::Object(params.clone()));
        }
        body
    }
}

impl std::fmt::Display for Problem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.0.status, self.0.code)
    }
}

impl std::error::Error for Problem {}

impl ResponseError for Problem {
    fn status_code(&self) -> StatusCode {
        StatusCode::from_u16(self.0.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code())
            .content_type(CONTENT_TYPE)
            .json(self.body())
    }
}
