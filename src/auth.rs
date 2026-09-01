use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, errors::ErrorKind};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use serde::Deserialize;
use uuid::Uuid;

use crate::problem::Problem;

#[derive(Debug, Clone)]
pub struct Actor {
    pub user_id: Uuid,
    pub org_id: Uuid,
    pub role: String,
    pub role_id: Option<Uuid>,
    pub rv: i64,
    pub iat: i64,
}

#[derive(Deserialize)]
struct Claims {
    sub: String,
    org: String,
    role: String,
    rid: Option<String>,
    #[serde(default)]
    rv: i64,
    iat: i64,
}

pub fn role_version_key(role_id: Uuid) -> String {
    format!("role:{role_id}:version")
}

pub fn user_revocation_key(user_id: Uuid) -> String {
    format!("user:{user_id}:revoked_at")
}

pub fn verify_access_token(token: &str, secret: &str) -> Result<Actor, Problem> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = true;
    let data =
        decode::<Claims>(token, &DecodingKey::from_secret(secret.as_bytes()), &validation).map_err(|err| match err
            .kind()
        {
            ErrorKind::ExpiredSignature => {
                Problem::unauthorized("Token expired", "TOKEN_EXPIRED").key("auth.token_expired")
            }
            _ => Problem::unauthorized("Invalid token", "TOKEN_INVALID").key("auth.token_invalid"),
        })?;
    let claims = data.claims;
    let user_id = Uuid::parse_str(&claims.sub)
        .map_err(|_| Problem::unauthorized("Invalid token", "TOKEN_INVALID").key("auth.token_invalid"))?;
    let org_id = Uuid::parse_str(&claims.org)
        .map_err(|_| Problem::unauthorized("Invalid token", "TOKEN_INVALID").key("auth.token_invalid"))?;
    Ok(Actor {
        user_id,
        org_id,
        role: claims.role,
        role_id: claims.rid.as_deref().and_then(|rid| Uuid::parse_str(rid).ok()),
        rv: claims.rv,
        iat: claims.iat,
    })
}

pub async fn enforce_staleness(redis: &mut ConnectionManager, actor: &Actor) -> Result<(), Problem> {
    if let Some(role_id) = actor.role_id {
        match redis.get::<_, Option<i64>>(role_version_key(role_id)).await {
            Ok(Some(current)) if current != actor.rv => {
                return Err(
                    Problem::unauthorized("Permissions changed, re-login", "TOKEN_STALE").key("auth.token_stale")
                );
            }
            Ok(_) => {}
            Err(err) => tracing::warn!(error = %err, "staleness check failed open"),
        }
    }
    match redis.get::<_, Option<i64>>(user_revocation_key(actor.user_id)).await {
        Ok(Some(revoked_at)) if actor.iat < revoked_at => {
            Err(Problem::unauthorized("Session revoked", "TOKEN_REVOKED").key("auth.token_revoked"))
        }
        Ok(_) => Ok(()),
        Err(err) => {
            tracing::warn!(error = %err, "revocation check failed open");
            Ok(())
        }
    }
}

pub fn service_auth_ok(provided: &str, secret: &str) -> bool {
    !secret.is_empty() && constant_time_eq::constant_time_eq(provided.as_bytes(), secret.as_bytes())
}
