//! Error type for the qhlv core, plus its mapping to HTTP responses.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

/// A qhlv result. Handlers and store/space operations return this.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The referenced object does not exist (or has already expired).
    #[error("not found")]
    NotFound,

    /// Missing or wrong owner token, or the object has no owner. Maps to 403.
    #[error("forbidden")]
    Forbidden,

    /// The client's payload or request failed validation. Maps to 422.
    #[error("validation error: {0}")]
    Validation(String),

    /// A datastore (Redis) operation failed. Maps to 500.
    #[error("store error: {0}")]
    Redis(#[from] redis::RedisError),

    /// (De)serialization of a payload/point/viewport failed. Maps to 500.
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),

    /// A `Space` implementation failed (e.g. an embedding call). Maps to 500.
    #[error("space error: {0}")]
    Space(String),
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            // Client-facing, safe to surface as-is.
            Error::NotFound => (StatusCode::NOT_FOUND, "not found").into_response(),
            Error::Forbidden => (StatusCode::FORBIDDEN, "forbidden").into_response(),
            Error::Validation(msg) => (StatusCode::UNPROCESSABLE_ENTITY, msg).into_response(),
            // Internal failures: log the detail, return an opaque 500 to the client.
            Error::Redis(_) | Error::Json(_) | Error::Space(_) => {
                tracing::warn!("internal error: {self}");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
            }
        }
    }
}
