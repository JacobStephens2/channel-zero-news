//! Typed application error that converts into an HTTP JSON response.
//!
//! REST handlers return `Result<T, AppError>`; nothing on the request path
//! unwraps. WebSocket-level problems are reported in-band as
//! [`crate::protocol::ServerMsg::Error`] rather than as HTTP errors.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};

use crate::registry::CreateRoomError;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    BadRequest(String),
    /// Creation could not retain a room (live-room cap or join-code attempts).
    #[error("{0}")]
    Unavailable(String),
    /// The creating client address already has its quota of live rooms.
    #[error("{0}")]
    TooManyRooms(String),
    #[error("{0}")]
    Internal(String),
}

impl AppError {
    fn parts(&self) -> (StatusCode, &'static str) {
        match self {
            AppError::NotFound(_) => (StatusCode::NOT_FOUND, "not_found"),
            AppError::BadRequest(_) => (StatusCode::BAD_REQUEST, "bad_request"),
            AppError::Unavailable(_) => (StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
            AppError::TooManyRooms(_) => (StatusCode::TOO_MANY_REQUESTS, "too_many_rooms"),
            AppError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
        }
    }
}

impl From<CreateRoomError> for AppError {
    fn from(err: CreateRoomError) -> Self {
        match err {
            CreateRoomError::TooManyRooms => AppError::TooManyRooms(err.to_string()),
            CreateRoomError::AtCapacity | CreateRoomError::NoJoinCode => {
                AppError::Unavailable(err.to_string())
            }
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code) = self.parts();
        if status == StatusCode::INTERNAL_SERVER_ERROR {
            tracing::error!(error = %self, "internal error");
        }
        let body = Json(serde_json::json!({
            "error": code,
            "message": self.to_string(),
        }));
        (status, body).into_response()
    }
}
