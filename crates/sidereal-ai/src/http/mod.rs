//! HTTP API for the query companion.
//!
//! Exposes `POST /ask` for natural-language questions, a `/health` probe, and
//! a minimal self-contained chat page at `/`.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::agent::{AgentError, Answer, QueryAgent, ToolInvocation};

/// Request body for `POST /ask`.
#[derive(Debug, Deserialize)]
pub struct AskRequest {
    /// The natural-language question.
    pub question: String,
}

/// Error response body.
///
/// Failures that occurred after tool calls were made include the provenance
/// gathered so far, so partial evidence remains auditable.
#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    provenance: Vec<ToolInvocation>,
}

/// Build the HTTP router.
pub fn router(agent: Arc<QueryAgent>) -> Router {
    Router::new()
        .route("/", get(handle_index))
        .route("/health", get(handle_health))
        .route("/ask", post(handle_ask))
        .with_state(agent)
}

/// Serve the HTTP API on the given address until the process exits.
pub async fn serve(listen_address: &str, agent: Arc<QueryAgent>) -> Result<(), ServeError> {
    let listener = tokio::net::TcpListener::bind(listen_address)
        .await
        .map_err(|source| ServeError::Bind {
            address: listen_address.to_owned(),
            source,
        })?;
    tracing::info!(address = %listen_address, "HTTP API listening");
    axum::serve(listener, router(agent))
        .await
        .map_err(ServeError::Serve)
}

/// Errors that can occur while running the HTTP server.
#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    /// The listen address could not be bound.
    #[error("failed to bind {address}: {source}")]
    Bind {
        /// The address that could not be bound.
        address: String,
        /// The underlying I/O error.
        source: std::io::Error,
    },
    /// The server failed while running.
    #[error("server error: {0}")]
    Serve(#[source] std::io::Error),
}

async fn handle_index() -> Html<&'static str> {
    Html(include_str!("chat.html"))
}

async fn handle_health() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

async fn handle_ask(
    State(agent): State<Arc<QueryAgent>>,
    Json(request): Json<AskRequest>,
) -> Result<Json<Answer>, ApiError> {
    let question = request.question.trim();
    if question.is_empty() {
        return Err(ApiError::EmptyQuestion);
    }
    let answer = agent.ask(question).await?;
    Ok(Json(answer))
}

/// Errors returned by API handlers.
#[derive(Debug, thiserror::Error)]
enum ApiError {
    /// The request contained no question.
    #[error("question must not be empty")]
    EmptyQuestion,
    /// The agent failed to answer.
    #[error(transparent)]
    Agent(#[from] AgentError),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::EmptyQuestion => StatusCode::BAD_REQUEST,
            Self::Agent(AgentError::Configuration(_)) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Agent(AgentError::MaxTurns { .. } | AgentError::Model { .. }) => {
                StatusCode::BAD_GATEWAY
            }
        };
        let error = self.to_string();
        let provenance = match self {
            Self::Agent(
                AgentError::MaxTurns { provenance, .. } | AgentError::Model { provenance, .. },
            ) => provenance,
            Self::EmptyQuestion | Self::Agent(AgentError::Configuration(_)) => Vec::new(),
        };
        (status, Json(ErrorResponse { error, provenance })).into_response()
    }
}
