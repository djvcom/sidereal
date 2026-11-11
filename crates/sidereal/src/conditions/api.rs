//! HTTP API for reliability conditions.
//!
//! | Method | Path                          | Purpose                                         |
//! |--------|-------------------------------|-------------------------------------------------|
//! | POST   | `/preview`                    | How do these requests behave today?             |
//! | GET    | `/attributes`                 | Which attributes can a condition refer to?      |
//! | POST   | `/`                           | Create a condition                              |
//! | GET    | `/`                           | Every condition with its current standing       |
//! | GET    | `/{id}`                       | One condition with its current standing         |
//! | PUT    | `/{id}`                       | Change a condition                              |
//! | DELETE | `/{id}`                       | Retire a condition                              |
//! | GET    | `/{id}/history`               | Every revision: who changed what, and when      |
//! | GET    | `/{id}/backtest`              | How it would have fared over stored history     |
//!
//! `/preview` is what a conversation with a non-technical author is built
//! on: describe some requests, see how they behave, be offered a target.
//! It needs no condition to exist.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use super::evaluate::{self, Backtest, Profile, Status};
use super::sql::Window;
use super::store::{ConditionStore, Revision, Stored};
use super::{Condition, Invalid, Selector, Success};
use crate::attributes::{AttributeRegistry, KnownAttribute};
use crate::query::QueryEngine;
use crate::storage::Signal;
use crate::TelemetryError;

/// Header naming who is making a change, recorded in the revision trail.
pub const AUTHOR_HEADER: &str = "x-sidereal-author";

/// Default period a preview looks back over, in hours.
const DEFAULT_PREVIEW_HOURS: u32 = 7 * 24;

/// Longest period a preview or backtest may look back over, in days.
const MAX_LOOKBACK_DAYS: u32 = 90;

/// State shared by the condition routes.
#[derive(Clone)]
pub struct ConditionApiState {
    /// Runs the queries.
    pub engine: Arc<QueryEngine>,
    /// Holds the definitions.
    pub store: Arc<ConditionStore>,
    /// Knows which attributes exist.
    pub attributes: Arc<AttributeRegistry>,
}

/// Build the router for condition routes, to be nested under a prefix.
pub fn condition_router(state: ConditionApiState) -> Router {
    Router::new()
        .route("/", post(create).get(list))
        .route("/preview", post(preview))
        .route("/attributes", get(attributes))
        .route("/{id}", get(read).put(update).delete(retire))
        .route("/{id}/history", get(history))
        .route("/{id}/backtest", get(backtest))
        .with_state(state)
}

/// A failed request.
enum ApiError {
    Invalid(String),
    NotFound(String),
    Conflict(String),
    Internal(TelemetryError),
}

impl From<TelemetryError> for ApiError {
    fn from(error: TelemetryError) -> Self {
        Self::Internal(error)
    }
}

impl From<Invalid> for ApiError {
    fn from(error: Invalid) -> Self {
        Self::Invalid(error.0)
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error) = match self {
            Self::Invalid(message) => (StatusCode::BAD_REQUEST, message),
            Self::NotFound(id) => (
                StatusCode::NOT_FOUND,
                format!("there is no condition called {id}"),
            ),
            Self::Conflict(id) => (
                StatusCode::CONFLICT,
                format!("a condition called {id} already exists"),
            ),
            Self::Internal(error) => {
                tracing::error!(%error, "condition request failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "the request could not be completed".to_owned(),
                )
            }
        };
        (status, Json(ErrorBody { error })).into_response()
    }
}

fn author(headers: &HeaderMap) -> Option<String> {
    headers
        .get(AUTHOR_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

/// Body of `POST /preview`.
#[derive(Debug, Deserialize)]
pub struct PreviewRequest {
    /// Which requests to look at.
    pub requests: Selector,
    /// What good looks like. Without it, a request is good if no span
    /// ended in error.
    #[serde(default)]
    pub success: Success,
    /// How far back to look, in hours. Defaults to seven days.
    #[serde(default)]
    pub lookback_hours: Option<u32>,
}

async fn preview(
    State(state): State<ConditionApiState>,
    Json(request): Json<PreviewRequest>,
) -> Result<Json<Profile>, ApiError> {
    request.requests.validate()?;
    request.success.validate()?;
    let hours = request
        .lookback_hours
        .unwrap_or(DEFAULT_PREVIEW_HOURS)
        .clamp(1, MAX_LOOKBACK_DAYS * 24);
    let window = Window::hours_ending(Utc::now(), hours);
    let profile =
        evaluate::profile(&state.engine, &request.requests, &request.success, window).await?;
    Ok(Json(profile))
}

/// A condition with where it stands.
#[derive(Serialize)]
struct Standing {
    #[serde(flatten)]
    stored: Stored,
    status: Status,
}

async fn standing(state: &ConditionApiState, stored: Stored) -> Result<Standing, ApiError> {
    let status =
        evaluate::status(&state.engine, &stored.condition, stored.created, Utc::now()).await?;
    Ok(Standing { stored, status })
}

/// A newly created condition, with how it would have fared so far.
#[derive(Serialize)]
struct Created {
    #[serde(flatten)]
    standing: Standing,
    backtest: Backtest,
}

async fn create(
    State(state): State<ConditionApiState>,
    headers: HeaderMap,
    Json(condition): Json<Condition>,
) -> Result<Response, ApiError> {
    condition.validate()?;
    if state.store.get(&condition.id).await?.is_some() {
        return Err(ApiError::Conflict(condition.id));
    }
    state.store.save(&condition, author(&headers)).await?;
    let stored = state
        .store
        .get(&condition.id)
        .await?
        .ok_or_else(|| ApiError::NotFound(condition.id.clone()))?;
    let backtest = evaluate::backtest(
        &state.engine,
        &stored.condition,
        Utc::now(),
        stored.condition.target.window_days,
    )
    .await?;
    let standing = standing(&state, stored).await?;
    Ok((StatusCode::CREATED, Json(Created { standing, backtest })).into_response())
}

async fn list(State(state): State<ConditionApiState>) -> Result<Json<Vec<Standing>>, ApiError> {
    let mut all = Vec::new();
    for stored in state.store.list().await? {
        all.push(standing(&state, stored).await?);
    }
    Ok(Json(all))
}

async fn existing(state: &ConditionApiState, id: &str) -> Result<Stored, ApiError> {
    state
        .store
        .get(id)
        .await?
        .ok_or_else(|| ApiError::NotFound(id.to_owned()))
}

async fn read(
    State(state): State<ConditionApiState>,
    Path(id): Path<String>,
) -> Result<Json<Standing>, ApiError> {
    let stored = existing(&state, &id).await?;
    Ok(Json(standing(&state, stored).await?))
}

async fn update(
    State(state): State<ConditionApiState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(condition): Json<Condition>,
) -> Result<Json<Standing>, ApiError> {
    if condition.id != id {
        return Err(ApiError::Invalid(
            "the id in the body must match the id in the path".to_owned(),
        ));
    }
    condition.validate()?;
    existing(&state, &id).await?;
    state.store.save(&condition, author(&headers)).await?;
    let stored = existing(&state, &id).await?;
    Ok(Json(standing(&state, stored).await?))
}

async fn retire(
    State(state): State<ConditionApiState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    if state.store.retire(&id, author(&headers)).await? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::NotFound(id))
    }
}

async fn history(
    State(state): State<ConditionApiState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<Revision>>, ApiError> {
    let revisions = state.store.history(&id).await?;
    if revisions.is_empty() {
        return Err(ApiError::NotFound(id));
    }
    Ok(Json(revisions))
}

/// Query parameters of `GET /{id}/backtest`.
#[derive(Debug, Default, Deserialize)]
pub struct BacktestParams {
    /// How many days of history to test against. Defaults to the
    /// condition's own period.
    #[serde(default)]
    pub days: Option<u32>,
}

async fn backtest(
    State(state): State<ConditionApiState>,
    Path(id): Path<String>,
    Query(params): Query<BacktestParams>,
) -> Result<Json<Backtest>, ApiError> {
    let stored = existing(&state, &id).await?;
    let days = params
        .days
        .unwrap_or(stored.condition.target.window_days)
        .clamp(1, MAX_LOOKBACK_DAYS);
    Ok(Json(
        evaluate::backtest(&state.engine, &stored.condition, Utc::now(), days).await?,
    ))
}

async fn attributes(State(state): State<ConditionApiState>) -> Json<Vec<KnownAttribute>> {
    Json(state.attributes.known(Signal::Traces))
}
