//! Deployment tracking module for sidereal.
//!
//! Provides deployment tracking built on OTEL log events:
//!
//! - **Events**: Deployments are captured as logs with `event.name = "deployment"`
//! - **Querying**: List deployments, service versions, and deployment history
//! - **Correlation**: Link deployments to errors they may have introduced
//! - **API**: REST endpoints for deployment queries
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
//! │  OTEL Log Event │────>│  Logs Table      │────>│ Deployment API  │
//! │  event.name =   │     │  (DataFusion)    │     │ (axum handlers) │
//! │  "deployment"   │     └──────────────────┘     └─────────────────┘
//! └─────────────────┘                                      │
//!                                                          v
//!                                              ┌─────────────────────┐
//!                                              │  Error Correlation  │
//!                                              │  (compare errors    │
//!                                              │   before/after)     │
//!                                              └─────────────────────┘
//! ```
//!
//! # Deployment Event Schema
//!
//! Deployment events should be emitted as OTEL log records with:
//!
//! | Field | Value |
//! |-------|-------|
//! | `event.name` | `"deployment"` |
//! | `event.domain` | `"sidereal"` (optional) |
//! | `service.name` | Service being deployed |
//! | `service.version` | Version being deployed |
//! | `deployment.environment.name` | Target environment |
//!
//! Additional attributes (stored in `log_attributes`):
//!
//! | Attribute | Description |
//! |-----------|-------------|
//! | `deployment.id` | Unique deployment identifier |
//! | `deployment.status` | `started`, `succeeded`, `failed`, `rolled_back` |
//! | `vcs.revision` | Git commit SHA |
//! | `vcs.branch` | Git branch name |
//! | `vcs.message` | Commit message |
//!
//! # Example
//!
//! ```ignore
//! use sidereal::deployments::{
//!     DeploymentFilter, DeploymentQueryBuilder,
//! };
//!
//! // Query recent deployments for a service
//! let filter = DeploymentFilter::default()
//!     .with_service("api-server")
//!     .with_environment("production");
//!
//! let sql = DeploymentQueryBuilder::new(filter)
//!     .limit(10)
//!     .build();
//! ```

pub mod api;
mod queries;
mod types;

pub use api::{deployment_router, DeploymentApiState};
pub use queries::{
    DeploymentErrorsQueryBuilder, DeploymentQueryBuilder, ServiceVersionsQueryBuilder,
    DEFAULT_DEPLOYMENT_LIMIT, DEPLOYMENT_EVENT_NAME,
};
pub use types::{Deployment, DeploymentFilter, DeploymentStatus, ServiceVersion};
