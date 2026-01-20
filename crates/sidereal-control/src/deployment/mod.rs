//! Deployment orchestration and lifecycle management.
//!
//! This module coordinates the deployment process from artifact download
//! through worker provisioning and scheduler registration.

mod manager;

pub use manager::{DeploymentManager, DeploymentRequest};
