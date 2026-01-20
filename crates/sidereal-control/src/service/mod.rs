//! Service lifecycle management.
//!
//! Provides the main service runner with signal handling and graceful shutdown.

use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::api;
use crate::config::ControlConfig;
use crate::deployment::DeploymentManager;
use crate::error::{ControlError, ControlResult};
use crate::provisioner::{create_provisioner, WorkerProvisioner};
use crate::scheduler::SchedulerClient;
use crate::store::{DeploymentStore, MemoryStore, PostgresStore};

/// The control service.
///
/// Manages the lifecycle of the control plane, including:
/// - Database connections
/// - Worker provisioner
/// - HTTP API server
/// - Signal handling and graceful shutdown
pub struct ControlService {
    config: ControlConfig,
    cancel: CancellationToken,
}

impl ControlService {
    /// Create a new control service with the given configuration.
    #[must_use]
    pub fn new(config: ControlConfig) -> Self {
        Self {
            config,
            cancel: CancellationToken::new(),
        }
    }

    /// Run the control service.
    ///
    /// This will:
    /// 1. Connect to the database (or use in-memory store as fallback)
    /// 2. Create the worker provisioner
    /// 3. Create the deployment manager
    /// 4. Start the HTTP API server
    /// 5. Wait for shutdown signal
    pub async fn run(&self) -> ControlResult<()> {
        let store = self.create_store().await;
        let provisioner = self.create_provisioner()?;

        let scheduler = SchedulerClient::with_url(&self.config.scheduler.url)?;
        info!(url = %self.config.scheduler.url, "scheduler client configured");

        let manager = Arc::new(DeploymentManager::new(
            Arc::clone(&store),
            provisioner,
            scheduler,
            self.config.artifacts.clone(),
            self.config.deployment.clone(),
        ));
        info!("deployment manager initialised");

        let state = api::AppState {
            manager,
            store: Arc::clone(&store),
        };

        let app = api::router(state);

        let listener = TcpListener::bind(&self.config.server.listen_addr)
            .await
            .map_err(|e| ControlError::internal(format!("failed to bind: {e}")))?;

        info!(
            addr = %self.config.server.listen_addr,
            "control service listening"
        );

        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal(self.cancel.clone()))
            .await
            .map_err(|e| ControlError::internal(format!("server error: {e}")))?;

        info!("control service shutdown complete");
        Ok(())
    }

    /// Request graceful shutdown.
    pub fn shutdown(&self) {
        self.cancel.cancel();
    }

    async fn create_store(&self) -> Arc<dyn DeploymentStore> {
        match PostgresStore::new(&self.config.database.url).await {
            Ok(store) => {
                info!(url = %self.config.database.url, "connected to PostgreSQL");
                Arc::new(store)
            }
            Err(e) => {
                error!(
                    error = %e,
                    "failed to connect to PostgreSQL, using in-memory store"
                );
                Arc::new(MemoryStore::new())
            }
        }
    }

    fn create_provisioner(&self) -> ControlResult<Arc<dyn WorkerProvisioner>> {
        let provisioner = create_provisioner(&self.config.provisioner)?;
        info!(
            provisioner_type = ?self.config.provisioner.provisioner_type,
            "worker provisioner configured"
        );
        Ok(provisioner)
    }
}

async fn shutdown_signal(cancel: CancellationToken) {
    let ctrl_c = async {
        if let Err(e) = signal::ctrl_c().await {
            error!(error = %e, "failed to install Ctrl+C handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(e) => {
                error!(error = %e, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {
            info!("received Ctrl+C, initiating shutdown");
        }
        () = terminate => {
            info!("received SIGTERM, initiating shutdown");
        }
        () = cancel.cancelled() => {
            info!("shutdown requested");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_creation() {
        let config = ControlConfig::default();
        let service = ControlService::new(config);
        assert!(!service.cancel.is_cancelled());
    }

    #[test]
    fn service_shutdown() {
        let config = ControlConfig::default();
        let service = ControlService::new(config);
        service.shutdown();
        assert!(service.cancel.is_cancelled());
    }
}
