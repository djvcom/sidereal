//! Service lifecycle management.
//!
//! Handles starting and stopping all Sidereal services.

use std::sync::Arc;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::config::ServerConfig;

/// Manages the lifecycle of all Sidereal services.
pub struct Services {
    config: ServerConfig,
    cancel: CancellationToken,
    handles: Vec<(&'static str, JoinHandle<()>)>,
}

impl Services {
    /// Create a new service manager.
    pub fn new(config: ServerConfig) -> Self {
        Self {
            config,
            cancel: CancellationToken::new(),
            handles: Vec::new(),
        }
    }

    /// Start all enabled services.
    pub fn start(&mut self) -> anyhow::Result<()> {
        info!("Starting Sidereal services");

        // Start services in dependency order
        if self.config.scheduler.enabled {
            self.start_scheduler()?;
        }

        if self.config.control.enabled {
            self.start_control()?;
        }

        if self.config.build.enabled {
            self.start_build()?;
        }

        if self.config.gateway.enabled {
            self.start_gateway()?;
        }

        info!(
            services = self.handles.len(),
            "All enabled services started"
        );

        Ok(())
    }

    /// Wait for all services to complete or for shutdown.
    pub async fn wait(&mut self) {
        let cancel = self.cancel.clone();

        // Wait for any service to exit (which indicates a problem)
        // or for shutdown to be requested
        tokio::select! {
            () = cancel.cancelled() => {
                info!("Shutdown requested");
            }
            result = self.wait_for_any_exit() => {
                if let Some((name, result)) = result {
                    match result {
                        Ok(()) => info!(service = name, "Service exited"),
                        Err(e) => error!(service = name, error = %e, "Service panicked"),
                    }
                }
            }
        }
    }

    /// Request graceful shutdown of all services.
    pub fn shutdown(&self) {
        info!("Initiating graceful shutdown");
        self.cancel.cancel();
    }

    /// Get the cancellation token for external shutdown triggers.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    async fn wait_for_any_exit(
        &mut self,
    ) -> Option<(&'static str, Result<(), tokio::task::JoinError>)> {
        if self.handles.is_empty() {
            std::future::pending::<()>().await;
            return None;
        }

        loop {
            // Check for finished handles
            for i in 0..self.handles.len() {
                if self.handles[i].1.is_finished() {
                    let (name, handle) = self.handles.remove(i);
                    let result = handle.await;
                    return Some((name, result));
                }
            }

            if self.handles.is_empty() {
                return None;
            }

            // Small yield to avoid busy loop
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    fn start_gateway(&mut self) -> anyhow::Result<()> {
        let config = self.create_gateway_config()?;
        let cancel = self.cancel.clone();

        let handle = tokio::spawn(async move {
            info!("Gateway service starting");
            if let Err(e) = sidereal_gateway::run(config, cancel).await {
                error!(error = %e, "Gateway service failed");
            }
            info!("Gateway service stopped");
        });

        self.handles.push(("gateway", handle));
        info!(
            listen = %self.config.gateway.listen,
            "Gateway service spawned"
        );

        Ok(())
    }

    fn start_scheduler(&mut self) -> anyhow::Result<()> {
        let config = self.create_scheduler_config();
        let cancel = self.cancel.clone();

        let handle = tokio::spawn(async move {
            info!("Scheduler service starting");
            if let Err(e) = run_scheduler(config, cancel).await {
                error!(error = %e, "Scheduler service failed");
            }
            info!("Scheduler service stopped");
        });

        self.handles.push(("scheduler", handle));
        info!("Scheduler service spawned");

        Ok(())
    }

    fn start_control(&mut self) -> anyhow::Result<()> {
        let config = self.create_control_config();
        let cancel = self.cancel.clone();

        let handle = tokio::spawn(async move {
            info!("Control service starting");
            if let Err(e) = run_control(config, cancel).await {
                error!(error = %e, "Control service failed");
            }
            info!("Control service stopped");
        });

        self.handles.push(("control", handle));
        info!("Control service spawned");

        Ok(())
    }

    fn start_build(&mut self) -> anyhow::Result<()> {
        let config = self.create_build_config();
        let cancel = self.cancel.clone();

        let handle = tokio::spawn(async move {
            info!("Build service starting");
            if let Err(e) = run_build(config, cancel).await {
                error!(error = %e, "Build service failed");
            }
            info!("Build service stopped");
        });

        self.handles.push(("build", handle));
        info!("Build service spawned");

        Ok(())
    }

    fn create_gateway_config(&self) -> anyhow::Result<sidereal_gateway::GatewayConfig> {
        use sidereal_gateway::config::{
            ApiProxyConfig, GatewayConfig, LoadBalanceStrategyConfig, RoutingConfig,
            SchedulerResolverConfig, ServerConfig,
        };
        use std::path::PathBuf;

        // Scheduler resolver reads placement data from Valkey
        Ok(GatewayConfig {
            server: ServerConfig {
                bind_address: self.config.gateway.listen,
                ..Default::default()
            },
            routing: RoutingConfig::Scheduler(SchedulerResolverConfig {
                valkey_url: self.config.valkey.url.clone(),
                enable_cache: true,
                cache_ttl_secs: 5,
                vsock_uds_path: PathBuf::from("/var/run/firecracker/vsock"),
                load_balance: LoadBalanceStrategyConfig::default(),
            }),
            middleware: Default::default(),
            limits: Default::default(),
            metrics: None,
            api: Some(ApiProxyConfig {
                build_socket: self.config.server.socket_dir.join("build.sock"),
                control_socket: self.config.server.socket_dir.join("control.sock"),
            }),
        })
    }

    fn create_scheduler_config(&self) -> sidereal_scheduler::SchedulerConfig {
        use sidereal_core::Transport;
        use sidereal_scheduler::config::{ApiConfig, ValkeyConfig};

        let socket_path = self.config.server.socket_dir.join("scheduler.sock");

        sidereal_scheduler::SchedulerConfig {
            api: ApiConfig {
                listen: Transport::unix(&socket_path),
            },
            valkey: ValkeyConfig {
                url: self.config.valkey.url.clone(),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn create_control_config(&self) -> sidereal_control::ControlConfig {
        use sidereal_control::config::{
            ArtifactConfig, DatabaseConfig, ProvisionerConfig, ProvisionerType, SchedulerConfig,
            ServerConfig,
        };
        use sidereal_core::Transport;

        let socket_path = self.config.server.socket_dir.join("control.sock");
        let scheduler_url = format!(
            "unix://{}",
            self.config
                .server
                .socket_dir
                .join("scheduler.sock")
                .display()
        );

        let provisioner_type = match self.config.control.provisioner {
            crate::config::ProvisionerType::Mock => ProvisionerType::Mock,
            crate::config::ProvisionerType::Firecracker => ProvisionerType::Firecracker,
        };

        // Map storage backend to artifact store URL
        let store_url = match self.config.storage.backend {
            crate::config::StorageBackend::Filesystem => {
                format!(
                    "file://{}",
                    self.config.server.data_dir.join("artifacts").display()
                )
            }
            crate::config::StorageBackend::S3 => {
                format!("s3://{}", self.config.storage.bucket)
            }
        };

        sidereal_control::ControlConfig {
            server: ServerConfig {
                listen: Transport::unix(&socket_path),
                ..Default::default()
            },
            database: DatabaseConfig {
                url: self.config.database.url.clone(),
                max_connections: self.config.database.max_connections,
                ..Default::default()
            },
            scheduler: SchedulerConfig {
                url: scheduler_url,
                ..Default::default()
            },
            provisioner: ProvisionerConfig {
                provisioner_type,
                kernel_path: self.config.build.vm.kernel_path.clone(),
                work_dir: self.config.server.data_dir.join("vms"),
            },
            artifacts: ArtifactConfig {
                store_url,
                cache_dir: self.config.server.data_dir.join("artifact-cache"),
                endpoint: self.config.storage.endpoint.clone(),
                region: self.config.storage.region.clone(),
                access_key_id: self.config.storage.access_key_id.clone(),
                secret_access_key: self.config.storage.secret_access_key.clone(),
            },
            ..Default::default()
        }
    }

    fn create_build_config(&self) -> sidereal_build::ServiceConfig {
        use sidereal_build::artifact::StorageConfig;
        use sidereal_build::service::{PathsConfig, ServerConfig, WorkerConfig};
        use sidereal_core::Transport;

        let socket_path = self.config.server.socket_dir.join("build.sock");

        // Map server storage settings to build service storage config
        let storage_type = match self.config.storage.backend {
            crate::config::StorageBackend::Filesystem => "local",
            crate::config::StorageBackend::S3 => "s3",
        };

        sidereal_build::ServiceConfig {
            server: ServerConfig {
                listen: Transport::unix(&socket_path),
                ..Default::default()
            },
            paths: PathsConfig {
                checkouts: self.config.server.data_dir.join("checkouts"),
                caches: self.config.server.data_dir.join("caches"),
                artifacts: self.config.server.data_dir.join("artifacts"),
                runtime: self.config.build.paths.runtime.clone(),
            },
            worker: WorkerConfig {
                count: self.config.build.workers,
            },
            storage: StorageConfig {
                storage_type: storage_type.to_owned(),
                path: self.config.storage.bucket.clone(),
                region: self.config.storage.region.clone(),
                endpoint: self.config.storage.endpoint.clone(),
                access_key_id: self.config.storage.access_key_id.clone(),
                secret_access_key: self.config.storage.secret_access_key.clone(),
            },
            forge_auth: self.config.build.forge_auth.clone(),
            ..Default::default()
        }
    }
}

/// Serve an axum router over the given transport with graceful shutdown.
async fn serve_transport(
    transport: sidereal_core::Transport,
    app: axum::Router,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    use sidereal_core::Transport;

    match transport {
        Transport::Tcp { addr } => {
            let listener = tokio::net::TcpListener::bind(addr).await?;
            axum::serve(listener, app)
                .with_graceful_shutdown(cancel.cancelled_owned())
                .await?;
        }
        Transport::Unix { path } => {
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            if path.exists() {
                tokio::fs::remove_file(&path).await?;
            }
            let listener = tokio::net::UnixListener::bind(&path)?;
            axum::serve(listener, app)
                .with_graceful_shutdown(cancel.cancelled_owned())
                .await?;
        }
    }
    Ok(())
}

/// Run the scheduler service.
///
/// This wraps the scheduler startup logic since the crate doesn't export a `run` function.
async fn run_scheduler(
    config: sidereal_scheduler::SchedulerConfig,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    use sidereal_scheduler::store::InMemoryPlacementStore;
    use sidereal_scheduler::{
        api, HealthTracker, LeastLoaded, PlacementAlgorithm, PlacementAlgorithmType,
        PlacementStore, PowerOfTwoChoices, RoundRobin, ScalingPolicy, ValkeyPlacementStore,
        WorkerRegistry,
    };

    let registry = Arc::new(WorkerRegistry::new());
    let health_tracker = Arc::new(HealthTracker::new(config.health.clone(), registry.clone()));

    let _placement_algorithm: Arc<dyn PlacementAlgorithm> = match config.placement.algorithm {
        PlacementAlgorithmType::RoundRobin => Arc::new(RoundRobin::new()),
        PlacementAlgorithmType::PowerOfTwo => Arc::new(PowerOfTwoChoices::new()),
        PlacementAlgorithmType::LeastLoaded => Arc::new(LeastLoaded::new()),
    };

    let scaling_policy = Arc::new(ScalingPolicy::new(config.scaling.clone()));

    let placement_store: Arc<dyn PlacementStore> =
        match ValkeyPlacementStore::new(&config.valkey).await {
            Ok(store) => {
                info!(url = %config.valkey.url, "Scheduler connected to Valkey");
                Arc::new(store)
            }
            Err(e) => {
                error!(error = %e, "Failed to connect to Valkey, using in-memory store");
                Arc::new(InMemoryPlacementStore::new())
            }
        };

    let state = Arc::new(api::AppState {
        registry: registry.clone(),
        health_tracker: health_tracker.clone(),
        scaling_policy,
        placement_store,
    });

    // Spawn health check loop
    let health_clone = health_tracker.clone();
    let cancel_health = cancel.clone();
    tokio::spawn(async move {
        let interval = health_clone.heartbeat_interval();
        let mut ticker = tokio::time::interval(interval);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let timed_out = health_clone.check_heartbeat_timeouts();
                    for worker_id in timed_out {
                        info!(worker_id = %worker_id, "Worker heartbeat timeout");
                    }
                }
                () = cancel_health.cancelled() => {
                    break;
                }
            }
        }
    });

    let app = api::router(state);
    info!(transport = %config.api.listen, "Scheduler API listening");

    serve_transport(config.api.listen, app, cancel).await?;

    Ok(())
}

/// Run the control service.
async fn run_control(
    config: sidereal_control::ControlConfig,
    _cancel: CancellationToken,
) -> anyhow::Result<()> {
    use sidereal_control::service::ControlService;

    let service = ControlService::new(config);
    service.run().await?;

    Ok(())
}

/// Run the build service.
async fn run_build(
    config: sidereal_build::ServiceConfig,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    use sidereal_build::{
        api, artifact::ArtifactStore, service::BuildWorker, BuildQueue, ForgeAuth,
    };

    // Ensure directories exist
    tokio::fs::create_dir_all(&config.paths.checkouts).await?;
    tokio::fs::create_dir_all(&config.paths.caches).await?;
    tokio::fs::create_dir_all(&config.paths.artifacts).await?;

    let forge_auth = ForgeAuth::from_config(&config.forge_auth)?;

    let artifact_store = Arc::new(ArtifactStore::new(&config.storage)?);
    let queue = Arc::new(BuildQueue::new(config.server.max_queue_size));

    // Spawn build workers
    let mut worker_handles = Vec::new();
    for id in 0..config.worker.count {
        let worker = BuildWorker::with_options(
            id,
            Arc::clone(&queue),
            &config.paths,
            &config.limits,
            Arc::clone(&artifact_store),
            forge_auth.clone(),
            None, // cache config
            Some(&config.vm),
        )?;
        let worker_cancel = cancel.clone();
        worker_handles.push(tokio::spawn(async move {
            worker.run(worker_cancel).await;
        }));
    }

    info!(count = config.worker.count, "Build workers started");

    let state = Arc::new(api::AppState {
        queue: Arc::clone(&queue),
        forge_auth: forge_auth.clone(),
    });
    let app = api::router(state);

    info!(transport = %config.server.listen, "Build service listening");
    serve_transport(config.server.listen, app, cancel).await?;

    // Wait for workers to finish
    info!("Waiting for build workers to finish");
    for handle in worker_handles {
        let _ = handle.await;
    }

    // Cancel any remaining builds
    queue
        .cancel_all(sidereal_build::CancelReason::Shutdown)
        .await;

    Ok(())
}
