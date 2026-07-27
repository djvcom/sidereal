//! Sidereal Telemetry binary entry point.
//!
//! Starts the telemetry service with:
//! - OTLP gRPC receiver (port 4317)
//! - OTLP HTTP receiver (port 4318)
//! - Query API (port 3100)

use std::sync::Arc;
use std::time::Duration;

use sidereal::{
    auth::{grpc_auth_interceptor, OidcValidator},
    buffer::{start_background_flush, FlushHandle, Ingester},
    config::{BufferConfig, ParquetConfig, WalConfig},
    deployments::{deployment_router, DeploymentApiState},
    errors::{error_router, ErrorApiState},
    ingest::{
        otlp_http_router_with_auth, LogsServiceServer, MetricsServiceServer, OtlpGrpcReceiver,
        OtlpHttpState, TraceServiceServer,
    },
    query::{query_router, query_router_with_oidc, QueryApiState, QueryEngineBuilder},
    redact::RedactionEngine,
    retention::{start_background_retention, RetentionSweeper},
    schema::{
        logs::logs_storage_schema, metrics::number_metrics_storage_schema,
        traces::traces_storage_schema,
    },
    storage::{base_url, create_object_store, Signal},
    wal::Wal,
    TelemetryConfig,
};
use tokio::signal;
use tonic::transport::Server as TonicServer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "sidereal=info,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("Starting sidereal");

    let config = TelemetryConfig::load()?;
    tracing::info!(
        grpc_addr = %config.server.grpc_addr,
        http_addr = %config.server.http_addr,
        query_addr = %config.server.query_addr,
        "Configuration loaded"
    );

    let store = create_object_store(&config.storage)?;
    let base_url_str = base_url(&config.storage);
    tracing::info!(base_url = %base_url_str, "Object store created");

    if let Some(wal) = &config.wal {
        tracing::info!(
            path = %wal.path.display(),
            fsync = wal.fsync,
            "Write-ahead log enabled"
        );
    } else {
        tracing::warn!("Write-ahead log disabled — buffered telemetry is lost on crash");
    }

    let (trace_ingester, metrics_ingester, logs_ingester) = create_ingesters(
        store.clone(),
        &config.buffer,
        &config.parquet,
        config.wal.as_ref(),
    )?;

    for ingester in [&trace_ingester, &metrics_ingester, &logs_ingester] {
        ingester.recover_from_wal().await?;
    }

    let flush_handles = start_flush_tasks(&trace_ingester, &metrics_ingester, &logs_ingester);

    let retention_handle = config.retention.as_ref().map(|retention| {
        let sweeper = Arc::new(RetentionSweeper::new(store.clone(), retention.days));
        start_background_retention(sweeper, retention)
    });

    let query_engine = Arc::new(
        QueryEngineBuilder::new(store.clone(), &base_url_str)
            .with_timeout(Some(Duration::from_secs(config.query.timeout_secs)))
            .with_memory_limit(config.query.memory_limit_bytes)
            .build()
            .await?,
    );
    tracing::info!(
        memory_limit_bytes = config.query.memory_limit_bytes,
        timeout_secs = config.query.timeout_secs,
        "Query engine initialised"
    );

    let redaction = Arc::new(RedactionEngine::new(&config.redaction)?);
    if redaction.is_enabled() {
        tracing::info!(
            "Redaction engine enabled with {} rules",
            config.redaction.rules.len()
        );
    } else {
        tracing::debug!("Redaction engine disabled");
    }

    let grpc_receiver = OtlpGrpcReceiver::new(
        trace_ingester.clone(),
        metrics_ingester.clone(),
        logs_ingester.clone(),
        redaction.clone(),
    );
    let http_state = OtlpHttpState {
        trace_ingester,
        metrics_ingester,
        logs_ingester,
        redaction,
    };

    let query_state = QueryApiState::new(query_engine.clone());
    let error_aggregator = Arc::new(sidereal::errors::ErrorAggregator::new(query_engine.clone()));
    let error_state = ErrorApiState {
        aggregator: error_aggregator,
    };
    let deployment_state = DeploymentApiState {
        engine: query_engine,
    };

    let grpc_addr = config.server.grpc_addr;
    let http_addr = config.server.http_addr;
    let query_addr = config.server.query_addr;
    let auth_key = config.auth.api_key().map(str::to_owned);

    if config.auth.is_enabled() {
        tracing::info!("API key authentication enabled for OTLP endpoints");
    } else {
        tracing::warn!("API key authentication disabled — OTLP endpoints are open");
    }

    let oidc_validator = if let Some(oidc_config) = &config.auth.oidc {
        tracing::info!(issuer = %oidc_config.issuer, "Initialising OIDC validator");
        Some(OidcValidator::new(oidc_config).await?)
    } else {
        tracing::warn!("OIDC disabled — query API is unauthenticated");
        None
    };

    let grpc_server = match auth_key.as_deref() {
        Some(key) => {
            let interceptor = grpc_auth_interceptor(key);
            TonicServer::builder()
                .add_service(TraceServiceServer::with_interceptor(
                    grpc_receiver.clone(),
                    interceptor.clone(),
                ))
                .add_service(MetricsServiceServer::with_interceptor(
                    grpc_receiver.clone(),
                    interceptor.clone(),
                ))
                .add_service(LogsServiceServer::with_interceptor(
                    grpc_receiver,
                    interceptor,
                ))
                .serve_with_shutdown(grpc_addr, shutdown_signal("gRPC"))
        }
        None => TonicServer::builder()
            .add_service(TraceServiceServer::new(grpc_receiver.clone()))
            .add_service(MetricsServiceServer::new(grpc_receiver.clone()))
            .add_service(LogsServiceServer::new(grpc_receiver))
            .serve_with_shutdown(grpc_addr, shutdown_signal("gRPC")),
    };

    let http_router = otlp_http_router_with_auth(
        http_state,
        sidereal::ingest::DEFAULT_MAX_BODY_SIZE,
        auth_key.as_deref(),
    );
    let http_listener = tokio::net::TcpListener::bind(http_addr).await?;
    let http_server = axum::serve(http_listener, http_router)
        .with_graceful_shutdown(shutdown_signal("HTTP OTLP"));

    let api_router = match oidc_validator {
        Some(validator) => query_router_with_oidc(query_state, validator),
        None => query_router(query_state),
    }
    .nest("/errors", error_router(error_state))
    .nest("/deployments", deployment_router(deployment_state));
    let query_listener = tokio::net::TcpListener::bind(query_addr).await?;
    let query_server = axum::serve(query_listener, api_router)
        .with_graceful_shutdown(shutdown_signal("Query API"));

    tracing::info!(
        grpc = %grpc_addr,
        http = %http_addr,
        query = %query_addr,
        "Servers starting"
    );

    tokio::select! {
        result = grpc_server => {
            if let Err(e) = result {
                tracing::error!(error = %e, "gRPC server error");
            }
        }
        result = http_server => {
            if let Err(e) = result {
                tracing::error!(error = %e, "HTTP OTLP server error");
            }
        }
        result = query_server => {
            if let Err(e) = result {
                tracing::error!(error = %e, "Query API server error");
            }
        }
    }

    tracing::info!("Shutting down background tasks");
    for handle in flush_handles {
        handle.shutdown().await;
    }
    if let Some(handle) = retention_handle {
        handle.shutdown().await;
    }

    tracing::info!("Shutdown complete");
    Ok(())
}

/// Create ingesters for all signal types, attaching a write-ahead log when
/// one is configured.
fn create_ingesters(
    store: Arc<dyn object_store::ObjectStore>,
    buffer_config: &BufferConfig,
    parquet_config: &ParquetConfig,
    wal_config: Option<&WalConfig>,
) -> Result<(Arc<Ingester>, Arc<Ingester>, Arc<Ingester>), sidereal::TelemetryError> {
    let create = |signal: Signal,
                  schema: arrow::datatypes::SchemaRef|
     -> Result<Arc<Ingester>, sidereal::TelemetryError> {
        let ingester = Ingester::new(
            signal,
            schema.clone(),
            store.clone(),
            buffer_config.clone(),
            parquet_config.clone(),
        );
        let ingester = match wal_config {
            Some(wal) => ingester.with_wal(Wal::open(&wal.path, signal, schema, wal.fsync)?),
            None => ingester,
        };
        Ok(Arc::new(ingester))
    };

    Ok((
        create(Signal::Traces, traces_storage_schema())?,
        create(Signal::Metrics, number_metrics_storage_schema())?,
        create(Signal::Logs, logs_storage_schema())?,
    ))
}

/// Start background flush tasks for all ingesters.
fn start_flush_tasks(
    trace_ingester: &Arc<Ingester>,
    metrics_ingester: &Arc<Ingester>,
    logs_ingester: &Arc<Ingester>,
) -> Vec<FlushHandle> {
    vec![
        start_background_flush(trace_ingester.clone()),
        start_background_flush(metrics_ingester.clone()),
        start_background_flush(logs_ingester.clone()),
    ]
}

/// Create a shutdown signal future for graceful shutdown.
async fn shutdown_signal(server_name: &'static str) {
    let ctrl_c = async {
        if let Err(e) = signal::ctrl_c().await {
            tracing::error!(error = %e, "Failed to install Ctrl+C handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(e) => {
                tracing::error!(error = %e, "Failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {
            tracing::info!(server = server_name, "Received Ctrl+C, initiating shutdown");
        }
        () = terminate => {
            tracing::info!(server = server_name, "Received SIGTERM, initiating shutdown");
        }
    }
}
