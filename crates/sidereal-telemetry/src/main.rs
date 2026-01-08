//! Sidereal Telemetry binary entry point.
//!
//! Starts the telemetry service with:
//! - OTLP gRPC receiver (port 4317)
//! - OTLP HTTP receiver (port 4318)
//! - Query API (port 3100)

use std::sync::Arc;

use sidereal_telemetry::{
    buffer::{start_background_flush, FlushHandle, Ingester},
    config::{BufferConfig, ParquetConfig},
    deployments::{deployment_router, DeploymentApiState},
    errors::{error_router, ErrorApiState},
    ingest::{
        otlp_http_router, LogsServiceServer, MetricsServiceServer, OtlpGrpcReceiver, OtlpHttpState,
        TraceServiceServer,
    },
    query::{query_router, QueryApiState, QueryEngine},
    redact::RedactionEngine,
    schema::{logs::logs_schema, metrics::number_metrics_schema, traces::traces_schema},
    storage::{base_url, create_object_store, Signal},
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
                .unwrap_or_else(|_| "sidereal_telemetry=info,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("Starting sidereal-telemetry");

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

    let (trace_ingester, metrics_ingester, logs_ingester) =
        create_ingesters(store.clone(), &config.buffer, &config.parquet);
    let flush_handles = start_flush_tasks(&trace_ingester, &metrics_ingester, &logs_ingester);

    let query_engine = Arc::new(QueryEngine::new(store.clone(), &base_url_str).await?);
    tracing::info!("Query engine initialised");

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
    let error_aggregator = Arc::new(sidereal_telemetry::errors::ErrorAggregator::new(
        query_engine.clone(),
    ));
    let error_state = ErrorApiState {
        aggregator: error_aggregator,
    };
    let deployment_state = DeploymentApiState {
        engine: query_engine,
    };

    let grpc_addr = config.server.grpc_addr;
    let http_addr = config.server.http_addr;
    let query_addr = config.server.query_addr;

    let grpc_server = TonicServer::builder()
        .add_service(TraceServiceServer::new(grpc_receiver.clone()))
        .add_service(MetricsServiceServer::new(grpc_receiver.clone()))
        .add_service(LogsServiceServer::new(grpc_receiver))
        .serve_with_shutdown(grpc_addr, shutdown_signal("gRPC"));

    let http_router = otlp_http_router(http_state);
    let http_listener = tokio::net::TcpListener::bind(http_addr).await?;
    let http_server = axum::serve(http_listener, http_router)
        .with_graceful_shutdown(shutdown_signal("HTTP OTLP"));

    let api_router = query_router(query_state)
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

    tracing::info!("Shutdown complete");
    Ok(())
}

/// Create ingesters for all signal types.
fn create_ingesters(
    store: Arc<dyn object_store::ObjectStore>,
    buffer_config: &BufferConfig,
    parquet_config: &ParquetConfig,
) -> (Arc<Ingester>, Arc<Ingester>, Arc<Ingester>) {
    let trace_ingester = Arc::new(Ingester::new(
        Signal::Traces,
        traces_schema(),
        store.clone(),
        buffer_config.clone(),
        parquet_config.clone(),
    ));

    let metrics_ingester = Arc::new(Ingester::new(
        Signal::Metrics,
        number_metrics_schema(),
        store.clone(),
        buffer_config.clone(),
        parquet_config.clone(),
    ));

    let logs_ingester = Arc::new(Ingester::new(
        Signal::Logs,
        logs_schema(),
        store,
        buffer_config.clone(),
        parquet_config.clone(),
    ));

    (trace_ingester, metrics_ingester, logs_ingester)
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
