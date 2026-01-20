//! HTTP client for the scheduler API.

use std::time::Duration;

use reqwest::{Client, StatusCode};

use crate::config::SchedulerConfig;
use crate::error::{ControlError, ControlResult};

use super::{HealthResponse, PlacementInfo, PlacementStatus, ReadyResponse, WorkerInfo};

/// Raw placement response from the scheduler API.
#[derive(serde::Deserialize)]
struct RawPlacement {
    function: String,
    status: String,
    workers: Vec<super::PlacementWorker>,
}

/// HTTP client for interacting with the scheduler service.
#[derive(Debug, Clone)]
pub struct SchedulerClient {
    client: Client,
    base_url: String,
}

impl SchedulerClient {
    /// Create a new scheduler client from configuration.
    pub fn new(config: &SchedulerConfig) -> ControlResult<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()
            .map_err(ControlError::Http)?;

        Ok(Self {
            client,
            base_url: config.url.trim_end_matches('/').to_owned(),
        })
    }

    /// Create a new scheduler client with a custom base URL.
    pub fn with_url(url: impl Into<String>) -> ControlResult<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(ControlError::Http)?;

        Ok(Self {
            client,
            base_url: url.into().trim_end_matches('/').to_owned(),
        })
    }

    /// Check if the scheduler is healthy.
    pub async fn health(&self) -> ControlResult<HealthResponse> {
        let url = format!("{}/health", self.base_url);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(ControlError::Http)?;

        if !response.status().is_success() {
            return Err(ControlError::scheduler(format!(
                "health check failed: {}",
                response.status()
            )));
        }

        response.json().await.map_err(ControlError::Http)
    }

    /// Check if the scheduler is ready.
    pub async fn ready(&self) -> ControlResult<ReadyResponse> {
        let url = format!("{}/ready", self.base_url);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(ControlError::Http)?;

        response.json().await.map_err(ControlError::Http)
    }

    /// List all registered workers.
    pub async fn list_workers(&self) -> ControlResult<Vec<WorkerInfo>> {
        let url = format!("{}/workers", self.base_url);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(ControlError::Http)?;

        if !response.status().is_success() {
            return Err(ControlError::scheduler(format!(
                "failed to list workers: {}",
                response.status()
            )));
        }

        response.json().await.map_err(ControlError::Http)
    }

    /// Get a specific worker by ID.
    pub async fn get_worker(&self, worker_id: &str) -> ControlResult<Option<WorkerInfo>> {
        let url = format!("{}/workers/{}", self.base_url, worker_id);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(ControlError::Http)?;

        match response.status() {
            StatusCode::OK => response.json().await.map(Some).map_err(ControlError::Http),
            StatusCode::NOT_FOUND => Ok(None),
            status => Err(ControlError::scheduler(format!(
                "failed to get worker: {status}"
            ))),
        }
    }

    /// Remove a worker from the registry.
    pub async fn remove_worker(&self, worker_id: &str) -> ControlResult<()> {
        let url = format!("{}/workers/{}", self.base_url, worker_id);
        let response = self
            .client
            .delete(&url)
            .send()
            .await
            .map_err(ControlError::Http)?;

        match response.status() {
            StatusCode::NO_CONTENT => Ok(()),
            StatusCode::NOT_FOUND => Err(ControlError::scheduler(format!(
                "worker not found: {worker_id}"
            ))),
            status => Err(ControlError::scheduler(format!(
                "failed to remove worker: {status}"
            ))),
        }
    }

    /// Drain a worker (stop accepting new requests).
    pub async fn drain_worker(&self, worker_id: &str) -> ControlResult<()> {
        let url = format!("{}/workers/{}/drain", self.base_url, worker_id);
        let response = self
            .client
            .post(&url)
            .send()
            .await
            .map_err(ControlError::Http)?;

        match response.status() {
            StatusCode::ACCEPTED => Ok(()),
            StatusCode::NOT_FOUND => Err(ControlError::scheduler(format!(
                "worker not found: {worker_id}"
            ))),
            status => Err(ControlError::scheduler(format!(
                "failed to drain worker: {status}"
            ))),
        }
    }

    /// Get placement information for a function.
    pub async fn get_placement(&self, function: &str) -> ControlResult<PlacementInfo> {
        let url = format!("{}/placements/{}", self.base_url, function);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(ControlError::Http)?;

        if !response.status().is_success() {
            return Err(ControlError::scheduler(format!(
                "failed to get placement: {}",
                response.status()
            )));
        }

        let raw: RawPlacement = response.json().await.map_err(ControlError::Http)?;

        Ok(PlacementInfo {
            function: raw.function,
            status: PlacementStatus::parse(&raw.status),
            workers: raw.workers,
        })
    }

    /// List workers that can handle a specific function.
    ///
    /// This is a convenience method that gets the placement and returns only
    /// available workers.
    pub async fn list_workers_for_function(
        &self,
        function: &str,
    ) -> ControlResult<Vec<WorkerInfo>> {
        let placement = self.get_placement(function).await?;

        if placement.status != PlacementStatus::Available {
            return Ok(Vec::new());
        }

        let all_workers = self.list_workers().await?;
        let worker_ids: std::collections::HashSet<_> = placement
            .workers
            .iter()
            .map(|w| w.worker_id.as_str())
            .collect();

        Ok(all_workers
            .into_iter()
            .filter(|w| worker_ids.contains(w.id.as_str()))
            .collect())
    }

    /// List healthy workers.
    pub async fn list_healthy_workers(&self) -> ControlResult<Vec<WorkerInfo>> {
        let workers = self.list_workers().await?;
        Ok(workers.into_iter().filter(WorkerInfo::is_healthy).collect())
    }

    /// Wait for the scheduler to become ready.
    ///
    /// Returns an error if the scheduler is not ready within the timeout.
    pub async fn wait_ready(&self, timeout: Duration) -> ControlResult<()> {
        let start = std::time::Instant::now();
        let poll_interval = Duration::from_millis(500);

        loop {
            match self.ready().await {
                Ok(response) if response.ready => return Ok(()),
                Ok(_) => {}
                Err(_) if start.elapsed() < timeout => {}
                Err(e) => return Err(e),
            }

            if start.elapsed() >= timeout {
                return Err(ControlError::scheduler(
                    "scheduler not ready within timeout",
                ));
            }

            tokio::time::sleep(poll_interval).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::{WorkerCapacity, WorkerInfo};

    #[test]
    fn client_creation() {
        let config = SchedulerConfig::default();
        let client = SchedulerClient::new(&config);
        assert!(client.is_ok());
    }

    #[test]
    fn client_with_url() {
        let client = SchedulerClient::with_url("http://localhost:8082");
        assert!(client.is_ok());
    }

    #[test]
    fn placement_status_parsing() {
        assert_eq!(
            PlacementStatus::parse("available"),
            PlacementStatus::Available
        );
        assert_eq!(
            PlacementStatus::parse("provisioning"),
            PlacementStatus::Provisioning
        );
        assert_eq!(
            PlacementStatus::parse("all_unhealthy"),
            PlacementStatus::AllUnhealthy
        );
        assert_eq!(
            PlacementStatus::parse("not_found"),
            PlacementStatus::NotFound
        );
        assert_eq!(PlacementStatus::parse("unknown"), PlacementStatus::NotFound);
    }

    #[test]
    fn worker_info_status_checks() {
        let worker = WorkerInfo {
            id: "w1".to_owned(),
            address: "127.0.0.1:8080".to_owned(),
            vsock_cid: None,
            functions: vec!["greet".to_owned()],
            status: "Healthy".to_owned(),
            capacity: WorkerCapacity {
                max_concurrent: 10,
                current_load: 5,
                memory_mb: 512,
                memory_used_mb: 256,
            },
            registered_at_secs_ago: 100,
            last_heartbeat_secs_ago: 5,
        };

        assert!(worker.is_healthy());
        assert!(worker.is_available());
        assert!(!worker.is_draining());

        let draining = WorkerInfo {
            status: "Draining".to_owned(),
            ..worker.clone()
        };
        assert!(!draining.is_healthy());
        assert!(!draining.is_available());
        assert!(draining.is_draining());

        let degraded = WorkerInfo {
            status: "Degraded".to_owned(),
            ..worker
        };
        assert!(!degraded.is_healthy());
        assert!(degraded.is_available());
    }

    #[test]
    fn worker_capacity_calculations() {
        let capacity = WorkerCapacity {
            max_concurrent: 10,
            current_load: 5,
            memory_mb: 512,
            memory_used_mb: 256,
        };

        assert!((capacity.utilisation() - 0.5).abs() < f64::EPSILON);
        assert!(capacity.has_capacity());

        let full = WorkerCapacity {
            current_load: 10,
            ..capacity
        };
        assert!(!full.has_capacity());

        let zero = WorkerCapacity {
            max_concurrent: 0,
            current_load: 0,
            memory_mb: 512,
            memory_used_mb: 0,
        };
        assert!((zero.utilisation() - 0.0).abs() < f64::EPSILON);
    }
}
