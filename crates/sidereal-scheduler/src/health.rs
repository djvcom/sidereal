//! Health tracking for workers.

use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::config::HealthConfig;
use crate::registry::{WorkerId, WorkerRegistry, WorkerStatus};

/// Tracks worker health based on heartbeats and active checks.
#[derive(Debug)]
pub struct HealthTracker {
    config: HealthConfig,
    health_data: DashMap<WorkerId, WorkerHealth>,
    registry: Arc<WorkerRegistry>,
}

impl HealthTracker {
    /// Creates a new health tracker.
    pub fn new(config: HealthConfig, registry: Arc<WorkerRegistry>) -> Self {
        Self {
            config,
            health_data: DashMap::new(),
            registry,
        }
    }

    /// Records a successful heartbeat from a worker.
    pub fn record_heartbeat(&self, worker_id: &str, latency: Duration) {
        let mut entry = self.health_data.entry(worker_id.to_owned()).or_default();

        entry.last_success = Some(Instant::now());
        entry.consecutive_failures = 0;
        entry.update_latency(latency);

        // If worker was unhealthy, check if it should become healthy again
        if entry.consecutive_successes >= self.config.healthy_threshold {
            let _ = self
                .registry
                .update_status(worker_id, WorkerStatus::Healthy);
        }
        entry.consecutive_successes += 1;
    }

    /// Records a failed health check for a worker.
    pub fn record_failure(&self, worker_id: &str) {
        let mut entry = self.health_data.entry(worker_id.to_owned()).or_default();

        entry.last_failure = Some(Instant::now());
        entry.consecutive_failures += 1;
        entry.consecutive_successes = 0;

        // Check if worker should be marked unhealthy
        if entry.consecutive_failures >= self.config.unhealthy_threshold {
            let _ = self
                .registry
                .update_status(worker_id, WorkerStatus::Unhealthy);
        }
    }

    /// Checks for workers that have missed heartbeats.
    pub fn check_heartbeat_timeouts(&self) -> Vec<WorkerId> {
        let now = Instant::now();
        let timeout = self.config.heartbeat_timeout;
        let mut timed_out = Vec::new();

        for entry in &self.health_data {
            let last_seen = entry.last_success.unwrap_or(entry.first_seen);
            if now.duration_since(last_seen) > timeout {
                timed_out.push(entry.key().clone());
            }
        }

        // Mark timed-out workers as unhealthy
        for worker_id in &timed_out {
            self.record_failure(worker_id);
        }

        timed_out
    }

    /// Gets health data for a worker.
    pub fn get(&self, worker_id: &str) -> Option<WorkerHealth> {
        self.health_data.get(worker_id).map(|r| r.clone())
    }

    /// Removes health data for a deregistered worker.
    pub fn remove(&self, worker_id: &str) {
        self.health_data.remove(worker_id);
    }

    /// Lists workers that need active ping checks.
    pub fn workers_needing_ping(&self) -> Vec<WorkerId> {
        let now = Instant::now();
        let interval = self.config.ping_interval;
        let mut need_ping = Vec::new();

        for worker_id in self.registry.worker_ids() {
            let should_ping = match self.health_data.get(&worker_id) {
                Some(health) => {
                    let last_check = health
                        .last_success
                        .or(health.last_failure)
                        .unwrap_or(health.first_seen);
                    now.duration_since(last_check) > interval
                }
                None => true,
            };

            if should_ping {
                need_ping.push(worker_id);
            }
        }

        need_ping
    }

    /// Returns the expected heartbeat interval for workers.
    pub const fn heartbeat_interval(&self) -> Duration {
        self.config.heartbeat_interval
    }
}

/// Health data for a single worker.
#[derive(Debug, Clone)]
pub struct WorkerHealth {
    /// First time this worker was seen.
    pub first_seen: Instant,
    /// Last successful health check.
    pub last_success: Option<Instant>,
    /// Last failed health check.
    pub last_failure: Option<Instant>,
    /// Consecutive failure count.
    pub consecutive_failures: u32,
    /// Consecutive success count.
    pub consecutive_successes: u32,
    /// Recent latencies (sliding window).
    latencies: Vec<Duration>,
    /// Error rate (0.0-1.0).
    pub error_rate: f64,
}

impl WorkerHealth {
    /// Updates latency tracking with a new sample.
    pub fn update_latency(&mut self, latency: Duration) {
        const MAX_SAMPLES: usize = 100;

        self.latencies.push(latency);
        if self.latencies.len() > MAX_SAMPLES {
            self.latencies.remove(0);
        }
    }

    /// Returns the p99 latency.
    #[must_use]
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss,
        clippy::as_conversions
    )]
    pub fn latency_p99(&self) -> Duration {
        if self.latencies.is_empty() {
            return Duration::ZERO;
        }

        let mut sorted = self.latencies.clone();
        sorted.sort();

        let index = ((sorted.len() as f64) * 0.99).ceil() as usize - 1;
        sorted
            .get(index.min(sorted.len() - 1))
            .copied()
            .unwrap_or(Duration::ZERO)
    }

    /// Returns the average latency.
    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::as_conversions)]
    pub fn latency_avg(&self) -> Duration {
        if self.latencies.is_empty() {
            return Duration::ZERO;
        }

        let total: Duration = self.latencies.iter().sum();
        total / self.latencies.len() as u32
    }
}

impl Default for WorkerHealth {
    fn default() -> Self {
        Self {
            first_seen: Instant::now(),
            last_success: None,
            last_failure: None,
            consecutive_failures: 0,
            consecutive_successes: 0,
            latencies: Vec::new(),
            error_rate: 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tracker() -> (HealthTracker, Arc<WorkerRegistry>) {
        let registry = Arc::new(WorkerRegistry::new());
        let config = HealthConfig::default();
        let tracker = HealthTracker::new(config, registry.clone());
        (tracker, registry)
    }

    #[test]
    fn record_heartbeat_success() {
        let (tracker, _) = make_tracker();

        tracker.record_heartbeat("worker-1", Duration::from_millis(50));

        let health = tracker.get("worker-1").unwrap();
        assert!(health.last_success.is_some());
        assert_eq!(health.consecutive_failures, 0);
        assert_eq!(health.consecutive_successes, 1);
    }

    #[test]
    fn record_failures_increases_count() {
        let (tracker, _) = make_tracker();

        tracker.record_failure("worker-1");
        tracker.record_failure("worker-1");
        tracker.record_failure("worker-1");

        let health = tracker.get("worker-1").unwrap();
        assert_eq!(health.consecutive_failures, 3);
    }

    #[test]
    fn latency_tracking() {
        let mut health = WorkerHealth::default();

        health.update_latency(Duration::from_millis(10));
        health.update_latency(Duration::from_millis(20));
        health.update_latency(Duration::from_millis(30));

        assert_eq!(health.latency_avg(), Duration::from_millis(20));
    }
}
