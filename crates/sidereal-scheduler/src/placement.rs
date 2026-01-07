//! Placement algorithms for worker selection.

use dashmap::DashMap;
use parking_lot::Mutex;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::registry::{WorkerId, WorkerInfo};

/// Trait for placement algorithms.
pub trait PlacementAlgorithm: Send + Sync {
    /// Selects a worker from the available workers for a function.
    ///
    /// Returns `None` if no suitable worker is available.
    fn select_worker(&self, function: &str, workers: &[&WorkerInfo]) -> Option<WorkerId>;

    /// Returns the algorithm name.
    fn name(&self) -> &'static str;
}

/// Round-robin placement algorithm.
///
/// Simple algorithm that distributes requests evenly across workers.
#[derive(Debug, Default)]
pub struct RoundRobin {
    counters: DashMap<String, AtomicU64>,
}

impl RoundRobin {
    /// Creates a new round-robin placer.
    #[must_use]
    pub fn new() -> Self {
        Self {
            counters: DashMap::new(),
        }
    }
}

impl PlacementAlgorithm for RoundRobin {
    fn select_worker(&self, function: &str, workers: &[&WorkerInfo]) -> Option<WorkerId> {
        if workers.is_empty() {
            return None;
        }

        let counter = self
            .counters
            .entry(function.to_string())
            .or_insert_with(|| AtomicU64::new(0));

        let index = counter.fetch_add(1, Ordering::Relaxed) as usize % workers.len();
        Some(workers[index].id.clone())
    }

    fn name(&self) -> &'static str {
        "round_robin"
    }
}

/// Power-of-two-choices placement algorithm.
///
/// Selects two random workers and picks the one with lower load.
/// Provides better load distribution than round-robin with minimal overhead.
pub struct PowerOfTwoChoices {
    rng: Mutex<SmallRng>,
}

impl PowerOfTwoChoices {
    /// Creates a new power-of-two-choices placer.
    #[must_use]
    pub fn new() -> Self {
        Self {
            rng: Mutex::new(SmallRng::from_entropy()),
        }
    }
}

impl Default for PowerOfTwoChoices {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for PowerOfTwoChoices {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PowerOfTwoChoices").finish()
    }
}

impl PlacementAlgorithm for PowerOfTwoChoices {
    fn select_worker(&self, _function: &str, workers: &[&WorkerInfo]) -> Option<WorkerId> {
        if workers.is_empty() {
            return None;
        }

        if workers.len() == 1 {
            return Some(workers[0].id.clone());
        }

        let (a, b) = {
            let mut rng = self.rng.lock();
            let a = rng.gen_range(0..workers.len());
            let mut b = rng.gen_range(0..workers.len());
            // Ensure we pick two different workers if possible
            if b == a && workers.len() > 1 {
                b = (b + 1) % workers.len();
            }
            (a, b)
        };

        // Select the worker with lower load
        let worker = if workers[a].capacity.current_load <= workers[b].capacity.current_load {
            &workers[a]
        } else {
            &workers[b]
        };

        Some(worker.id.clone())
    }

    fn name(&self) -> &'static str {
        "power_of_two"
    }
}

/// Least-loaded placement algorithm.
///
/// Always selects the worker with the lowest current load.
/// Best for uneven workloads but requires scanning all workers.
#[derive(Debug, Default)]
pub struct LeastLoaded;

impl LeastLoaded {
    /// Creates a new least-loaded placer.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl PlacementAlgorithm for LeastLoaded {
    fn select_worker(&self, _function: &str, workers: &[&WorkerInfo]) -> Option<WorkerId> {
        workers
            .iter()
            .filter(|w| w.capacity.has_capacity())
            .min_by_key(|w| w.capacity.current_load)
            .map(|w| w.id.clone())
    }

    fn name(&self) -> &'static str {
        "least_loaded"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{WorkerCapacity, WorkerStatus};
    use sidereal_proto::TriggerType;
    use std::time::Instant;

    fn make_worker(id: &str, load: u32) -> WorkerInfo {
        WorkerInfo {
            id: id.to_string(),
            address: "127.0.0.1:8080".parse().unwrap(),
            vsock_cid: None,
            functions: vec!["test".to_string()],
            capacity: WorkerCapacity {
                max_concurrent: 10,
                current_load: load,
                memory_mb: 512,
                memory_used_mb: 0,
                cpu_millicores: 1000,
            },
            status: WorkerStatus::Healthy,
            triggers: vec![TriggerType::Http],
            capabilities: vec![],
            metadata: vec![],
            last_heartbeat: Instant::now(),
            registered_at: Instant::now(),
            active_invocations: 0,
        }
    }

    #[test]
    fn round_robin_distributes_evenly() {
        let rr = RoundRobin::new();
        let workers: Vec<WorkerInfo> = (0..3).map(|i| make_worker(&format!("w{i}"), 0)).collect();
        let worker_refs: Vec<&WorkerInfo> = workers.iter().collect();

        let mut counts = std::collections::HashMap::new();
        for _ in 0..300 {
            let id = rr.select_worker("test", &worker_refs).unwrap();
            *counts.entry(id).or_insert(0) += 1;
        }

        // Each worker should get ~100 requests
        for count in counts.values() {
            assert_eq!(*count, 100);
        }
    }

    #[test]
    fn round_robin_empty_returns_none() {
        let rr = RoundRobin::new();
        assert!(rr.select_worker("test", &[]).is_none());
    }

    #[test]
    fn power_of_two_prefers_less_loaded() {
        let p2 = PowerOfTwoChoices::new();
        let workers = vec![
            make_worker("w0", 8),
            make_worker("w1", 2), // Least loaded
            make_worker("w2", 5),
        ];
        let worker_refs: Vec<&WorkerInfo> = workers.iter().collect();

        // Run many iterations - w1 should be selected more often
        let mut counts = std::collections::HashMap::new();
        for _ in 0..1000 {
            let id = p2.select_worker("test", &worker_refs).unwrap();
            *counts.entry(id).or_insert(0) += 1;
        }

        // w1 (load=2) should be selected more than w0 (load=8)
        let w0_count = *counts.get("w0").unwrap_or(&0);
        let w1_count = *counts.get("w1").unwrap_or(&0);
        assert!(
            w1_count > w0_count,
            "w1={w1_count} should be > w0={w0_count}"
        );
    }

    #[test]
    fn least_loaded_picks_minimum() {
        let ll = LeastLoaded::new();
        let workers = vec![
            make_worker("w0", 8),
            make_worker("w1", 2),
            make_worker("w2", 5),
        ];
        let worker_refs: Vec<&WorkerInfo> = workers.iter().collect();

        let selected = ll.select_worker("test", &worker_refs).unwrap();
        assert_eq!(selected, "w1");
    }

    #[test]
    fn least_loaded_skips_full_workers() {
        let ll = LeastLoaded::new();
        let mut workers = vec![
            make_worker("w0", 10), // Full
            make_worker("w1", 5),
        ];
        workers[0].capacity.max_concurrent = 10;
        workers[1].capacity.max_concurrent = 10;

        let worker_refs: Vec<&WorkerInfo> = workers.iter().collect();

        let selected = ll.select_worker("test", &worker_refs).unwrap();
        assert_eq!(selected, "w1");
    }
}
