//! Load balancing strategies for distributing requests across backends.

use std::sync::atomic::{AtomicUsize, Ordering};

use super::WorkerAddress;

/// Strategy for selecting a backend from a pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LoadBalanceStrategy {
    /// Select backends in order, cycling through the list.
    #[default]
    RoundRobin,
    /// Select a random backend for each request.
    Random,
}

/// Load balancer that distributes requests across multiple backends.
#[derive(Debug)]
pub struct LoadBalancer {
    strategy: LoadBalanceStrategy,
    counter: AtomicUsize,
}

impl LoadBalancer {
    /// Create a new load balancer with the given strategy.
    pub const fn new(strategy: LoadBalanceStrategy) -> Self {
        Self {
            strategy,
            counter: AtomicUsize::new(0),
        }
    }

    /// Select a backend from the given list.
    ///
    /// Returns `None` if the list is empty.
    pub fn select<'a>(&self, backends: &'a [WorkerAddress]) -> Option<&'a WorkerAddress> {
        if backends.is_empty() {
            return None;
        }

        if backends.len() == 1 {
            return backends.first();
        }

        let index = match self.strategy {
            LoadBalanceStrategy::RoundRobin => {
                let idx = self.counter.fetch_add(1, Ordering::Relaxed);
                idx % backends.len()
            }
            LoadBalanceStrategy::Random => {
                // Use a simple pseudo-random selection based on time
                #[allow(clippy::as_conversions)]
                let nanos = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as usize)
                    .unwrap_or(0);
                nanos % backends.len()
            }
        };

        backends.get(index)
    }

    /// Get the current strategy.
    pub const fn strategy(&self) -> LoadBalanceStrategy {
        self.strategy
    }
}

impl Default for LoadBalancer {
    fn default() -> Self {
        Self::new(LoadBalanceStrategy::RoundRobin)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_backends() -> Vec<WorkerAddress> {
        vec![
            WorkerAddress::Http {
                url: "http://backend1:8080".into(),
            },
            WorkerAddress::Http {
                url: "http://backend2:8080".into(),
            },
            WorkerAddress::Http {
                url: "http://backend3:8080".into(),
            },
        ]
    }

    #[test]
    fn empty_backends_returns_none() {
        let lb = LoadBalancer::default();
        let backends: Vec<WorkerAddress> = vec![];
        assert!(lb.select(&backends).is_none());
    }

    #[test]
    fn single_backend_always_selected() {
        let lb = LoadBalancer::default();
        let backends = vec![WorkerAddress::Http {
            url: "http://backend1:8080".into(),
        }];

        for _ in 0..10 {
            let selected = lb.select(&backends).unwrap();
            assert_eq!(
                selected,
                &WorkerAddress::Http {
                    url: "http://backend1:8080".into()
                }
            );
        }
    }

    #[test]
    fn round_robin_cycles_through_backends() {
        let lb = LoadBalancer::new(LoadBalanceStrategy::RoundRobin);
        let backends = test_backends();

        // Should cycle through 0, 1, 2, 0, 1, 2...
        for i in 0..9 {
            let selected = lb.select(&backends).unwrap();
            let expected = &backends[i % 3];
            assert_eq!(selected, expected, "Iteration {}", i);
        }
    }

    #[test]
    fn random_selects_from_pool() {
        let lb = LoadBalancer::new(LoadBalanceStrategy::Random);
        let backends = test_backends();

        // Just verify it selects valid backends
        for _ in 0..100 {
            let selected = lb.select(&backends).unwrap();
            assert!(backends.contains(selected));
        }
    }

    #[test]
    fn default_is_round_robin() {
        let lb = LoadBalancer::default();
        assert_eq!(lb.strategy(), LoadBalanceStrategy::RoundRobin);
    }
}
