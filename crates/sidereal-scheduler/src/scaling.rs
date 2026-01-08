//! Scaling policies for worker autoscaling.

use parking_lot::RwLock;
use std::time::{Duration, Instant};

use crate::config::ScalingConfig;

/// Scaling policy for autoscaling decisions.
#[derive(Debug)]
pub struct ScalingPolicy {
    config: ScalingConfig,
    last_scale_up: RwLock<Option<Instant>>,
    last_scale_down: RwLock<Option<Instant>>,
}

impl ScalingPolicy {
    /// Creates a new scaling policy.
    pub const fn new(config: ScalingConfig) -> Self {
        Self {
            config,
            last_scale_up: RwLock::new(None),
            last_scale_down: RwLock::new(None),
        }
    }

    /// Evaluates whether to scale based on cluster metrics.
    pub fn evaluate(&self, metrics: &ClusterMetrics) -> ScalingDecision {
        let now = Instant::now();

        // Check cooldowns
        {
            let last_scale_up = self.last_scale_up.read();
            if let Some(last) = *last_scale_up {
                if now.duration_since(last) < self.config.scale_up_cooldown {
                    return ScalingDecision::NoChange {
                        reason: "scale-up cooldown active",
                    };
                }
            }
        }

        {
            let last_scale_down = self.last_scale_down.read();
            if let Some(last) = *last_scale_down {
                if now.duration_since(last) < self.config.scale_down_cooldown {
                    return ScalingDecision::NoChange {
                        reason: "scale-down cooldown active",
                    };
                }
            }
        }

        // Calculate utilisation
        let utilisation = if metrics.total_capacity > 0 {
            f64::from(metrics.total_load) / f64::from(metrics.total_capacity)
        } else {
            // No capacity, definitely need to scale up
            1.0
        };

        // Check if we need to scale up
        if utilisation > self.config.scale_up_threshold {
            if metrics.worker_count >= self.config.max_workers {
                return ScalingDecision::NoChange {
                    reason: "already at max workers",
                };
            }

            #[allow(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                clippy::as_conversions
            )]
            let target_capacity =
                (f64::from(metrics.total_load) / self.config.target_utilisation).ceil() as u32;
            let needed_additional = target_capacity.saturating_sub(metrics.total_capacity);

            // Estimate workers needed (assuming average capacity)
            let avg_capacity = if metrics.worker_count > 0 {
                metrics.total_capacity / metrics.worker_count
            } else {
                10 // Default assumption
            };

            let workers_needed = (needed_additional / avg_capacity).max(1);
            let to_add = workers_needed
                .min(self.config.scale_up_step)
                .min(self.config.max_workers - metrics.worker_count);

            if to_add > 0 {
                *self.last_scale_up.write() = Some(now);
                return ScalingDecision::ScaleUp {
                    count: to_add,
                    reason: format!(
                        "utilisation {utilisation:.2} > threshold {}",
                        self.config.scale_up_threshold
                    ),
                };
            }
        }

        // Check if we can scale down
        if utilisation < self.config.scale_down_threshold {
            if metrics.worker_count <= self.config.min_workers {
                return ScalingDecision::NoChange {
                    reason: "already at min workers",
                };
            }

            let to_remove = self
                .config
                .scale_down_step
                .min(metrics.worker_count - self.config.min_workers);

            if to_remove > 0 {
                *self.last_scale_down.write() = Some(now);
                return ScalingDecision::ScaleDown {
                    count: to_remove,
                    reason: format!(
                        "utilisation {utilisation:.2} < threshold {}",
                        self.config.scale_down_threshold
                    ),
                };
            }
        }

        ScalingDecision::NoChange {
            reason: "utilisation within thresholds",
        }
    }

    /// Resets cooldowns (for testing).
    #[cfg(test)]
    pub fn reset_cooldowns(&self) {
        *self.last_scale_up.write() = None;
        *self.last_scale_down.write() = None;
    }

    /// Returns the configuration.
    pub const fn config(&self) -> &ScalingConfig {
        &self.config
    }
}

/// Scaling decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalingDecision {
    /// Scale up by adding workers.
    ScaleUp { count: u32, reason: String },
    /// Scale down by removing workers.
    ScaleDown { count: u32, reason: String },
    /// No change needed.
    NoChange { reason: &'static str },
}

impl ScalingDecision {
    /// Returns true if this is a scale-up decision.
    #[must_use]
    pub const fn is_scale_up(&self) -> bool {
        matches!(self, Self::ScaleUp { .. })
    }

    /// Returns true if this is a scale-down decision.
    #[must_use]
    pub const fn is_scale_down(&self) -> bool {
        matches!(self, Self::ScaleDown { .. })
    }

    /// Returns true if no change is needed.
    #[must_use]
    pub const fn is_no_change(&self) -> bool {
        matches!(self, Self::NoChange { .. })
    }
}

/// Cluster-wide metrics for scaling decisions.
#[derive(Debug, Clone, Default)]
pub struct ClusterMetrics {
    /// Number of workers.
    pub worker_count: u32,
    /// Total capacity across all workers.
    pub total_capacity: u32,
    /// Total current load.
    pub total_load: u32,
    /// Pending invocations waiting for capacity.
    pub pending_invocations: u32,
    /// Average latency.
    pub avg_latency: Duration,
}

impl ClusterMetrics {
    /// Calculates utilisation as a fraction (0.0-1.0).
    #[must_use]
    pub fn utilisation(&self) -> f64 {
        if self.total_capacity == 0 {
            return 0.0;
        }
        f64::from(self.total_load) / f64::from(self.total_capacity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_policy() -> ScalingPolicy {
        ScalingPolicy::new(ScalingConfig {
            min_workers: 1,
            max_workers: 10,
            target_utilisation: 0.7,
            scale_up_threshold: 0.8,
            scale_down_threshold: 0.3,
            scale_up_cooldown: Duration::from_secs(0), // Disable for tests
            scale_down_cooldown: Duration::from_secs(0),
            scale_up_step: 2,
            scale_down_step: 1,
        })
    }

    #[test]
    fn scale_up_when_overloaded() {
        let policy = make_policy();
        let metrics = ClusterMetrics {
            worker_count: 3,
            total_capacity: 30,
            total_load: 27, // 90% utilisation
            ..Default::default()
        };

        let decision = policy.evaluate(&metrics);
        assert!(decision.is_scale_up());
    }

    #[test]
    fn scale_down_when_underloaded() {
        let policy = make_policy();
        let metrics = ClusterMetrics {
            worker_count: 5,
            total_capacity: 50,
            total_load: 10, // 20% utilisation
            ..Default::default()
        };

        let decision = policy.evaluate(&metrics);
        assert!(decision.is_scale_down());
    }

    #[test]
    fn no_change_when_within_thresholds() {
        let policy = make_policy();
        let metrics = ClusterMetrics {
            worker_count: 3,
            total_capacity: 30,
            total_load: 15, // 50% utilisation
            ..Default::default()
        };

        let decision = policy.evaluate(&metrics);
        assert!(decision.is_no_change());
    }

    #[test]
    fn respects_min_workers() {
        let policy = make_policy();
        let metrics = ClusterMetrics {
            worker_count: 1, // At minimum
            total_capacity: 10,
            total_load: 1, // 10% utilisation
            ..Default::default()
        };

        let decision = policy.evaluate(&metrics);
        assert!(decision.is_no_change());
    }

    #[test]
    fn respects_max_workers() {
        let policy = make_policy();
        let metrics = ClusterMetrics {
            worker_count: 10, // At maximum
            total_capacity: 100,
            total_load: 95, // 95% utilisation
            ..Default::default()
        };

        let decision = policy.evaluate(&metrics);
        assert!(decision.is_no_change());
    }

    #[test]
    fn cooldown_prevents_rapid_scaling() {
        let policy = ScalingPolicy::new(ScalingConfig {
            scale_up_cooldown: Duration::from_secs(60),
            ..make_policy().config().clone()
        });

        let metrics = ClusterMetrics {
            worker_count: 3,
            total_capacity: 30,
            total_load: 27,
            ..Default::default()
        };

        // First scale-up should succeed
        let decision = policy.evaluate(&metrics);
        assert!(decision.is_scale_up());

        // Second scale-up should be blocked by cooldown
        let decision = policy.evaluate(&metrics);
        assert!(decision.is_no_change());
    }
}
