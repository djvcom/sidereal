//! Circuit breaker implementation for backend resilience.
//!
//! The circuit breaker prevents cascading failures by temporarily stopping
//! requests to failing backends. It has three states:
//!
//! - **Closed**: Normal operation, requests pass through
//! - **Open**: Backend is failing, requests are rejected immediately
//! - **HalfOpen**: Testing if backend has recovered

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

use crate::config::CircuitBreakerConfig;
use crate::error::GatewayError;

/// Circuit breaker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation - requests pass through.
    Closed,
    /// Backend is failing - requests are rejected.
    Open,
    /// Testing recovery - limited requests pass through.
    HalfOpen,
}

/// Circuit breaker for a single backend.
#[derive(Debug)]
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: RwLock<CircuitState>,
    failure_count: AtomicU32,
    success_count: AtomicU32,
    last_failure_time: AtomicU64,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with the given configuration.
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: RwLock::new(CircuitState::Closed),
            failure_count: AtomicU32::new(0),
            success_count: AtomicU32::new(0),
            last_failure_time: AtomicU64::new(0),
        }
    }

    /// Check if a request should be allowed through.
    pub async fn allow_request(&self) -> Result<(), GatewayError> {
        let state = *self.state.read().await;

        match state {
            CircuitState::Closed => Ok(()),
            CircuitState::Open => {
                // Check if reset timeout has elapsed
                let last_failure = self.last_failure_time.load(Ordering::Relaxed);
                let now = Instant::now().elapsed().as_millis() as u64;
                let elapsed_ms = now.saturating_sub(last_failure);

                if elapsed_ms >= self.config.reset_timeout_ms as u64 {
                    // Transition to half-open
                    let mut state_guard = self.state.write().await;
                    if *state_guard == CircuitState::Open {
                        *state_guard = CircuitState::HalfOpen;
                        self.success_count.store(0, Ordering::Relaxed);
                        tracing::info!("Circuit breaker transitioning to half-open");
                    }
                    Ok(())
                } else {
                    Err(GatewayError::CircuitOpen)
                }
            }
            CircuitState::HalfOpen => Ok(()),
        }
    }

    /// Record a successful request.
    pub async fn record_success(&self) {
        let state = *self.state.read().await;

        match state {
            CircuitState::Closed => {
                // Reset failure count on success
                self.failure_count.store(0, Ordering::Relaxed);
            }
            CircuitState::HalfOpen => {
                let count = self.success_count.fetch_add(1, Ordering::Relaxed) + 1;
                if count >= self.config.success_threshold {
                    // Transition to closed
                    let mut state_guard = self.state.write().await;
                    if *state_guard == CircuitState::HalfOpen {
                        *state_guard = CircuitState::Closed;
                        self.failure_count.store(0, Ordering::Relaxed);
                        self.success_count.store(0, Ordering::Relaxed);
                        tracing::info!("Circuit breaker closed after successful recovery");
                    }
                }
            }
            CircuitState::Open => {
                // Shouldn't happen, but reset if it does
                self.failure_count.store(0, Ordering::Relaxed);
            }
        }
    }

    /// Record a failed request.
    pub async fn record_failure(&self) {
        let state = *self.state.read().await;

        match state {
            CircuitState::Closed => {
                let count = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
                if count >= self.config.failure_threshold {
                    // Transition to open
                    let mut state_guard = self.state.write().await;
                    if *state_guard == CircuitState::Closed {
                        *state_guard = CircuitState::Open;
                        self.last_failure_time.store(
                            Instant::now().elapsed().as_millis() as u64,
                            Ordering::Relaxed,
                        );
                        tracing::warn!(
                            failure_count = count,
                            "Circuit breaker opened due to failures"
                        );
                    }
                }
            }
            CircuitState::HalfOpen => {
                // Any failure in half-open immediately opens the circuit
                let mut state_guard = self.state.write().await;
                if *state_guard == CircuitState::HalfOpen {
                    *state_guard = CircuitState::Open;
                    self.last_failure_time.store(
                        Instant::now().elapsed().as_millis() as u64,
                        Ordering::Relaxed,
                    );
                    self.success_count.store(0, Ordering::Relaxed);
                    tracing::warn!("Circuit breaker reopened after failure in half-open state");
                }
            }
            CircuitState::Open => {
                // Update last failure time
                self.last_failure_time.store(
                    Instant::now().elapsed().as_millis() as u64,
                    Ordering::Relaxed,
                );
            }
        }
    }

    /// Get the current state.
    pub async fn state(&self) -> CircuitState {
        *self.state.read().await
    }

    /// Get the current failure count.
    pub fn failure_count(&self) -> u32 {
        self.failure_count.load(Ordering::Relaxed)
    }
}

/// Registry of circuit breakers keyed by backend address.
#[derive(Debug)]
pub struct CircuitBreakerRegistry {
    config: CircuitBreakerConfig,
    breakers: dashmap::DashMap<String, Arc<CircuitBreaker>>,
}

impl CircuitBreakerRegistry {
    /// Create a new registry with the given configuration.
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            breakers: dashmap::DashMap::new(),
        }
    }

    /// Get or create a circuit breaker for the given backend key.
    pub fn get_or_create(&self, key: &str) -> Arc<CircuitBreaker> {
        if let Some(breaker) = self.breakers.get(key) {
            return breaker.clone();
        }

        let breaker = Arc::new(CircuitBreaker::new(self.config.clone()));
        self.breakers.insert(key.to_string(), breaker.clone());
        breaker
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> CircuitBreakerConfig {
        CircuitBreakerConfig {
            failure_threshold: 3,
            success_threshold: 2,
            reset_timeout_ms: 1000,
        }
    }

    #[tokio::test]
    async fn starts_closed() {
        let cb = CircuitBreaker::new(test_config());
        assert_eq!(cb.state().await, CircuitState::Closed);
        assert!(cb.allow_request().await.is_ok());
    }

    #[tokio::test]
    async fn opens_after_failures() {
        let cb = CircuitBreaker::new(test_config());

        // Record failures up to threshold
        cb.record_failure().await;
        assert_eq!(cb.state().await, CircuitState::Closed);

        cb.record_failure().await;
        assert_eq!(cb.state().await, CircuitState::Closed);

        cb.record_failure().await;
        assert_eq!(cb.state().await, CircuitState::Open);

        // Should reject requests
        assert!(matches!(
            cb.allow_request().await,
            Err(GatewayError::CircuitOpen)
        ));
    }

    #[tokio::test]
    async fn success_resets_failure_count() {
        let cb = CircuitBreaker::new(test_config());

        cb.record_failure().await;
        cb.record_failure().await;
        assert_eq!(cb.failure_count(), 2);

        cb.record_success().await;
        assert_eq!(cb.failure_count(), 0);
    }

    #[tokio::test]
    async fn registry_creates_breakers() {
        let registry = CircuitBreakerRegistry::new(test_config());

        let breaker1 = registry.get_or_create("backend-1");
        let breaker2 = registry.get_or_create("backend-1");
        let breaker3 = registry.get_or_create("backend-2");

        // Same key returns same breaker
        assert!(Arc::ptr_eq(&breaker1, &breaker2));

        // Different key returns different breaker
        assert!(!Arc::ptr_eq(&breaker1, &breaker3));
    }
}
