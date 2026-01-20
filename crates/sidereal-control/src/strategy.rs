//! Deployment strategies for different crash recovery approaches.
//!
//! This module defines the [`DeploymentStrategy`] enum which controls how
//! state transitions are persisted. Different strategies offer different
//! trade-offs between simplicity, performance, and crash recovery guarantees.

use serde::{Deserialize, Serialize};

/// Strategy for persisting deployment state transitions.
///
/// Each strategy offers different guarantees about crash recovery and
/// auditability. The strategy can be configured globally or per-deployment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentStrategy {
    /// Simple PostgreSQL transactions.
    ///
    /// State is updated within a single transaction. On crash, incomplete
    /// transactions are rolled back by the database.
    ///
    /// **Pros:**
    /// - Simple to implement and understand
    /// - Low overhead
    /// - Good enough for most use cases
    ///
    /// **Cons:**
    /// - Multi-step operations may leave partial state on crash
    /// - No automatic retry of interrupted operations
    ///
    /// **Use when:** You want simplicity and the default behaviour is acceptable.
    #[default]
    Simple,

    /// Intent log with replay on startup.
    ///
    /// Before each operation, an intent is written to a log. On startup,
    /// incomplete intents are replayed. This ensures that multi-step
    /// operations either complete fully or are retried.
    ///
    /// **Pros:**
    /// - Multi-step operations complete or retry
    /// - Clear audit trail of intended operations
    ///
    /// **Cons:**
    /// - Additional write per operation
    /// - Requires startup replay logic
    /// - Slightly more complex
    ///
    /// **Use when:** You need guaranteed completion of multi-step deployments.
    IntentLog,

    /// Full event sourcing.
    ///
    /// Instead of storing current state, events are stored and state is
    /// derived by replaying events. This provides a complete audit trail
    /// and allows point-in-time recovery.
    ///
    /// **Pros:**
    /// - Complete audit trail
    /// - Point-in-time state reconstruction
    /// - Natural support for event-driven architecture
    ///
    /// **Cons:**
    /// - More complex implementation
    /// - Requires event replay for queries
    /// - Higher storage requirements
    ///
    /// **Use when:** You need full auditability or event-driven integration.
    EventSourced,
}

impl DeploymentStrategy {
    /// Get the strategy name as a static string.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Simple => "simple",
            Self::IntentLog => "intent_log",
            Self::EventSourced => "event_sourced",
        }
    }

    /// Check if this strategy requires intent log support.
    #[must_use]
    pub const fn requires_intent_log(&self) -> bool {
        matches!(self, Self::IntentLog)
    }

    /// Check if this strategy requires event sourcing support.
    #[must_use]
    pub const fn requires_event_sourcing(&self) -> bool {
        matches!(self, Self::EventSourced)
    }
}

impl std::fmt::Display for DeploymentStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_simple() {
        assert_eq!(DeploymentStrategy::default(), DeploymentStrategy::Simple);
    }

    #[test]
    fn serde_roundtrip() {
        let strategies = [
            DeploymentStrategy::Simple,
            DeploymentStrategy::IntentLog,
            DeploymentStrategy::EventSourced,
        ];

        for strategy in strategies {
            let json = serde_json::to_string(&strategy).unwrap();
            let parsed: DeploymentStrategy = serde_json::from_str(&json).unwrap();
            assert_eq!(strategy, parsed);
        }
    }

    #[test]
    fn serde_from_string() {
        let simple: DeploymentStrategy = serde_json::from_str(r#""simple""#).unwrap();
        assert_eq!(simple, DeploymentStrategy::Simple);

        let intent: DeploymentStrategy = serde_json::from_str(r#""intent_log""#).unwrap();
        assert_eq!(intent, DeploymentStrategy::IntentLog);

        let event: DeploymentStrategy = serde_json::from_str(r#""event_sourced""#).unwrap();
        assert_eq!(event, DeploymentStrategy::EventSourced);
    }
}
