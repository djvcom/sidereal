//! Typestate pattern for deployment state machine.
//!
//! This module encodes deployment states in the type system, making invalid
//! state transitions a compile-time error rather than a runtime error.
//!
//! # Example
//!
//! ```ignore
//! let pending = Deployment::<Pending>::create(data);
//! let registering = pending.start_registering()?;
//! let active = registering.activate()?;
//! // active.start_registering() would not compile - invalid transition
//! ```

use std::marker::PhantomData;

use crate::error::{ControlError, ControlResult};
use crate::types::{DeploymentData, DeploymentId, PersistedState};

// =============================================================================
// State marker types (zero-sized)
// =============================================================================

/// Marker trait for deployment states.
pub trait DeploymentState: private::Sealed + Send + Sync {
    /// Get the persisted state representation.
    fn persisted() -> PersistedState;

    /// Get the state name for error messages.
    fn name() -> &'static str;
}

mod private {
    pub trait Sealed {}
}

/// Deployment created, waiting to start registration.
#[derive(Debug, Clone, Copy)]
pub struct Pending;

/// Registering functions with the scheduler.
#[derive(Debug, Clone, Copy)]
pub struct Registering;

/// Deployment is live and serving traffic.
#[derive(Debug, Clone, Copy)]
pub struct Active;

/// Deployment was replaced by a newer deployment.
#[derive(Debug, Clone, Copy)]
pub struct Superseded;

/// Deployment failed during registration or provisioning.
#[derive(Debug, Clone, Copy)]
pub struct Failed;

/// Deployment was explicitly terminated.
#[derive(Debug, Clone, Copy)]
pub struct Terminated;

// Implement the sealed trait
impl private::Sealed for Pending {}
impl private::Sealed for Registering {}
impl private::Sealed for Active {}
impl private::Sealed for Superseded {}
impl private::Sealed for Failed {}
impl private::Sealed for Terminated {}

// Implement DeploymentState for each state
impl DeploymentState for Pending {
    fn persisted() -> PersistedState {
        PersistedState::Pending
    }
    fn name() -> &'static str {
        "pending"
    }
}

impl DeploymentState for Registering {
    fn persisted() -> PersistedState {
        PersistedState::Registering
    }
    fn name() -> &'static str {
        "registering"
    }
}

impl DeploymentState for Active {
    fn persisted() -> PersistedState {
        PersistedState::Active
    }
    fn name() -> &'static str {
        "active"
    }
}

impl DeploymentState for Superseded {
    fn persisted() -> PersistedState {
        PersistedState::Superseded
    }
    fn name() -> &'static str {
        "superseded"
    }
}

impl DeploymentState for Failed {
    fn persisted() -> PersistedState {
        PersistedState::Failed
    }
    fn name() -> &'static str {
        "failed"
    }
}

impl DeploymentState for Terminated {
    fn persisted() -> PersistedState {
        PersistedState::Terminated
    }
    fn name() -> &'static str {
        "terminated"
    }
}

// =============================================================================
// Deployment struct parameterised by state
// =============================================================================

/// A deployment in a specific state.
///
/// The state parameter `S` determines which transitions are available.
/// Invalid transitions are caught at compile time.
#[derive(Debug)]
pub struct Deployment<S: DeploymentState> {
    /// The underlying deployment data.
    data: DeploymentData,
    /// Zero-sized state marker.
    _state: PhantomData<S>,
}

impl<S: DeploymentState> Deployment<S> {
    /// Get a reference to the deployment data.
    #[must_use]
    pub const fn data(&self) -> &DeploymentData {
        &self.data
    }

    /// Get the deployment ID.
    #[must_use]
    pub const fn id(&self) -> &DeploymentId {
        &self.data.id
    }

    /// Get the current state as a persisted value.
    #[must_use]
    pub fn state(&self) -> PersistedState {
        S::persisted()
    }

    /// Get the state name.
    #[must_use]
    pub fn state_name(&self) -> &'static str {
        S::name()
    }

    /// Convert into the underlying data (consuming the deployment).
    #[must_use]
    pub fn into_data(self) -> DeploymentData {
        self.data
    }

    /// Internal helper to transition to a new state.
    fn transition<T: DeploymentState>(self) -> Deployment<T> {
        Deployment {
            data: self.data,
            _state: PhantomData,
        }
    }

    /// Internal helper to transition with data modification.
    fn transition_with<T: DeploymentState>(
        mut self,
        f: impl FnOnce(&mut DeploymentData),
    ) -> Deployment<T> {
        f(&mut self.data);
        self.data.updated_at = chrono::Utc::now();
        Deployment {
            data: self.data,
            _state: PhantomData,
        }
    }
}

// =============================================================================
// State transitions
// =============================================================================

impl Deployment<Pending> {
    /// Create a new deployment in the pending state.
    #[must_use]
    pub const fn create(data: DeploymentData) -> Self {
        Self {
            data,
            _state: PhantomData,
        }
    }

    /// Transition to the registering state.
    ///
    /// This should be called when starting to register functions with the scheduler.
    #[must_use]
    pub fn start_registering(self) -> Deployment<Registering> {
        self.transition()
    }

    /// Transition directly to failed state.
    ///
    /// Use this when the deployment fails before registration starts.
    #[must_use]
    pub fn fail(self, error: String) -> Deployment<Failed> {
        self.transition_with(|data| {
            data.error = Some(error);
        })
    }
}

impl Deployment<Registering> {
    /// Transition to the active state.
    ///
    /// This should be called when all functions have been registered with the
    /// scheduler and the deployment is ready to serve traffic.
    #[must_use]
    pub fn activate(self) -> Deployment<Active> {
        self.transition()
    }

    /// Transition to the failed state.
    ///
    /// Use this when function registration fails.
    #[must_use]
    pub fn fail(self, error: String) -> Deployment<Failed> {
        self.transition_with(|data| {
            data.error = Some(error);
        })
    }
}

impl Deployment<Active> {
    /// Transition to the superseded state.
    ///
    /// This should be called when a newer deployment replaces this one.
    #[must_use]
    pub fn supersede(self) -> Deployment<Superseded> {
        self.transition()
    }

    /// Transition to the terminated state.
    ///
    /// Use this when explicitly tearing down the deployment.
    #[must_use]
    pub fn terminate(self) -> Deployment<Terminated> {
        self.transition()
    }
}

// =============================================================================
// Loading from persisted state
// =============================================================================

/// A type-erased deployment that can be in any state.
///
/// This is used when loading from the database where the state is not known
/// at compile time.
#[derive(Debug)]
pub enum AnyDeployment {
    /// Deployment in pending state.
    Pending(Deployment<Pending>),
    /// Deployment in registering state.
    Registering(Deployment<Registering>),
    /// Deployment in active state.
    Active(Deployment<Active>),
    /// Deployment in superseded state.
    Superseded(Deployment<Superseded>),
    /// Deployment in failed state.
    Failed(Deployment<Failed>),
    /// Deployment in terminated state.
    Terminated(Deployment<Terminated>),
}

impl AnyDeployment {
    /// Create an `AnyDeployment` from data and persisted state.
    #[must_use]
    pub const fn from_persisted(data: DeploymentData, state: PersistedState) -> Self {
        match state {
            PersistedState::Pending => Self::Pending(Deployment {
                data,
                _state: PhantomData,
            }),
            PersistedState::Registering => Self::Registering(Deployment {
                data,
                _state: PhantomData,
            }),
            PersistedState::Active => Self::Active(Deployment {
                data,
                _state: PhantomData,
            }),
            PersistedState::Superseded => Self::Superseded(Deployment {
                data,
                _state: PhantomData,
            }),
            PersistedState::Failed => Self::Failed(Deployment {
                data,
                _state: PhantomData,
            }),
            PersistedState::Terminated => Self::Terminated(Deployment {
                data,
                _state: PhantomData,
            }),
        }
    }

    /// Get a reference to the deployment data.
    #[must_use]
    pub const fn data(&self) -> &DeploymentData {
        match self {
            Self::Pending(d) => d.data(),
            Self::Registering(d) => d.data(),
            Self::Active(d) => d.data(),
            Self::Superseded(d) => d.data(),
            Self::Failed(d) => d.data(),
            Self::Terminated(d) => d.data(),
        }
    }

    /// Get the deployment ID.
    #[must_use]
    pub const fn id(&self) -> &DeploymentId {
        match self {
            Self::Pending(d) => d.id(),
            Self::Registering(d) => d.id(),
            Self::Active(d) => d.id(),
            Self::Superseded(d) => d.id(),
            Self::Failed(d) => d.id(),
            Self::Terminated(d) => d.id(),
        }
    }

    /// Get the current state.
    #[must_use]
    pub const fn state(&self) -> PersistedState {
        match self {
            Self::Pending(_) => PersistedState::Pending,
            Self::Registering(_) => PersistedState::Registering,
            Self::Active(_) => PersistedState::Active,
            Self::Superseded(_) => PersistedState::Superseded,
            Self::Failed(_) => PersistedState::Failed,
            Self::Terminated(_) => PersistedState::Terminated,
        }
    }

    /// Try to extract a pending deployment.
    ///
    /// Returns an error if the deployment is not in the pending state.
    pub fn try_into_pending(self) -> ControlResult<Deployment<Pending>> {
        match self {
            Self::Pending(d) => Ok(d),
            other => Err(ControlError::InvalidStateTransition {
                from: other.state().as_str(),
                to: "pending",
            }),
        }
    }

    /// Try to extract a registering deployment.
    ///
    /// Returns an error if the deployment is not in the registering state.
    pub fn try_into_registering(self) -> ControlResult<Deployment<Registering>> {
        match self {
            Self::Registering(d) => Ok(d),
            other => Err(ControlError::InvalidStateTransition {
                from: other.state().as_str(),
                to: "registering",
            }),
        }
    }

    /// Try to extract an active deployment.
    ///
    /// Returns an error if the deployment is not in the active state.
    pub fn try_into_active(self) -> ControlResult<Deployment<Active>> {
        match self {
            Self::Active(d) => Ok(d),
            other => Err(ControlError::InvalidStateTransition {
                from: other.state().as_str(),
                to: "active",
            }),
        }
    }

    /// Check if the deployment is in a terminal state (cannot transition further).
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Superseded(_) | Self::Failed(_) | Self::Terminated(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ProjectId;

    fn test_data() -> DeploymentData {
        DeploymentData::new(
            ProjectId::new("test-project"),
            "production".to_owned(),
            "abc123".to_owned(),
            "s3://bucket/artifact.rootfs".to_owned(),
            vec![],
        )
    }

    #[test]
    fn happy_path_transitions() {
        let pending = Deployment::<Pending>::create(test_data());
        assert_eq!(pending.state(), PersistedState::Pending);

        let registering = pending.start_registering();
        assert_eq!(registering.state(), PersistedState::Registering);

        let active = registering.activate();
        assert_eq!(active.state(), PersistedState::Active);

        let superseded = active.supersede();
        assert_eq!(superseded.state(), PersistedState::Superseded);
    }

    #[test]
    fn fail_from_pending() {
        let pending = Deployment::<Pending>::create(test_data());
        let failed = pending.fail("pre-registration failure".to_owned());
        assert_eq!(failed.state(), PersistedState::Failed);
        assert_eq!(
            failed.data().error.as_deref(),
            Some("pre-registration failure")
        );
    }

    #[test]
    fn fail_from_registering() {
        let pending = Deployment::<Pending>::create(test_data());
        let registering = pending.start_registering();
        let failed = registering.fail("scheduler unavailable".to_owned());
        assert_eq!(failed.state(), PersistedState::Failed);
    }

    #[test]
    fn terminate_active() {
        let pending = Deployment::<Pending>::create(test_data());
        let active = pending.start_registering().activate();
        let terminated = active.terminate();
        assert_eq!(terminated.state(), PersistedState::Terminated);
    }

    #[test]
    fn any_deployment_roundtrip() {
        let data = test_data();
        let id = data.id.clone();

        let any = AnyDeployment::from_persisted(data, PersistedState::Active);
        assert_eq!(any.state(), PersistedState::Active);
        assert_eq!(any.id(), &id);

        let active = any.try_into_active().unwrap();
        assert_eq!(active.id(), &id);
    }

    #[test]
    fn any_deployment_wrong_state() {
        let data = test_data();
        let any = AnyDeployment::from_persisted(data, PersistedState::Pending);

        let result = any.try_into_active();
        assert!(result.is_err());
    }
}
