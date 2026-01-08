//! Worker registry for tracking registered workers.

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use sidereal_proto::{Capacity as ProtoCapacity, TriggerType, WorkerStatusProto};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Instant;

use crate::error::{Result, SchedulerError};

/// Unique worker identifier.
pub type WorkerId = String;

/// Worker registry.
///
/// Thread-safe registry for tracking all registered workers.
#[derive(Debug)]
pub struct WorkerRegistry {
    workers: DashMap<WorkerId, WorkerInfo>,
    /// Index of functions to workers that can handle them.
    function_workers: DashMap<String, HashSet<WorkerId>>,
}

impl WorkerRegistry {
    /// Creates a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            workers: DashMap::new(),
            function_workers: DashMap::new(),
        }
    }

    /// Registers a new worker.
    pub fn register(&self, info: WorkerInfo) -> Result<()> {
        let worker_id = info.id.clone();

        if self.workers.contains_key(&worker_id) {
            return Err(SchedulerError::WorkerAlreadyRegistered(worker_id));
        }

        // Update function index
        for function in &info.functions {
            self.function_workers
                .entry(function.clone())
                .or_default()
                .insert(worker_id.clone());
        }

        self.workers.insert(worker_id, info);
        Ok(())
    }

    /// Deregisters a worker.
    pub fn deregister(&self, worker_id: &str) -> Result<WorkerInfo> {
        let (_, info) = self
            .workers
            .remove(worker_id)
            .ok_or_else(|| SchedulerError::WorkerNotFound(worker_id.to_owned()))?;

        // Remove from function index
        for function in &info.functions {
            if let Some(mut workers) = self.function_workers.get_mut(function) {
                workers.remove(worker_id);
            }
        }

        Ok(info)
    }

    /// Gets a worker by ID.
    pub fn get(&self, worker_id: &str) -> Option<WorkerInfo> {
        self.workers.get(worker_id).map(|r| r.clone())
    }

    /// Updates a worker's status.
    pub fn update_status(&self, worker_id: &str, status: WorkerStatus) -> Result<()> {
        let mut worker = self
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| SchedulerError::WorkerNotFound(worker_id.to_owned()))?;

        worker.status = status;
        Ok(())
    }

    /// Updates a worker from a heartbeat.
    pub fn update_heartbeat(
        &self,
        worker_id: &str,
        current_load: u32,
        memory_used_mb: u32,
        active_invocations: u32,
    ) -> Result<()> {
        let mut worker = self
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| SchedulerError::WorkerNotFound(worker_id.to_owned()))?;

        worker.capacity.current_load = current_load;
        worker.capacity.memory_used_mb = memory_used_mb;
        worker.active_invocations = active_invocations;
        worker.last_heartbeat = Instant::now();

        Ok(())
    }

    /// Lists all workers.
    pub fn list_all(&self) -> Vec<WorkerInfo> {
        self.workers.iter().map(|r| r.value().clone()).collect()
    }

    /// Lists workers that can handle a function.
    pub fn list_for_function(&self, function: &str) -> Vec<WorkerInfo> {
        let worker_ids = match self.function_workers.get(function) {
            Some(ids) => ids.clone(),
            None => return Vec::new(),
        };

        worker_ids
            .iter()
            .filter_map(|id| self.workers.get(id).map(|r| r.clone()))
            .collect()
    }

    /// Lists healthy workers that can handle a function.
    pub fn list_healthy_for_function(&self, function: &str) -> Vec<WorkerInfo> {
        self.list_for_function(function)
            .into_iter()
            .filter(|w| w.status == WorkerStatus::Healthy)
            .collect()
    }

    /// Returns the number of registered workers.
    pub fn len(&self) -> usize {
        self.workers.len()
    }

    /// Returns true if no workers are registered.
    pub fn is_empty(&self) -> bool {
        self.workers.is_empty()
    }

    /// Returns all worker IDs.
    pub fn worker_ids(&self) -> Vec<WorkerId> {
        self.workers.iter().map(|r| r.key().clone()).collect()
    }
}

impl Default for WorkerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Worker information.
#[derive(Debug, Clone)]
pub struct WorkerInfo {
    /// Unique worker identifier.
    pub id: WorkerId,
    /// Network address for dispatch.
    pub address: SocketAddr,
    /// vsock CID if in Firecracker VM.
    pub vsock_cid: Option<u32>,
    /// Functions this worker can handle.
    pub functions: Vec<String>,
    /// Worker capacity.
    pub capacity: WorkerCapacity,
    /// Current status.
    pub status: WorkerStatus,
    /// Supported trigger types.
    pub triggers: Vec<TriggerType>,
    /// Worker capabilities (GPU, etc.).
    pub capabilities: Vec<String>,
    /// Metadata (version, build tags).
    pub metadata: Vec<(String, String)>,
    /// Time of last heartbeat.
    pub last_heartbeat: Instant,
    /// Time worker registered.
    pub registered_at: Instant,
    /// Active invocation count.
    pub active_invocations: u32,
}

impl WorkerInfo {
    /// Creates a new worker info from a registration request.
    pub fn from_register_request(
        request: &sidereal_proto::RegisterRequest,
        address: SocketAddr,
    ) -> Self {
        Self {
            id: request.worker_id.clone(),
            address,
            vsock_cid: request.vsock_cid,
            functions: request.functions.clone(),
            capacity: WorkerCapacity::from_proto(&request.capacity),
            status: WorkerStatus::Starting,
            triggers: request.triggers.clone(),
            capabilities: request.capabilities.clone(),
            metadata: request.metadata.clone(),
            last_heartbeat: Instant::now(),
            registered_at: Instant::now(),
            active_invocations: 0,
        }
    }

    /// Converts the status to the protocol representation.
    #[must_use]
    pub const fn status_proto(&self) -> WorkerStatusProto {
        self.status.to_proto()
    }
}

/// Worker capacity.
#[derive(Debug, Clone)]
pub struct WorkerCapacity {
    /// Maximum concurrent invocations.
    pub max_concurrent: u32,
    /// Current load (concurrent invocations).
    pub current_load: u32,
    /// Total memory in MB.
    pub memory_mb: u32,
    /// Memory used in MB.
    pub memory_used_mb: u32,
    /// CPU in millicores.
    pub cpu_millicores: u32,
}

impl WorkerCapacity {
    /// Creates capacity from protocol message.
    #[must_use]
    pub const fn from_proto(proto: &ProtoCapacity) -> Self {
        Self {
            max_concurrent: proto.max_concurrent,
            current_load: 0,
            memory_mb: proto.memory_mb,
            memory_used_mb: 0,
            cpu_millicores: proto.cpu_millicores,
        }
    }

    /// Returns the utilisation as a fraction (0.0-1.0).
    #[must_use]
    pub fn utilisation(&self) -> f64 {
        if self.max_concurrent == 0 {
            return 0.0;
        }
        f64::from(self.current_load) / f64::from(self.max_concurrent)
    }

    /// Returns true if the worker has available capacity.
    #[must_use]
    pub const fn has_capacity(&self) -> bool {
        self.current_load < self.max_concurrent
    }
}

impl Default for WorkerCapacity {
    fn default() -> Self {
        Self {
            max_concurrent: 10,
            current_load: 0,
            memory_mb: 512,
            memory_used_mb: 0,
            cpu_millicores: 1000,
        }
    }
}

/// Worker status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WorkerStatus {
    /// Worker is starting up.
    Starting,
    /// Worker is healthy and ready.
    Healthy,
    /// Worker is operational but degraded.
    Degraded,
    /// Worker is unhealthy.
    Unhealthy,
    /// Worker is draining.
    Draining,
    /// Worker has terminated.
    Terminated,
}

impl WorkerStatus {
    /// Converts from protocol representation.
    #[must_use]
    pub const fn from_proto(proto: WorkerStatusProto) -> Self {
        match proto {
            WorkerStatusProto::Starting => Self::Starting,
            WorkerStatusProto::Healthy => Self::Healthy,
            WorkerStatusProto::Degraded => Self::Degraded,
            WorkerStatusProto::Unhealthy => Self::Unhealthy,
            WorkerStatusProto::Draining => Self::Draining,
            WorkerStatusProto::Terminated => Self::Terminated,
        }
    }

    /// Converts to protocol representation.
    #[must_use]
    pub const fn to_proto(self) -> WorkerStatusProto {
        match self {
            Self::Starting => WorkerStatusProto::Starting,
            Self::Healthy => WorkerStatusProto::Healthy,
            Self::Degraded => WorkerStatusProto::Degraded,
            Self::Unhealthy => WorkerStatusProto::Unhealthy,
            Self::Draining => WorkerStatusProto::Draining,
            Self::Terminated => WorkerStatusProto::Terminated,
        }
    }

    /// Returns true if the worker can accept requests.
    #[must_use]
    pub const fn is_available(self) -> bool {
        matches!(self, Self::Healthy | Self::Degraded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_worker(id: &str, functions: Vec<&str>) -> WorkerInfo {
        WorkerInfo {
            id: id.to_string(),
            address: "127.0.0.1:8080".parse().unwrap(),
            vsock_cid: None,
            functions: functions.into_iter().map(String::from).collect(),
            capacity: WorkerCapacity::default(),
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
    fn register_and_get() {
        let registry = WorkerRegistry::new();
        let worker = make_worker("worker-1", vec!["greet", "farewell"]);

        registry.register(worker.clone()).unwrap();

        let retrieved = registry.get("worker-1").unwrap();
        assert_eq!(retrieved.id, "worker-1");
        assert_eq!(retrieved.functions.len(), 2);
    }

    #[test]
    fn duplicate_registration_fails() {
        let registry = WorkerRegistry::new();
        let worker = make_worker("worker-1", vec!["greet"]);

        registry.register(worker.clone()).unwrap();
        let result = registry.register(worker);

        assert!(matches!(
            result,
            Err(SchedulerError::WorkerAlreadyRegistered(_))
        ));
    }

    #[test]
    fn list_for_function() {
        let registry = WorkerRegistry::new();
        registry
            .register(make_worker("worker-1", vec!["greet", "farewell"]))
            .unwrap();
        registry
            .register(make_worker("worker-2", vec!["greet"]))
            .unwrap();
        registry
            .register(make_worker("worker-3", vec!["other"]))
            .unwrap();

        let greet_workers = registry.list_for_function("greet");
        assert_eq!(greet_workers.len(), 2);

        let other_workers = registry.list_for_function("other");
        assert_eq!(other_workers.len(), 1);

        let missing = registry.list_for_function("nonexistent");
        assert!(missing.is_empty());
    }

    #[test]
    fn deregister_removes_from_index() {
        let registry = WorkerRegistry::new();
        registry
            .register(make_worker("worker-1", vec!["greet"]))
            .unwrap();

        assert_eq!(registry.list_for_function("greet").len(), 1);

        registry.deregister("worker-1").unwrap();

        assert!(registry.list_for_function("greet").is_empty());
        assert!(registry.get("worker-1").is_none());
    }

    #[test]
    fn update_status() {
        let registry = WorkerRegistry::new();
        registry
            .register(make_worker("worker-1", vec!["greet"]))
            .unwrap();

        registry
            .update_status("worker-1", WorkerStatus::Draining)
            .unwrap();

        let worker = registry.get("worker-1").unwrap();
        assert_eq!(worker.status, WorkerStatus::Draining);
    }

    #[test]
    fn capacity_utilisation() {
        let mut capacity = WorkerCapacity::default();
        capacity.max_concurrent = 10;
        capacity.current_load = 7;

        assert!((capacity.utilisation() - 0.7).abs() < f64::EPSILON);
        assert!(capacity.has_capacity());

        capacity.current_load = 10;
        assert!(!capacity.has_capacity());
    }
}
