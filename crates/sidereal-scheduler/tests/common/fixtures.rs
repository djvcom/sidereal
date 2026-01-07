//! Test fixtures for scheduler integration tests.

use sidereal_proto::TriggerType;
use sidereal_scheduler::registry::{WorkerCapacity, WorkerInfo, WorkerStatus};
use sidereal_scheduler::store::WorkerEndpoint;
use std::net::SocketAddr;
use std::time::Instant;

/// Builder for creating test WorkerInfo instances.
pub struct WorkerBuilder {
    id: String,
    address: SocketAddr,
    vsock_cid: Option<u32>,
    functions: Vec<String>,
    max_concurrent: u32,
    current_load: u32,
    memory_mb: u32,
    memory_used_mb: u32,
    status: WorkerStatus,
}

impl WorkerBuilder {
    /// Creates a new worker builder with the given ID.
    pub fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            address: "127.0.0.1:8080".parse().unwrap(),
            vsock_cid: None,
            functions: vec![],
            max_concurrent: 10,
            current_load: 0,
            memory_mb: 512,
            memory_used_mb: 0,
            status: WorkerStatus::Starting,
        }
    }

    /// Sets the worker's network address.
    pub fn with_address(mut self, addr: &str) -> Self {
        self.address = addr.parse().unwrap();
        self
    }

    /// Sets the worker's vsock CID.
    pub fn with_vsock_cid(mut self, cid: u32) -> Self {
        self.vsock_cid = Some(cid);
        self
    }

    /// Sets the functions this worker can handle.
    pub fn with_functions(mut self, functions: Vec<&str>) -> Self {
        self.functions = functions.into_iter().map(String::from).collect();
        self
    }

    /// Adds a single function to this worker.
    pub fn with_function(mut self, function: &str) -> Self {
        self.functions.push(function.to_string());
        self
    }

    /// Sets the worker's load (current and max concurrent).
    pub fn with_load(mut self, current: u32, max: u32) -> Self {
        self.current_load = current;
        self.max_concurrent = max;
        self
    }

    /// Sets the worker's memory (total and used).
    pub fn with_memory(mut self, total_mb: u32, used_mb: u32) -> Self {
        self.memory_mb = total_mb;
        self.memory_used_mb = used_mb;
        self
    }

    /// Sets the worker's status.
    pub fn with_status(mut self, status: WorkerStatus) -> Self {
        self.status = status;
        self
    }

    /// Sets the worker as healthy.
    pub fn healthy(mut self) -> Self {
        self.status = WorkerStatus::Healthy;
        self
    }

    /// Sets the worker as unhealthy.
    pub fn unhealthy(mut self) -> Self {
        self.status = WorkerStatus::Unhealthy;
        self
    }

    /// Builds the WorkerInfo.
    pub fn build(self) -> WorkerInfo {
        WorkerInfo {
            id: self.id,
            address: self.address,
            vsock_cid: self.vsock_cid,
            functions: self.functions,
            capacity: WorkerCapacity {
                max_concurrent: self.max_concurrent,
                current_load: self.current_load,
                memory_mb: self.memory_mb,
                memory_used_mb: self.memory_used_mb,
                cpu_millicores: 1000,
            },
            status: self.status,
            triggers: vec![TriggerType::Http],
            capabilities: vec![],
            metadata: vec![],
            last_heartbeat: Instant::now(),
            registered_at: Instant::now(),
            active_invocations: 0,
        }
    }

    /// Builds a WorkerEndpoint (for placement store tests).
    pub fn build_endpoint(self) -> WorkerEndpoint {
        WorkerEndpoint {
            worker_id: self.id,
            address: self.address,
            vsock_cid: self.vsock_cid,
            status: self.status,
        }
    }
}

/// Creates multiple workers for the same function with incrementing IDs.
pub fn create_workers(prefix: &str, count: usize, function: &str) -> Vec<WorkerInfo> {
    (0..count)
        .map(|i| {
            WorkerBuilder::new(&format!("{prefix}-{i}"))
                .with_function(function)
                .healthy()
                .build()
        })
        .collect()
}

/// Creates workers with varying loads for load balancing tests.
pub fn create_workers_with_loads(function: &str, loads: &[(u32, u32)]) -> Vec<WorkerInfo> {
    loads
        .iter()
        .enumerate()
        .map(|(i, &(current, max))| {
            WorkerBuilder::new(&format!("worker-{i}"))
                .with_function(function)
                .with_load(current, max)
                .healthy()
                .build()
        })
        .collect()
}
