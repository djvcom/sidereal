//! Transport abstraction for service communication.
//!
//! Provides a unified interface for both Unix socket and TCP transports,
//! allowing services to communicate via either mechanism.

mod tcp;
mod unix;

use std::net::SocketAddr;
use std::path::PathBuf;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};

pub use tcp::{TcpConnection, TcpListener};
pub use unix::{UnixConnection, UnixListener};

/// Errors that can occur during transport operations.
#[derive(Error, Debug)]
pub enum TransportError {
    /// I/O error from the underlying transport.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The transport address is invalid.
    #[error("Invalid address: {0}")]
    InvalidAddress(String),

    /// Connection was refused by the remote endpoint.
    #[error("Connection refused: {0}")]
    ConnectionRefused(String),

    /// The socket path does not exist.
    #[error("Socket not found: {0}")]
    SocketNotFound(String),
}

/// Result type for transport operations.
pub type Result<T> = std::result::Result<T, TransportError>;

/// Transport endpoint configuration.
///
/// Represents either a Unix socket path or a TCP address.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Transport {
    /// Unix domain socket transport.
    Unix {
        /// Path to the Unix socket.
        path: PathBuf,
    },
    /// TCP transport.
    Tcp {
        /// Socket address (IP and port).
        addr: SocketAddr,
    },
}

impl Transport {
    /// Creates a Unix socket transport.
    pub fn unix(path: impl Into<PathBuf>) -> Self {
        Self::Unix { path: path.into() }
    }

    /// Creates a TCP transport.
    pub const fn tcp(addr: SocketAddr) -> Self {
        Self::Tcp { addr }
    }

    /// Binds to the transport address and returns a listener.
    ///
    /// For Unix sockets, this creates the socket file.
    /// For TCP, this binds to the address and port.
    pub async fn bind(&self) -> Result<Box<dyn Listener>> {
        match self {
            Self::Unix { path } => {
                let listener = UnixListener::bind(path)?;
                Ok(Box::new(listener))
            }
            Self::Tcp { addr } => {
                let listener = TcpListener::bind(*addr).await?;
                Ok(Box::new(listener))
            }
        }
    }

    /// Connects to the transport address.
    ///
    /// Returns a connection that can be used for bidirectional communication.
    pub async fn connect(&self) -> Result<Box<dyn Connection>> {
        match self {
            Self::Unix { path } => {
                let conn = UnixConnection::connect(path).await?;
                Ok(Box::new(conn))
            }
            Self::Tcp { addr } => {
                let conn = TcpConnection::connect(*addr).await?;
                Ok(Box::new(conn))
            }
        }
    }

    /// Returns a human-readable description of the transport.
    pub fn display_address(&self) -> String {
        match self {
            Self::Unix { path } => format!("unix://{}", path.display()),
            Self::Tcp { addr } => format!("tcp://{addr}"),
        }
    }
}

impl std::fmt::Display for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_address())
    }
}

/// A listener that accepts incoming connections.
#[async_trait]
pub trait Listener: Send + Sync {
    /// Accepts a new incoming connection.
    async fn accept(&self) -> Result<Box<dyn Connection>>;

    /// Returns the local address this listener is bound to.
    fn local_addr(&self) -> Result<String>;
}

/// A bidirectional connection for communication.
///
/// Connections implement both `AsyncRead` and `AsyncWrite` for
/// streaming data in both directions.
pub trait Connection: AsyncRead + AsyncWrite + Send + Sync + Unpin {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_display() {
        let unix = Transport::unix("/run/sidereal/test.sock");
        assert_eq!(unix.to_string(), "unix:///run/sidereal/test.sock");

        let tcp = Transport::tcp("127.0.0.1:8080".parse().unwrap());
        assert_eq!(tcp.to_string(), "tcp://127.0.0.1:8080");
    }

    #[test]
    fn transport_serde() {
        let unix = Transport::unix("/tmp/test.sock");
        let json = serde_json::to_string(&unix).unwrap();
        assert!(json.contains("\"type\":\"unix\""));

        let tcp = Transport::tcp("127.0.0.1:8080".parse().unwrap());
        let json = serde_json::to_string(&tcp).unwrap();
        assert!(json.contains("\"type\":\"tcp\""));
    }
}
