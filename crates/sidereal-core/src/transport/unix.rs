//! Unix socket transport implementation.

use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{UnixListener as TokioUnixListener, UnixStream};

use super::{Connection, Listener, Result, TransportError};

/// Unix socket listener that accepts incoming connections.
#[derive(Debug)]
pub struct UnixListener {
    inner: TokioUnixListener,
    path: std::path::PathBuf,
}

impl UnixListener {
    /// Binds to the given socket path.
    ///
    /// If the socket file already exists, it is removed first.
    pub fn bind(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        // Remove existing socket file if present
        if path.exists() {
            std::fs::remove_file(path)?;
        }

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let inner = TokioUnixListener::bind(path)?;
        Ok(Self {
            inner,
            path: path.to_path_buf(),
        })
    }
}

impl Drop for UnixListener {
    fn drop(&mut self) {
        // Clean up socket file on drop
        let _ = std::fs::remove_file(&self.path);
    }
}

#[async_trait]
impl Listener for UnixListener {
    async fn accept(&self) -> Result<Box<dyn Connection>> {
        let (stream, _addr) = self.inner.accept().await?;
        Ok(Box::new(UnixConnection { inner: stream }))
    }

    fn local_addr(&self) -> Result<String> {
        Ok(format!("unix://{}", self.path.display()))
    }
}

/// Unix socket connection for bidirectional communication.
#[derive(Debug)]
pub struct UnixConnection {
    inner: UnixStream,
}

impl UnixConnection {
    /// Connects to the given socket path.
    pub async fn connect(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        if !path.exists() {
            return Err(TransportError::SocketNotFound(path.display().to_string()));
        }

        let inner = UnixStream::connect(path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::ConnectionRefused {
                TransportError::ConnectionRefused(path.display().to_string())
            } else {
                TransportError::Io(e)
            }
        })?;
        Ok(Self { inner })
    }
}

impl Connection for UnixConnection {}

impl AsyncRead for UnixConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for UnixConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn unix_echo() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("test.sock");

        let listener = UnixListener::bind(&sock_path).unwrap();
        assert!(sock_path.exists());

        let server = tokio::spawn(async move {
            let mut conn = listener.accept().await.unwrap();
            let mut buf = [0u8; 5];
            conn.read_exact(&mut buf).await.unwrap();
            conn.write_all(&buf).await.unwrap();
        });

        let mut client = UnixConnection::connect(&sock_path).await.unwrap();
        client.write_all(b"hello").await.unwrap();
        let mut buf = [0u8; 5];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn socket_cleanup_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("cleanup.sock");

        {
            let _listener = UnixListener::bind(&sock_path).unwrap();
            assert!(sock_path.exists());
        }

        // Socket file should be removed after listener is dropped
        assert!(!sock_path.exists());
    }
}
