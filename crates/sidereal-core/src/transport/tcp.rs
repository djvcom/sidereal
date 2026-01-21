//! TCP transport implementation.

use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener as TokioTcpListener, TcpStream};

use super::{Connection, Listener, Result, TransportError};

/// TCP listener that accepts incoming connections.
#[derive(Debug)]
pub struct TcpListener {
    inner: TokioTcpListener,
}

impl TcpListener {
    /// Binds to the given address.
    pub async fn bind(addr: SocketAddr) -> Result<Self> {
        let inner = TokioTcpListener::bind(addr).await?;
        Ok(Self { inner })
    }
}

#[async_trait]
impl Listener for TcpListener {
    async fn accept(&self) -> Result<Box<dyn Connection>> {
        let (stream, _addr) = self.inner.accept().await?;
        Ok(Box::new(TcpConnection { inner: stream }))
    }

    fn local_addr(&self) -> Result<String> {
        Ok(self.inner.local_addr()?.to_string())
    }
}

/// TCP connection for bidirectional communication.
#[derive(Debug)]
pub struct TcpConnection {
    inner: TcpStream,
}

impl TcpConnection {
    /// Connects to the given address.
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        let inner = TcpStream::connect(addr).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::ConnectionRefused {
                TransportError::ConnectionRefused(addr.to_string())
            } else {
                TransportError::Io(e)
            }
        })?;
        Ok(Self { inner })
    }
}

impl Connection for TcpConnection {}

impl AsyncRead for TcpConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for TcpConnection {
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
    async fn tcp_echo() {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap().parse().unwrap();

        let server = tokio::spawn(async move {
            let mut conn = listener.accept().await.unwrap();
            let mut buf = [0u8; 5];
            conn.read_exact(&mut buf).await.unwrap();
            conn.write_all(&buf).await.unwrap();
        });

        let mut client = TcpConnection::connect(addr).await.unwrap();
        client.write_all(b"hello").await.unwrap();
        let mut buf = [0u8; 5];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");

        server.await.unwrap();
    }
}
