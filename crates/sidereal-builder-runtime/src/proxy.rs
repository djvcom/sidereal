//! TCP-to-vsock proxy for cargo HTTP requests.
//!
//! Cargo expects HTTP proxies on TCP, but the host proxy listens on vsock.
//! This module provides a local TCP listener that forwards connections to
//! the host's vsock proxy.

use std::io;
use std::os::fd::IntoRawFd;
use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Host CID for vsock (always 2 for Firecracker).
const VMADDR_CID_HOST: u32 = 2;

/// Proxy port on the host.
const PROXY_PORT: u32 = 1080;

/// Local TCP port to listen on.
const LOCAL_PORT: u16 = 1080;

/// Run the TCP-to-vsock proxy.
///
/// This spawns a background task that accepts TCP connections on localhost
/// and forwards them to the host's vsock proxy.
pub async fn run_proxy() -> io::Result<()> {
    let listener = TcpListener::bind(("127.0.0.1", LOCAL_PORT)).await?;
    eprintln!("[proxy] Listening on 127.0.0.1:{LOCAL_PORT}");

    loop {
        match listener.accept().await {
            Ok((client, addr)) => {
                eprintln!("[proxy] Accepted connection from {addr}");
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(client).await {
                        eprintln!("[proxy] Connection error: {e}");
                    }
                });
            }
            Err(e) => {
                eprintln!("[proxy] Accept error: {e}");
            }
        }
    }
}

/// Handle a single proxy connection.
async fn handle_connection(mut client: TcpStream) -> io::Result<()> {
    // Connect to host via vsock
    let vsock = connect_vsock(VMADDR_CID_HOST, PROXY_PORT)?;

    // Convert to tokio async socket
    vsock.set_nonblocking(true)?;
    let vsock = tokio::net::UnixStream::from_std(vsock.into())?;

    // Relay data bidirectionally
    let (mut client_read, mut client_write) = client.split();
    let (mut vsock_read, mut vsock_write) = tokio::io::split(vsock);

    let client_to_vsock = async {
        let mut buf = [0u8; 8192];
        loop {
            let n = client_read.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            vsock_write.write_all(&buf[..n]).await?;
        }
        Ok::<_, io::Error>(())
    };

    let vsock_to_client = async {
        let mut buf = [0u8; 8192];
        loop {
            let n = vsock_read.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            client_write.write_all(&buf[..n]).await?;
        }
        Ok::<_, io::Error>(())
    };

    tokio::select! {
        result = client_to_vsock => result?,
        result = vsock_to_client => result?,
    }

    Ok(())
}

/// Connect to a vsock address.
fn connect_vsock(cid: u32, port: u32) -> io::Result<std::os::unix::net::UnixStream> {
    use libc::{connect, sa_family_t, sockaddr_vm, socket, AF_VSOCK, SOCK_STREAM};

    // Create vsock socket
    let fd = unsafe { socket(AF_VSOCK, SOCK_STREAM, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }

    // Safety: we just created this fd
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };

    // Set up the address
    let mut addr: sockaddr_vm = unsafe { std::mem::zeroed() };
    addr.svm_family = AF_VSOCK as sa_family_t;
    addr.svm_cid = cid;
    addr.svm_port = port;

    // Connect
    let result = unsafe {
        connect(
            fd.as_raw_fd(),
            std::ptr::from_ref(&addr).cast(),
            std::mem::size_of::<sockaddr_vm>() as u32,
        )
    };

    if result < 0 {
        return Err(io::Error::last_os_error());
    }

    // Convert to UnixStream (vsock fds are compatible)
    // Safety: the fd is a valid connected socket
    Ok(unsafe { std::os::unix::net::UnixStream::from_raw_fd(fd.into_raw_fd()) })
}

/// Get the proxy URL for cargo.
pub fn proxy_url() -> String {
    format!("http://127.0.0.1:{LOCAL_PORT}")
}
