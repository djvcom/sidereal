//! Network setup for the builder runtime.
//!
//! Provides loopback interface configuration and a vsock-to-TCP proxy bridge
//! so that cargo can fetch dependencies through the host's HTTP CONNECT proxy.
//!
//! Data flow:
//! ```text
//! cargo (HTTP_PROXY=http://127.0.0.1:1080)
//!   -> TCP 127.0.0.1:1080 (proxy bridge listener)
//!     -> vsock CID 2 port 1080 (Firecracker relays to host UDS)
//!       -> ProxyServer (host, validates domain against allowlist)
//!         -> TcpStream to upstream (crates.io, github.com, etc.)
//! ```

use std::net::SocketAddr;

use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_vsock::{VsockAddr, VsockStream};
use tracing::{debug, error, info};

/// Vsock CID for the host (always 2 in Firecracker).
const VMADDR_CID_HOST: u32 = 2;

/// Vsock port for S3/Garage forwarding (distinct from proxy port).
pub const S3_FORWARD_PORT: u32 = 3900;

/// Bring up the loopback (`lo`) interface so 127.0.0.1 is reachable.
///
/// The VM kernel does not auto-configure loopback; PID 1 must do it.
/// Uses raw `ioctl(SIOCSIFFLAGS)` with `IFF_UP` to avoid shelling out.
pub fn setup_loopback() -> std::io::Result<()> {
    #[allow(unsafe_code)]
    unsafe {
        let sock = libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0);
        if sock < 0 {
            return Err(std::io::Error::last_os_error());
        }

        let mut ifr: libc::ifreq = std::mem::zeroed();
        let name = b"lo\0";
        ifr.ifr_name[..name.len()].copy_from_slice(&name.map(|b| b as libc::c_char));
        ifr.ifr_ifru.ifru_flags = libc::IFF_UP as libc::c_short;

        #[cfg(target_env = "musl")]
        let request = libc::SIOCSIFFLAGS as libc::c_int;
        #[cfg(not(target_env = "musl"))]
        let request = libc::SIOCSIFFLAGS;
        let ret = libc::ioctl(sock, request, &ifr);
        libc::close(sock);

        if ret < 0 {
            return Err(std::io::Error::last_os_error());
        }
    }

    info!("Loopback interface brought up");
    Ok(())
}

/// Spawn a TCP listener that bridges each connection to the host's proxy via vsock.
///
/// Cargo connects to `listen_addr` (e.g. `127.0.0.1:1080`) and issues HTTP
/// CONNECT requests. Each accepted TCP connection is relayed byte-for-byte to
/// a `VsockStream` aimed at CID 2 (host) on `proxy_port`, where the
/// `ProxyServer` validates the domain and tunnels to the upstream.
///
/// Returns a `JoinHandle` for the listener task. The task runs until the VM
/// shuts down — no explicit cleanup is needed.
pub async fn start_proxy_bridge(
    listen_addr: SocketAddr,
    proxy_port: u32,
) -> std::io::Result<JoinHandle<()>> {
    let listener = TcpListener::bind(listen_addr).await?;
    info!(%listen_addr, proxy_port, "Proxy bridge listening");

    let handle = tokio::spawn(async move {
        loop {
            let (tcp_stream, peer) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    error!(error = %e, "Proxy bridge accept failed");
                    continue;
                }
            };

            debug!(%peer, "Proxy bridge accepted connection");

            tokio::spawn(async move {
                let vsock_addr = VsockAddr::new(VMADDR_CID_HOST, proxy_port);
                let vsock_stream = match VsockStream::connect(vsock_addr).await {
                    Ok(s) => s,
                    Err(e) => {
                        error!(error = %e, "Proxy bridge vsock connect failed");
                        return;
                    }
                };

                let (mut tcp_read, mut tcp_write) = tokio::io::split(tcp_stream);
                let (mut vsock_read, mut vsock_write) = tokio::io::split(vsock_stream);

                let client_to_host = tokio::io::copy(&mut tcp_read, &mut vsock_write);
                let host_to_client = tokio::io::copy(&mut vsock_read, &mut tcp_write);

                tokio::select! {
                    result = client_to_host => {
                        if let Err(e) = result {
                            debug!(error = %e, "Proxy bridge client->host ended");
                        }
                    }
                    result = host_to_client => {
                        if let Err(e) = result {
                            debug!(error = %e, "Proxy bridge host->client ended");
                        }
                    }
                }

                debug!(%peer, "Proxy bridge connection closed");
            });
        }
    });

    Ok(handle)
}

/// Start an S3/Garage bridge that forwards local port 3900 to the host.
///
/// This allows the VM to access Garage running on the host at 127.0.0.1:3900.
/// The S3 client in the VM connects to 127.0.0.1:3900 and traffic is bridged
/// via vsock to the host's S3 forwarder.
pub async fn start_s3_bridge(listen_port: u16) -> std::io::Result<JoinHandle<()>> {
    let listen_addr: SocketAddr = format!("127.0.0.1:{listen_port}").parse().unwrap();
    let listener = TcpListener::bind(listen_addr).await?;
    info!(%listen_addr, "S3 bridge listening");

    let handle = tokio::spawn(async move {
        loop {
            let (tcp_stream, peer) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    error!(error = %e, "S3 bridge accept failed");
                    continue;
                }
            };

            debug!(%peer, "S3 bridge accepted connection");

            tokio::spawn(async move {
                let vsock_addr = VsockAddr::new(VMADDR_CID_HOST, S3_FORWARD_PORT);
                let vsock_stream = match VsockStream::connect(vsock_addr).await {
                    Ok(s) => s,
                    Err(e) => {
                        error!(error = %e, "S3 bridge vsock connect failed");
                        return;
                    }
                };

                let (mut tcp_read, mut tcp_write) = tokio::io::split(tcp_stream);
                let (mut vsock_read, mut vsock_write) = tokio::io::split(vsock_stream);

                let client_to_host = tokio::io::copy(&mut tcp_read, &mut vsock_write);
                let host_to_client = tokio::io::copy(&mut vsock_read, &mut tcp_write);

                tokio::select! {
                    result = client_to_host => {
                        if let Err(e) = result {
                            debug!(error = %e, "S3 bridge client->host ended");
                        }
                    }
                    result = host_to_client => {
                        if let Err(e) = result {
                            debug!(error = %e, "S3 bridge host->client ended");
                        }
                    }
                }

                debug!(%peer, "S3 bridge connection closed");
            });
        }
    });

    Ok(handle)
}
