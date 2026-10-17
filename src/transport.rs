//! The IPC [`CustomTransport`] factory.

#![cfg(unix)]

use std::io;
use std::path::PathBuf;

use iroh::endpoint::transports::{CustomEndpoint, CustomTransport};
use iroh_base::CustomAddr;
use tokio::net::UnixDatagram;

use crate::addr::ipc_custom_addr;
use crate::endpoint::IpcEndpoint;

/// An iroh custom transport that binds a Unix `SOCK_DGRAM` socket at a fixed
/// path and serves it as a native iroh path for co-located peers.
///
/// Add it to an endpoint with
/// [`Builder::add_custom_transport`](iroh::endpoint::Builder::add_custom_transport),
/// advertise [`local_addr`](Self::local_addr) to peers (as a
/// [`TransportAddr::Custom`](iroh_base::TransportAddr::Custom)), and install
/// [`PreferIpcTransport`](crate::PreferIpcTransport) so iroh prefers the socket
/// for same-host peers.
#[derive(Debug, Clone)]
pub struct IpcTransport {
    socket_path: PathBuf,
    mode: Option<u32>,
}

impl IpcTransport {
    /// Create a transport that binds its datagram socket at `socket_path` when
    /// the iroh endpoint binds.
    ///
    /// An existing filesystem entry is never removed or replaced. Binding to
    /// an occupied path fails with [`io::ErrorKind::AddrInUse`], including when
    /// the entry is a socket left by an exited process. The owner must clean
    /// up that path explicitly after ensuring the previous endpoint is gone.
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
            mode: None,
        }
    }

    /// Set the filesystem mode applied to the socket file after binding
    /// (e.g. `0o666`).
    ///
    /// A Unix datagram `sendto` requires **write** permission on the
    /// destination socket *file*, so peers running as different users
    /// (a user CLI talking to a service-account daemon, and the daemon
    /// replying to the user's socket) need a mode wider than the bind
    /// default. The transport carries QUIC — the peer is authenticated
    /// by TLS, not by the socket mode — so widening the file mode adds
    /// no trust beyond reachability. Unset keeps the process default.
    #[must_use]
    pub fn with_mode(mut self, mode: u32) -> Self {
        self.mode = Some(mode);
        self
    }

    /// This transport's local IPC address — advertise it to peers so they can
    /// dial the socket.
    pub fn local_addr(&self) -> CustomAddr {
        ipc_custom_addr(&self.socket_path)
    }
}

/// Requested `SO_RCVBUF`/`SO_SNDBUF` for the datagram socket. QUIC bursts many
/// packets for a single co-located `put`; the default AF_UNIX receive buffer
/// (~208 KiB) overflows mid-burst, backpressures the sender, and triggers
/// QUIC PTO backoff (multi-second stalls). 8 MiB comfortably holds a
/// page-sized burst. Best-effort: the kernel clamps to `net.core.rmem_max` /
/// `wmem_max`, so on an untuned host this is capped — the batched `poll_recv`
/// drain is the primary fix; this is defense in depth.
const IPC_SOCKET_BUFFER_BYTES: usize = 8 * 1024 * 1024;

impl CustomTransport for IpcTransport {
    fn bind(&self) -> io::Result<Box<dyn CustomEndpoint>> {
        // The kernel grants pathname ownership through bind. Removing an
        // existing entry here steals a live endpoint's address and redirects
        // its peers to a different socket. A collision must fail untouched;
        // probing first would introduce a check/unlink/rebind race.
        let socket = UnixDatagram::bind(&self.socket_path)?;
        if let Some(mode) = self.mode {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&self.socket_path, std::fs::Permissions::from_mode(mode))?;
        }
        // Best-effort buffer enlargement; a failure just leaves the OS default,
        // so never fail `bind` over it.
        let sock = socket2::SockRef::from(&socket);
        let _ = sock.set_recv_buffer_size(IPC_SOCKET_BUFFER_BYTES);
        let _ = sock.set_send_buffer_size(IPC_SOCKET_BUFFER_BYTES);
        Ok(Box::new(IpcEndpoint::new(socket, self.socket_path.clone())))
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use iroh::endpoint::transports::CustomTransport;

    use super::IpcTransport;

    /// `with_mode(0o666)` must land on the bound socket file: a Unix
    /// datagram `sendto` needs write permission on the destination
    /// socket file, so cross-user peers (user CLI ↔ service-account
    /// daemon) break silently on the bind default.
    #[tokio::test]
    async fn with_mode_applies_to_bound_socket() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("mode.ipc.sock");
        let transport = IpcTransport::new(&path).with_mode(0o666);
        let _endpoint = transport.bind().expect("bind");
        let mode = std::fs::metadata(&path)
            .expect("metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o666, "socket file mode must be 0o666");
    }
}
