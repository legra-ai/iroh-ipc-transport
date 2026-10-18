//! Filesystem rendezvous, with leases instead of socket liveness probes.

use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use iroh::{EndpointId, TransportAddr};
use iroh_base::CustomAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::IPC_TRANSPORT_ID;
use crate::locking::Lock;

/// Local rendezvous directory shared by cooperating processes.
///
/// Records are useful only while their owner's advisory lease is held. No
/// networking, DNS, PID inspection, or Unix-socket connect is used to discover
/// a peer. The directory must be on a local filesystem supporting file locks.
/// QUIC authenticates peers; directory access alone grants no peer identity.
#[derive(Debug, Clone)]
pub struct LocalDiscovery {
    root: PathBuf,
}

impl LocalDiscovery {
    /// Choose a rendezvous directory. Creation happens asynchronously on bind.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Resolve an actively leased local endpoint, or `None` if absent/stale.
    /// Malformed records and filesystem failures are returned as errors.
    pub async fn resolve(&self, id: EndpointId) -> io::Result<Option<TransportAddr>> {
        if !tokio::fs::try_exists(&self.root).await? {
            return Ok(None);
        }
        let _guard = self.guard().await?;
        let path = self.record_path(id);
        match Lock::open(&path, false).await {
            Ok(_) => return Ok(None), // The previous process released its lease.
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
        }
        let mut file = tokio::fs::File::open(path).await?;
        // bounded: a transport address is at most 512 bytes, plus one overflow byte.
        let mut bytes = Vec::with_capacity(513);
        (&mut file).take(513).read_to_end(&mut bytes).await?;
        let addr = CustomAddr::from_parts(IPC_TRANSPORT_ID, &bytes);
        if bytes.len() > 512 || !crate::backend::Backend::valid_addr(&addr) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid IPC rendezvous record",
            ));
        }
        Ok(Some(TransportAddr::Custom(addr)))
    }

    pub(crate) fn record_path(&self, id: EndpointId) -> PathBuf {
        self.root.join(format!("{id}.ipc"))
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn socket_path(&self, id: EndpointId) -> PathBuf {
        self.root.join(format!("{id}.sock"))
    }

    pub(crate) async fn guard(&self) -> io::Result<Lock> {
        tokio::fs::create_dir_all(&self.root).await?;
        let path = self.root.join(".ipc.lock");
        // This single persistent lock serializes pathname creation/removal, so
        // nobody can acquire a lease on an unlinked inode. It is never removed.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match Lock::open(&path, true).await {
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                    result => return result,
                }
            }
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "IPC directory lock timeout"))?
    }
}

#[derive(Debug)]
pub(crate) struct Lease {
    discovery: LocalDiscovery,
    id: EndpointId,
    lock: Lock,
    #[cfg(target_os = "linux")]
    socket: Option<PathBuf>,
}

impl Lease {
    // Caller holds the directory guard until publication or failure cleanup.
    pub(crate) async fn acquire(discovery: LocalDiscovery, id: EndpointId) -> io::Result<Self> {
        let lock = Lock::open(&discovery.record_path(id), true)
            .await
            .map_err(|error| {
                if error.kind() == io::ErrorKind::WouldBlock {
                    io::Error::new(io::ErrorKind::AddrInUse, "IPC identity already bound")
                } else {
                    error
                }
            })?;
        #[cfg(target_os = "linux")]
        {
            // Only a valid record written by this transport grants ownership
            // of a stale socket pathname. Never delete an unrelated entry.
            let mut file = tokio::fs::File::open(discovery.record_path(id)).await?;
            let mut bytes = [0u8; 513];
            let count = file.read(&mut bytes).await?;
            let old = CustomAddr::from_parts(IPC_TRANSPORT_ID, &bytes[..count]);
            if crate::backend::Backend::path(&old).as_deref()
                == Some(discovery.socket_path(id).as_path())
            {
                remove_if_present(&discovery.socket_path(id)).await?;
            }
        }
        Ok(Self {
            discovery,
            id,
            lock,
            #[cfg(target_os = "linux")]
            socket: None,
        })
    }

    pub(crate) async fn publish(&mut self, addr: &CustomAddr) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            self.socket = crate::backend::Backend::path(addr);
        }
        self.lock.file.set_len(0).await?;
        self.lock.file.write_all(addr.data()).await?;
        self.lock.file.flush().await
    }

    pub(crate) async fn release(self) -> io::Result<()> {
        let _guard = self.discovery.guard().await?;
        #[cfg(target_os = "linux")]
        if let Some(path) = &self.socket {
            remove_if_present(path).await?;
        }
        // Close before unlink on Windows, where an open handle can forbid
        // deletion. The directory guard excludes all new binders/resolvers.
        let record = self.discovery.record_path(self.id);
        drop(self.lock);
        remove_if_present(&record).await
    }
}

pub(crate) async fn remove_if_present(path: &Path) -> io::Result<()> {
    match tokio::fs::remove_file(path).await {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}
