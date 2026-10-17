//! A bind collision must never seize another endpoint's socket pathname.

#![cfg(unix)]

use std::future::poll_fn;
use std::io::{self, IoSliceMut};
use std::os::unix::fs::MetadataExt;
use std::time::Duration;

use iroh::endpoint::transports::{CustomTransport, RecvInfo};
use iroh_ipc_transport::IpcTransport;
use tokio::net::UnixDatagram;

#[tokio::test]
async fn competing_bind_preserves_the_live_endpoint() {
    let dir = tempfile::Builder::new()
        .prefix("ipc-")
        .tempdir_in("/tmp")
        .expect("directory");
    let path = dir.path().join("peer.sock");
    let transport = IpcTransport::new(&path);
    let mut owner = transport.bind().expect("first bind");
    let original = tokio::fs::symlink_metadata(&path)
        .await
        .expect("original socket")
        .ino();

    let contender = transport.bind();
    let current = tokio::fs::symlink_metadata(&path)
        .await
        .expect("original socket remains")
        .ino();
    assert_eq!(current, original, "a bind must not replace a live socket");
    assert!(matches!(contender, Err(ref e) if e.kind() == io::ErrorKind::AddrInUse));

    let sender = UnixDatagram::bind(dir.path().join("sender.sock")).expect("sender");
    sender
        .send_to(b"still reachable", &path)
        .await
        .expect("send");
    let mut storage = [0u8; 64];
    let mut bufs = [IoSliceMut::new(&mut storage)];
    let mut metas = [noq_udp::RecvMeta::default()];
    let mut infos = [RecvInfo::new(transport.local_addr(), None)];
    let received = tokio::time::timeout(
        Duration::from_secs(1),
        poll_fn(|cx| owner.poll_recv(cx, &mut bufs, &mut metas, &mut infos)),
    )
    .await
    .expect("the original endpoint must remain reachable")
    .expect("receive");
    assert_eq!(received, 1);
    assert_eq!(&storage[..metas[0].len], b"still reachable");
}

#[tokio::test]
async fn bind_does_not_delete_an_existing_regular_file() {
    let dir = tempfile::Builder::new()
        .prefix("ipc-")
        .tempdir_in("/tmp")
        .expect("directory");
    let path = dir.path().join("not-a-socket");
    tokio::fs::write(&path, b"owned by someone else")
        .await
        .expect("existing file");

    let result = IpcTransport::new(&path).bind();
    assert!(matches!(result, Err(ref e) if e.kind() == io::ErrorKind::AddrInUse));
    assert_eq!(
        tokio::fs::read(&path).await.expect("file survives"),
        b"owned by someone else"
    );
}

#[tokio::test]
async fn stale_socket_requires_explicit_owner_cleanup() {
    let dir = tempfile::Builder::new()
        .prefix("ipc-")
        .tempdir_in("/tmp")
        .expect("directory");
    let path = dir.path().join("stale.sock");
    drop(UnixDatagram::bind(&path).expect("previous owner"));
    let original = tokio::fs::symlink_metadata(&path)
        .await
        .expect("stale socket")
        .ino();

    let result = IpcTransport::new(&path).bind();
    assert!(matches!(result, Err(ref e) if e.kind() == io::ErrorKind::AddrInUse));
    assert_eq!(
        tokio::fs::symlink_metadata(&path)
            .await
            .expect("stale socket survives")
            .ino(),
        original
    );
}
