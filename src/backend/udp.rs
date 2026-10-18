//! macOS loopback UDP, retaining Noq's batched datagram I/O.

use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::num::NonZeroUsize;
use std::task::{Context, Poll, ready};

use iroh::EndpointId;
use iroh::endpoint::transports::{RecvInfo, Transmit};
use iroh_base::CustomAddr;
use tokio::io::Interest;
use tokio::net::UdpSocket;

use crate::{IPC_TRANSPORT_ID, LocalDiscovery};

#[derive(Debug)]
pub(crate) struct Backend {
    socket: UdpSocket,
    state: noq_udp::UdpSocketState,
    addr: CustomAddr,
}

impl Backend {
    pub(crate) async fn bind(_discovery: &LocalDiscovery, _id: EndpointId) -> io::Result<Self> {
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let state = noq_udp::UdpSocketState::new((&socket).into())?;
        let addr = address(socket.local_addr()?.port());
        Ok(Self {
            socket,
            state,
            addr,
        })
    }

    pub(crate) fn local_addr(&self) -> CustomAddr {
        self.addr.clone()
    }
    pub(crate) fn valid_addr(addr: &CustomAddr) -> bool {
        port(addr).is_some()
    }
    pub(crate) fn max_transmit_segments(&self) -> NonZeroUsize {
        self.state.max_gso_segments()
    }
    pub(crate) async fn close(self) {
        drop(self);
    }

    pub(crate) fn poll_send(
        &mut self,
        cx: &mut Context<'_>,
        dst: &CustomAddr,
        transmit: &Transmit<'_>,
    ) -> Poll<io::Result<()>> {
        let Some(port) = port(dst) else {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid loopback address",
            )));
        };
        let packet = noq_udp::Transmit {
            destination: SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
            // iroh's custom-transport API does not expose outgoing ECN yet.
            ecn: None,
            contents: transmit.contents,
            segment_size: transmit.segment_size,
            src_ip: None,
        };
        loop {
            ready!(self.socket.poll_send_ready(cx))?;
            match self.socket.try_io(Interest::WRITABLE, || {
                self.state.try_send((&self.socket).into(), &packet)
            }) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                result => return Poll::Ready(result),
            }
        }
    }

    pub(crate) fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &mut [io::IoSliceMut<'_>],
        metas: &mut [noq_udp::RecvMeta],
        infos: &mut [RecvInfo],
    ) -> Poll<io::Result<usize>> {
        if bufs.is_empty() {
            return Poll::Ready(Ok(0));
        }
        loop {
            ready!(self.socket.poll_recv_ready(cx))?;
            match self.socket.try_io(Interest::READABLE, || {
                self.state.recv((&self.socket).into(), bufs, metas)
            }) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                Err(error) => return Poll::Ready(Err(error)),
                Ok(count) => {
                    // Bound socket is loopback-only. Reject spoofed external source
                    // addresses rather than converting them into local destinations.
                    for index in 0..count {
                        if metas[index].addr.ip() != Ipv4Addr::LOCALHOST {
                            metas[index].len = 0;
                            metas[index].stride = 0;
                        }
                        infos[index] = RecvInfo::new(
                            address(metas[index].addr.port()),
                            Some(self.addr.clone()),
                        );
                    }
                    return Poll::Ready(Ok(count));
                }
            }
        }
    }
}

fn address(port: u16) -> CustomAddr {
    let [hi, lo] = port.to_be_bytes();
    CustomAddr::from_parts(IPC_TRANSPORT_ID, &[1, hi, lo])
}

fn port(addr: &CustomAddr) -> Option<u16> {
    if addr.id() != IPC_TRANSPORT_ID {
        return None;
    }
    let [1, hi, lo] = *addr.data() else {
        return None;
    };
    let port = u16::from_be_bytes([hi, lo]);
    (port != 0).then_some(port)
}
