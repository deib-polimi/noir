#[cfg(not(feature = "async-tokio"))]
use std::net::{Shutdown, TcpListener, TcpStream};
use std::thread::JoinHandle;

use std::net::SocketAddr;


#[cfg(feature = "async-tokio")]
use tokio::io::AsyncWriteExt;
#[cfg(feature = "async-tokio")]
use tokio_uring::net::{TcpListener, TcpStream};

use ahash::AHashMap;
use anyhow::anyhow;
use std::net::ToSocketAddrs;

use crate::channel::{self, Sender, UnboundedReceiver, UnboundedSender};
use crate::network::remote::{deserialize, header_size, remote_recv};
use crate::network::{DemuxCoord, NetworkMessage, ReceiverEndpoint};
use crate::operator::ExchangeData;
use crate::profiler::{get_profiler, Profiler};

/// Like `NetworkReceiver`, but this should be used in a multiplexed channel (i.e. a remote one).
///
/// This receiver is handled in a separate thread that keeps track of the local registered receivers
/// and the open connections. The incoming messages are tagged with the receiver endpoint. Upon
/// arrival they are routed to the correct receiver according to the `ReceiverEndpoint` the message
/// is tagged with.
#[derive(Debug)]
pub(crate) struct DemuxHandle<In: ExchangeData> {
    coord: DemuxCoord,
    /// Tell the demultiplexer that a new receiver is present,
    tx_senders: UnboundedSender<(ReceiverEndpoint, Sender<NetworkMessage<In>>)>,
}

#[cfg(not(feature = "async-tokio"))]
impl<In: ExchangeData> DemuxHandle<In> {
    /// Construct a new `DemultiplexingReceiver` for a block.
    ///
    /// All the local replicas of this block should be registered to this demultiplexer.
    /// `num_client` is the number of multiplexers that will connect to this demultiplexer. Since
    /// the remote senders are all multiplexed this corresponds to the number of remote replicas in
    /// the previous block (relative to the block this demultiplexer refers to).
    pub fn new(
        coord: DemuxCoord,
        address: (String, u16),
        num_clients: usize,
    ) -> (Self, JoinHandle<()>) {
        let (tx_senders, rx_senders) = channel::unbounded();
        let join_handle = std::thread::Builder::new()
            .name(format!("noir-demux-reg-{}", coord))
            .spawn(move || bind_remotes(coord, address, num_clients, rx_senders))
            .unwrap();
        (Self { coord, tx_senders }, join_handle)
    }

    /// Register a local receiver to this demultiplexer.
    pub fn register(
        &mut self,
        receiver_endpoint: ReceiverEndpoint,
        sender: Sender<NetworkMessage<In>>,
    ) {
        tracing::debug!(
            "registering {} to the demultiplexer of {}",
            receiver_endpoint, self.coord
        );
        self.tx_senders
            .send((receiver_endpoint, sender))
            .unwrap_or_else(|_| panic!("register for {:?} failed", self.coord))
    }
}

/// Bind the socket of this demultiplexer.
#[cfg(not(feature = "async-tokio"))]
fn bind_remotes<In: ExchangeData>(
    coord: DemuxCoord,
    address: (String, u16),
    num_clients: usize,
    rx_senders: UnboundedReceiver<(ReceiverEndpoint, Sender<NetworkMessage<In>>)>,
) {
    let address = (address.0.as_ref(), address.1);
    let address: Vec<_> = address
        .to_socket_addrs()
        .map_err(|e| format!("Failed to get the address for {}: {:?}", coord, e))
        .unwrap()
        .collect();

    tracing::debug!("demux binding {}", address[0]);
    let listener = TcpListener::bind(&*address)
        .map_err(|e| {
            anyhow!(
                "Failed to bind socket for {} at {:?}: {:?}",
                coord,
                address,
                e
            )
        })
        .unwrap();
    let address = listener
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    info!(
        "Remote receiver at {} is ready to accept {} connections to {}",
        coord, num_clients, address
    );

    // the list of JoinHandle of all the spawned threads, including the demultiplexer one
    let mut join_handles = vec![];
    let mut tx_broadcast = vec![];

    let mut incoming = listener.incoming();
    let mut connected_clients = 0;
    while connected_clients < num_clients {
        let stream = incoming.next().unwrap();
        let stream = match stream {
            Ok(stream) => stream,
            Err(e) => {
                warn!("Failed to accept incoming connection at {}: {:?}", coord, e);
                continue;
            }
        };
        connected_clients += 1;
        let peer_addr = stream.peer_addr().unwrap();
        info!(
            "Remote receiver at {} accepted a new connection from {} ({} / {})",
            coord, peer_addr, connected_clients, num_clients
        );

        let (demux_tx, demux_rx) = channel::unbounded();
        let join_handle = std::thread::Builder::new()
            .name(format!("noir-demux-{}", coord))
            .spawn(move || {
                let mut senders = AHashMap::new();
                while let Ok((endpoint, sender)) = demux_rx.recv() {
                    senders.insert(endpoint, sender);
                }
                tracing::debug!("demux got senders");
                demux_thread::<In>(coord, senders, stream);
            })
            .unwrap();
        join_handles.push(join_handle);
        tx_broadcast.push(demux_tx);
    }
    tracing::debug!("All connection to {} started, waiting for senders", coord);

    // Broadcast senders
    while let Ok(t) = rx_senders.recv() {
        for tx in tx_broadcast.iter() {
            tx.send(t.clone()).unwrap();
        }
    }
    drop(tx_broadcast); // Start all demuxes
    for handle in join_handles {
        handle.join().unwrap();
    }
    tracing::debug!("all demuxes for {} finished", coord);
}

/// Handle the connection with a remote sender.
///
/// Will deserialize the message upon arrival and send to the corresponding recipient the
/// deserialized data. If the recipient is not yet known, it is waited until it registers.
///
/// # Upgrade path
///
/// Replace send with queue.
///
/// The queue uses a hierarchical queue:
/// + First try to reserve and put the value in the fast queue
/// + If the fast queue is full, put in the slow (unbounded?) queue
/// + Return an enum, either Queued or Overflowed
///
/// if overflowed send a yield request through a second channel
#[cfg(not(feature = "async-tokio"))]
#[tracing::instrument(skip_all)]
fn demux_thread<In: ExchangeData>(
    coord: DemuxCoord,
    senders: AHashMap<ReceiverEndpoint, Sender<NetworkMessage<In>>>,
    mut stream: TcpStream,
) {
    let address = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    tracing::debug!("demultiplexer for {} at {} started", coord, address);

    while let Some((dest, message)) = remote_recv(coord, &mut stream) {
        let message_len = message.len();
        let message = deserialize::<NetworkMessage<In>>(message).unwrap();
        get_profiler().net_bytes_in(message.sender, dest.coord, header_size() + message_len);

        if let Err(e) = senders[&dest].send(message) {
            warn!("failed to send message to {}: {:?}", dest, e);
        }
    }

    let _ = stream.shutdown(Shutdown::Both);
    tracing::debug!("demultiplexer for {} at {} exited", coord, address);
}

#[cfg(feature = "async-tokio")]
impl<In: ExchangeData> DemuxHandle<In> {
    /// Construct a new `DemultiplexingReceiver` for a block.
    ///
    /// All the local replicas of this block should be registered to this demultiplexer.
    /// `num_client` is the number of multiplexers that will connect to this demultiplexer. Since
    /// the remote senders are all multiplexed this corresponds to the number of remote replicas in
    /// the previous block (relative to the block this demultiplexer refers to).
    pub fn new(
        coord: DemuxCoord,
        address: (String, u16),
        num_clients: usize,
    ) -> (Self, JoinHandle<()>) {
        let (tx_senders, rx_senders) = channel::unbounded();

        let join_handle = std::thread::spawn(move || bind_remotes(coord, address, num_clients, rx_senders));
        (Self { coord, tx_senders }, join_handle)
    }

    /// Register a local receiver to this demultiplexer.
    pub fn register(
        &mut self,
        receiver_endpoint: ReceiverEndpoint,
        sender: Sender<NetworkMessage<In>>,
    ) {
        tracing::debug!(
            "registering {} to the demultiplexer of {}",
            receiver_endpoint, self.coord
        );
        self.tx_senders
            .send((receiver_endpoint, sender))
            .unwrap_or_else(|_| panic!("register for {:?} failed", self.coord))
    }
}

/// Bind the socket of this demultiplexer.
#[cfg(feature = "async-tokio")]
fn bind_remotes<In: ExchangeData>(
    coord: DemuxCoord,
    address: (String, u16),
    num_clients: usize,
    rx_senders: UnboundedReceiver<(ReceiverEndpoint, Sender<NetworkMessage<In>>)>,
) {
    let address = (address.0.as_ref(), address.1);
    let address: Vec<_> = address
        .to_socket_addrs()
        .map_err(|e| format!("Failed to get the address for {}: {:?}", coord, e))
        .unwrap()
        .collect();

    tokio_uring::start(async move {
        tracing::debug!("demux binding {}", address[0]);
        let listener = TcpListener::bind(address[0])
            .map_err(|e| {
                anyhow!(
                    "Failed to bind socket for {} at {:?}: {:?}",
                    coord,
                    address,
                    e
                )
            })
            .unwrap();
        
        // the list of JoinHandle of all the spawned threads, including the demultiplexer one
        let mut join_handles = vec![];
        let mut tx_broadcast = vec![];

        let mut connected_clients = 0;
        while connected_clients < num_clients {
            let stream = listener.accept().await;
            let (stream, peer_addr) = match stream {
                Ok(stream) => stream,
                Err(e) => {
                    tracing::warn!("Failed to accept incoming connection at {}: {:?}", coord, e);
                    continue;
                }
            };
            connected_clients += 1;
            tracing::info!(
                "Remote receiver at {} accepted a new connection from {} ({} / {})",
                coord, peer_addr, connected_clients, num_clients
            );

            let (demux_tx, demux_rx) = flume::unbounded();
            let join_handle = tokio_uring::spawn(async move {
                let mut senders = AHashMap::new();
                while let Ok((endpoint, sender)) = demux_rx.recv_async().await {
                    senders.insert(endpoint, sender);
                }
                tracing::debug!("async demux got senders");
                demux_thread::<In>(coord, senders, peer_addr, stream).await;
            });
            join_handles.push(join_handle);
            tx_broadcast.push(demux_tx);
            tracing::debug!(
                "Spawned task at {} for {} ({} / {})",
                coord, peer_addr, connected_clients, num_clients
            );
        }
        tracing::debug!("All connection to {} started, waiting for senders", coord);

        // Broadcast senders
        while let Ok(t) = rx_senders.recv_async().await {
            for tx in tx_broadcast.iter() {
                tx.send(t.clone()).unwrap();
            }
        }
        drop(tx_broadcast); // Start all demuxes
        tracing::debug!("Broadcast for {} finished, waiting for termination", coord);
        for handle in join_handles {
            handle.await.unwrap();
        }
        tracing::debug!("all demuxes for {} finished", coord);
    });
    tracing::debug!("Stopped runtime for: {} demux", coord);
}

/// Handle the connection with a remote sender.
///
/// Will deserialize the message upon arrival and send to the corresponding recipient the
/// deserialized data. If the recipient is not yet known, it is waited until it registers.
///
/// # Upgrade path
///
/// Replace send with queue.
///
/// The queue uses a hierarchical queue:
/// + First try to reserve and put the value in the fast queue
/// + If the fast queue is full, put in the slow (unbounded?) queue
/// + Return an enum, either Queued or Overflowed
///
/// if overflowed send a yield request through a second channel
#[cfg(feature = "async-tokio")]
#[tracing::instrument(skip_all)]
async fn demux_thread<In: ExchangeData>(
    coord: DemuxCoord,
    senders: AHashMap<ReceiverEndpoint, Sender<NetworkMessage<In>>>,
    address: SocketAddr,
    stream: TcpStream,
) {


    // let address = stream
    //     .peer_addr()
    //     .map(|a| a.to_string())
    //     .unwrap_or_else(|_| "unknown".to_string());
    tracing::debug!("demultiplexer for {} at {} started", coord, address);

    while let Some((dest, message)) = remote_recv(coord, &stream).await {
        let message_len = message.len();
        let message = deserialize::<NetworkMessage<In>>(message).unwrap();
        get_profiler().net_bytes_in(message.sender, dest.coord, header_size() + message_len);

        if let Err(e) = senders[&dest].send_async(message).await {
            warn!("failed to send message to {}: {:?}", dest, e);
        }
    }

    // stream.close().await.unwrap(); // TODO: close!
    tracing::debug!("demultiplexer for {} at {} finished", coord, address);
}
