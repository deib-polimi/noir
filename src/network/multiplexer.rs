use std::io::ErrorKind;
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::thread::{sleep, JoinHandle};
use std::time::Duration;

use anyhow::{anyhow};

use crate::channel::{Receiver, Sender, SendError, channel};
use crate::network::remote::{remote_send, CHANNEL_CAPACITY};
use crate::network::{DemuxCoord, NetworkMessage, ReceiverEndpoint};
use crate::operator::ExchangeData;

/// Maximum number of attempts to make for connecting to a remote host.
const CONNECT_ATTEMPTS: usize = 10;
/// Timeout for connecting to a remote host.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
/// To avoid spamming the connections, wait this timeout before trying again. If the connection
/// fails again this timeout will be doubled up to `RETRY_MAX_TIMEOUT`.
const RETRY_INITIAL_TIMEOUT: Duration = Duration::from_millis(50);
/// Maximum timeout between connection attempts.
const RETRY_MAX_TIMEOUT: Duration = Duration::from_secs(1);

/// Like `NetworkSender`, but this should be used in a multiplexed channel (i.e. a remote one).
///
/// The `ReceiverEndpoint` is sent alongside the actual message in order to demultiplex it.
#[derive(Debug, Clone)]
pub struct MultiplexingSender<Out: ExchangeData> {
    /// The internal sender that points to the actual multiplexed channel.
    sender: Sender<(ReceiverEndpoint, NetworkMessage<Out>)>,
}

impl<Out: ExchangeData> MultiplexingSender<Out> {
    /// Construct a new `MultiplexingSender` for a block.
    ///
    /// All the replicas of this block should point to this multiplexer (or one of its clones).
    pub fn new(coord: DemuxCoord, address: (String, u16)) -> (Self, JoinHandle<()>) {
        let (sender, receiver) = channel(CHANNEL_CAPACITY); // TODO: change
        let join_handle = std::thread::Builder::new()
            .name(format!("NetSender{}", coord))
            .spawn(move || Self::connect_remote(coord, address, receiver))
            .unwrap();
        (Self { sender }, join_handle)
    }

    /// Send a message to the channel.
    ///
    /// Unlikely the normal channels, the destination is required since the channel is multiplexed.
    pub fn send(&self, destination: ReceiverEndpoint, message: NetworkMessage<Out>) -> Result<(), SendError<(ReceiverEndpoint, NetworkMessage<Out>)>> {
        self.sender
            .blocking_send((destination, message))
    }

    /// Connect the sender to a remote channel located at the specified address.
    ///
    /// - At first the address is resolved to an actual address (DNS resolution)
    /// - Then at most `CONNECT_ATTEMPTS` are performed, and an exponential backoff is used in case
    ///   of errors.
    /// - If the connection cannot be established this function will panic.
    fn connect_remote(
        coord: DemuxCoord,
        address: (String, u16),
        local_receiver: Receiver<(ReceiverEndpoint, NetworkMessage<Out>)>,
    ) {
        let socket_addrs: Vec<_> = address
            .to_socket_addrs()
            .map_err(|e| format!("Failed to get the address for {}: {:?}", coord, e))
            .unwrap()
            .collect();
        let mut retry_delay = RETRY_INITIAL_TIMEOUT;
        for attempt in 1..=CONNECT_ATTEMPTS {
            debug!(
                "Attempt {} to connect to {} at {:?}",
                attempt, coord, socket_addrs
            );

            for address in socket_addrs.iter() {
                match TcpStream::connect_timeout(address, CONNECT_TIMEOUT) {
                    Ok(stream) => {
                        Self::handle_remote_connection(coord, local_receiver, stream);
                        return;
                    }
                    Err(err) => match err.kind() {
                        ErrorKind::TimedOut => {
                            debug!("Timeout connecting to {} at {:?}", coord, address);
                        }
                        _ => {
                            debug!("Failed to connect to {} at {}: {:?}", coord, address, err);
                        }
                    },
                }
            }

            debug!(
                "Retrying connection to {} at {:?} in {}s",
                coord,
                socket_addrs,
                retry_delay.as_secs_f32(),
            );

            sleep(retry_delay);
            retry_delay = (2 * retry_delay).min(RETRY_MAX_TIMEOUT);
        }
        panic!(
            "Failed to connect to remote {} at {:?} after {} attempts",
            coord, address, CONNECT_ATTEMPTS
        );
    }

    /// Handle the connection to the remote replica.
    ///
    /// Waits messages from the local receiver, then serialize the message and send it to the remote
    /// replica.
    fn handle_remote_connection(
        coord: DemuxCoord,
        mut local_receiver: Receiver<(ReceiverEndpoint, NetworkMessage<Out>)>,
        mut stream: TcpStream,
    ) {
        let address = stream
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        debug!("Connection to {} at {} established", coord, address);
        while let Some((dest, message)) = local_receiver.blocking_recv() {
            remote_send(message, dest, &mut stream);
        }
        let _ = stream.shutdown(Shutdown::Both);
        debug!("Remote sender for {} exited", coord);
    }
}
