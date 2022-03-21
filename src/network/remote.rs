use std::io::Read;
use std::io::Write;
use std::net::TcpStream;

use anyhow::Result;
use bincode::config::{FixintEncoding, RejectTrailing, WithOtherIntEncoding, WithOtherTrailing};
use bincode::{DefaultOptions, Options};
use serde::{Deserialize, Serialize};

use crate::network::{Coord, DemuxCoord, NetworkMessage, ReceiverEndpoint};
use crate::operator::ExchangeData;
use crate::profiler::{get_profiler, Profiler};
use crate::scheduler::ReplicaId;
use crate::stream::BlockId;

pub(crate) type SerializedMessage = Vec<u8>;

/// The capacity of the out-buffer.
pub(crate) const CHANNEL_CAPACITY: usize = 4;

lazy_static! {
/// Configuration of the header serializer: the integers must have a fixed length encoding.
    static ref HEADER_CONFIG: WithOtherTrailing<WithOtherIntEncoding<DefaultOptions, FixintEncoding>, RejectTrailing> =
        bincode::DefaultOptions::new().with_fixint_encoding().reject_trailing_bytes();
}

lazy_static! {
/// Configuration of the message serializer
    static ref MESSAGE_CONFIG: DefaultOptions = bincode::DefaultOptions::new();
}

/// Header of a message sent before the actual message.
#[derive(Serialize, Deserialize, Default)]
struct MessageHeader {
    /// The size of the actual message
    size: u32,
    /// The id of the replica this message is for.
    replica_id: ReplicaId,
    /// The id of the block that is sending the message.
    sender_block_id: BlockId,
}

/// Serialize and send a message to a remote socket.
///
/// The network protocol works as follow:
/// - send a `MessageHeader` serialized with bincode with `FixintEncoding`
/// - send the message
pub(crate) fn remote_send<T: ExchangeData>(
    what: NetworkMessage<T>,
    dest: ReceiverEndpoint,
    stream: &mut TcpStream,
) {
    let address = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let serialized = MESSAGE_CONFIG
        .serialize(&what)
        .unwrap_or_else(|e| panic!("Failed to serialize outgoing message to {}: {:?}", dest, e));
    let header = MessageHeader {
        size: serialized.len() as _,
        replica_id: dest.coord.replica_id,
        sender_block_id: dest.prev_block_id,
    };
    let serialized_header = HEADER_CONFIG.serialize(&header).unwrap();
    stream.write_all(&serialized_header).unwrap_or_else(|e| {
        panic!(
            "Failed to send size of message (was {} bytes) to {} at {}: {:?}",
            serialized.len(),
            dest,
            address,
            e
        )
    });

    stream.write_all(&serialized).unwrap_or_else(|e| {
        panic!(
            "Failed to send {} bytes to {} at {}: {:?}",
            serialized.len(),
            dest,
            address,
            e
        )
    });

    get_profiler().net_bytes_out(
        what.sender,
        dest.coord,
        serialized_header.len() + serialized.len(),
    );
}

/// Receive a message from the remote channel. Returns `None` if there was a failure receiving the
/// last message.
///
/// The message won't be deserialized, use `deserialize()`.
pub(crate) fn remote_recv(
    coord: DemuxCoord,
    stream: &mut TcpStream,
) -> Option<(ReceiverEndpoint, SerializedMessage)> {
    let address = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let header_size = header_size();
    let mut header = vec![0u8; header_size];
    match stream.read_exact(&mut header) {
        Ok(_) => {}
        Err(e) => {
            debug!(
                "Failed to receive {} bytes of header to {} from {}: {:?}",
                header_size, coord, address, e
            );
            return None;
        }
    }
    let header: MessageHeader = HEADER_CONFIG
        .deserialize(&header)
        .expect("Malformed header");
    let mut buf = vec![0u8; header.size as usize];
    stream.read_exact(&mut buf).unwrap_or_else(|e| {
        panic!(
            "Failed to receive {} bytes to {} from {}: {:?}",
            header.size, coord, address, e
        )
    });
    let receiver_endpoint = ReceiverEndpoint::new(
        Coord::new(coord.coord.block_id, coord.coord.host_id, header.replica_id),
        header.sender_block_id,
    );
    Some((receiver_endpoint, buf))
}

/// Try to deserialize a serialized message.
pub(crate) fn deserialize<T: ExchangeData>(message: SerializedMessage) -> Result<T> {
    Ok(MESSAGE_CONFIG.deserialize(&message)?)
}

pub(crate) fn header_size() -> usize {
    HEADER_CONFIG
        .serialized_size(&MessageHeader::default())
        .unwrap() as usize
}
