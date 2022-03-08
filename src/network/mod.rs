use std::fmt::{Debug, Display, Formatter};

use serde::{Deserialize, Serialize};

pub(crate) use network_channel::*;
pub(crate) use topology::*;

use crate::operator::StreamElement;
use crate::scheduler::{HostId, ReplicaId};
use crate::stream::BlockId;

mod demultiplexer;
mod multiplexer;
mod remote;
mod network_channel;
mod topology;

#[derive(Debug, Clone)]
pub enum NetworkDataIterator<T> {
    Single(std::iter::Once<T>),
    Batch(std::vec::IntoIter<T>),
}

impl<T> Iterator for NetworkDataIterator<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            NetworkDataIterator::Single(i) => i.next(),
            NetworkDataIterator::Batch(i) => i.next(),
        }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub enum NetworkData<T> {
    Single(T),
    Batch(Vec<T>),
}

/// What is sent from a replica to the next.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub struct NetworkMessage<T> {
    /// The list of messages inside the batch,
    data: NetworkData<StreamElement<T>>,
    /// The coordinates of the block that sent this message.
    sender: Coord,
}

/// Coordinates that identify a block inside the network.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct BlockCoord {
    /// The identifier of the block the replicas works on.
    pub block_id: BlockId,
    /// The identifier of where the replica is located.
    pub host_id: HostId,
}

/// Coordinates that identify a replica inside the network.
#[derive(
    Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd, Default, Deserialize, Serialize,
)]
pub struct Coord {
    /// The identifier of the block the replicas works on.
    pub block_id: BlockId,
    /// The identifier of where the replica is located.
    pub host_id: HostId,
    /// The identifier of the replica inside the host.
    pub replica_id: ReplicaId,
}

/// The identifier of a single receiver endpoint of a replicated block.
///
/// Note that a replicated block may have many predecessors, each with a different message type, so
/// it has to have more than one receiver. In particular it should have a receiver for each
/// previous block in the job graph.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct ReceiverEndpoint {
    /// Coordinate of the receiver replica.
    pub coord: Coord,
    /// Id of the sender block.
    pub prev_block_id: BlockId,
}

/// The identifier of a demultiplexer inside an host.
///
/// Each block has as many demultiplexers as incoming blocks in the job graph. This coordinate
/// identify each of them inside a specific host.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct DemuxCoord {
    /// The coordinate of the block inside an host.
    pub coord: BlockCoord,
    /// The id of the previous block in the job graph.
    pub prev_block_id: BlockId,
}

impl<T> NetworkMessage<T> {
    pub fn new_single(data: StreamElement<T>, sender: Coord) -> Self {
        Self {
            data: NetworkData::Single(data),
            sender,
        }
    }

    pub fn new_batch(data: Vec<StreamElement<T>>, sender: Coord) -> Self {
        Self {
            data: NetworkData::Batch(data),
            sender,
        }
    }

    /// The coordinates of the sending block.
    pub fn sender(&self) -> Coord {
        self.sender
    }

    /// The number of items in the batch.
    pub fn num_items(&self) -> usize {
        match &self.data {
            NetworkData::Single(_) => 1,
            NetworkData::Batch(v) => v.len(),
        }
    }
}

impl<T> IntoIterator for NetworkMessage<T> {
    type Item = StreamElement<T>;

    type IntoIter = NetworkDataIterator<StreamElement<T>>;

    fn into_iter(self) -> Self::IntoIter {
        match self.data {
            NetworkData::Single(i) => NetworkDataIterator::Single(std::iter::once(i)),
            NetworkData::Batch(v) => NetworkDataIterator::Batch(v.into_iter()),
        }
    }
}

impl Coord {
    pub fn new(block_id: BlockId, host_id: HostId, replica_id: ReplicaId) -> Self {
        Self {
            block_id,
            host_id,
            replica_id,
        }
    }
}

impl ReceiverEndpoint {
    pub fn new(coord: Coord, prev_block_id: BlockId) -> Self {
        Self {
            coord,
            prev_block_id,
        }
    }
}

impl DemuxCoord {
    pub fn new(from: Coord, to: Coord) -> Self {
        Self {
            coord: to.into(),
            prev_block_id: from.block_id,
        }
    }

    /// Check whether the connection from->to would pass through this `DemuxCoord`.
    pub fn includes_channel(&self, from: Coord, to: Coord) -> bool {
        self.coord == BlockCoord::from(to) && self.prev_block_id == from.block_id
    }
}

impl Display for Coord {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Coord[b{}, h{}, r{}]",
            self.block_id, self.host_id, self.replica_id
        )
    }
}

impl Display for BlockCoord {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BlockCoord[b{}, h{}]", self.block_id, self.host_id)
    }
}

impl Display for ReceiverEndpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ReceiverEndpoint[{}, prev {}]",
            self.coord, self.prev_block_id
        )
    }
}

impl Display for DemuxCoord {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DemuxCoord[{}, prev {}]", self.coord, self.prev_block_id)
    }
}

impl From<Coord> for BlockCoord {
    fn from(coord: Coord) -> Self {
        Self {
            block_id: coord.block_id,
            host_id: coord.host_id,
        }
    }
}

impl From<ReceiverEndpoint> for DemuxCoord {
    fn from(endpoint: ReceiverEndpoint) -> Self {
        Self {
            coord: endpoint.coord.into(),
            prev_block_id: endpoint.prev_block_id,
        }
    }
}
