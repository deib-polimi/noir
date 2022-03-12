use std::any::TypeId;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::block::{BlockStructure, OperatorReceiver, OperatorStructure};
use crate::network::{Coord, NetworkMessage, NetworkReceiver, ReceiverEndpoint};
use crate::operator::start::StartBlockReceiver;
use crate::operator::ExchangeData;
use crate::scheduler::ExecutionMetadata;
use crate::stream::BlockId;

/// This will receive the data from a single previous block.
#[derive(Debug)]
pub(crate) struct SingleStartBlockReceiver<Out: ExchangeData> {
    pub(super) receiver: Option<NetworkReceiver<Out>>,
    previous_replicas: Vec<Coord>,
    pub(super) previous_block_id: BlockId,
}

impl<Out: ExchangeData> SingleStartBlockReceiver<Out> {
    pub(super) fn new(previous_block_id: BlockId) -> Self {
        Self {
            receiver: None,
            previous_replicas: Default::default(),
            previous_block_id,
        }
    }
}

impl<Out: ExchangeData> StartBlockReceiver<Out> for SingleStartBlockReceiver<Out> {
    fn setup(&mut self, metadata: ExecutionMetadata) {
        let in_type = TypeId::of::<Out>();

        let mut network = metadata.network.lock();
        let endpoint = ReceiverEndpoint::new(metadata.coord, self.previous_block_id);
        self.receiver = Some(network.get_receiver(endpoint));
        drop(network);

        for &(prev, typ) in metadata.prev.iter() {
            // ignore this connection because it refers to a different type, another StartBlock
            // in this block will handle it
            if in_type != typ {
                continue;
            }
            if prev.block_id == self.previous_block_id {
                self.previous_replicas.push(prev);
            }
        }
    }

    fn prev_replicas(&self) -> Vec<Coord> {
        self.previous_replicas.clone()
    }

    fn cached_replicas(&self) -> usize {
        0
    }

    fn poll_recv(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<NetworkMessage<Out>>> {
        let receiver = self.receiver.as_mut().unwrap();
        Pin::new(receiver).poll_recv(cx)
    }
    
    fn blocking_recv_one(&mut self) -> Option<NetworkMessage<Out>> {
        log::warn!("USING BLOCKING START BLOCK!");
        let receiver = self.receiver.as_mut().unwrap();
        receiver.blocking_recv_one()
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("StartBlock");
        operator
            .receivers
            .push(OperatorReceiver::new::<Out>(self.previous_block_id));
        BlockStructure::default().add_operator(operator)
    }
}

impl<Out: ExchangeData> Clone for SingleStartBlockReceiver<Out> {
    fn clone(&self) -> Self {
        Self {
            receiver: None,
            previous_block_id: self.previous_block_id,
            previous_replicas: self.previous_replicas.clone(),
        }
    }
}
