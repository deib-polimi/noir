use std::any::TypeId;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;
use tokio_util::sync::ReusableBoxFuture;

use crate::block::{BlockStructure, OperatorReceiver, OperatorStructure};
use crate::network::{Coord, NetworkMessage, NetworkReceiver, ReceiverEndpoint};
use crate::operator::start::StartBlockReceiver;
use crate::operator::ExchangeData;
use crate::scheduler::ExecutionMetadata;
use crate::stream::BlockId;

#[derive(Debug)]
pub(crate) enum RecvState<T: ExchangeData> {
    Idle(NetworkReceiver<T>),
    Pending,
    Closed,
}

impl<T: ExchangeData> RecvState<T> {
    pub fn is_idle(&self) -> bool {
        match &self {
            RecvState::Idle(_) => true,
            _ => false,
        }
    }

    pub fn take(&mut self) -> Self {
        std::mem::replace(self, Self::Closed)
    }

    pub fn take_receiver(&mut self) -> NetworkReceiver<T> {
        match std::mem::replace(self, Self::Closed) {
            RecvState::Idle(receiver) => receiver,
            RecvState::Pending => panic!("Trying to take receiver from Pending state"),
            RecvState::Closed => panic!("Trying to take receiver from Closed state"),
        }
    }
}

// By reusing the same async fn for both `Some` and `None`, we make sure every future passed to
// ReusableBoxFuture has the same underlying type, and hence the same size and alignment.
async fn make_recv_future<T: ExchangeData>(
    data: Option<NetworkReceiver<T>>,
) -> Option<(NetworkMessage<T>, NetworkReceiver<T>)> {
    match data {
        Some(mut receiver) => receiver
                .recv()
                .await
                .map(move |msg| (msg, receiver)),
        None => unreachable!("this future should not be pollable in this state"),
    }
}

/// This will receive the data from a single previous block.
#[derive(Debug)]
pub(crate) struct SingleStartBlockReceiver<Out: ExchangeData> {
    pub(super) state: RecvState<Out>,
    pending: ReusableBoxFuture<'static, Option<(NetworkMessage<Out>, NetworkReceiver<Out>)>>,
    previous_replicas: Vec<Coord>,
    pub(super) previous_block_id: BlockId,
}

impl<Out: ExchangeData> SingleStartBlockReceiver<Out> {
    pub(super) fn new(previous_block_id: BlockId) -> Self {
        Self {
            state: RecvState::Closed,
            pending: ReusableBoxFuture::new(make_recv_future(None)),
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
        self.state = RecvState::Idle(network.get_receiver(endpoint));
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

    #[tracing::instrument(name = "start_single_poll_recv", skip_all)]
    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<NetworkMessage<Out>>> {
        let (result, next_state) = match self.state.take() {
            RecvState::Idle(receiver) => {
                self.pending.set(make_recv_future(Some(receiver)));

                match self.pending.poll(cx) {
                    Poll::Ready(Some((item, receiver))) => {
                        (Poll::Ready(Some(item)), RecvState::Idle(receiver))
                    }
                    Poll::Ready(None) => {
                        (Poll::Ready(None), RecvState::Closed)
                    }
                    Poll::Pending => (Poll::Pending, RecvState::Pending),
                }
            }
            RecvState::Pending => {
                match self.pending.poll(cx) {
                    Poll::Ready(Some((item, receiver))) => {
                        (Poll::Ready(Some(item)), RecvState::Idle(receiver))
                    }
                    Poll::Ready(None) => {
                        (Poll::Ready(None), RecvState::Closed)
                    }
                    Poll::Pending => (Poll::Pending, RecvState::Pending),
                }
            }
            RecvState::Closed => panic!("Trying to receive from illegal Closed state"),
        };

        self.state = next_state;
        result
    }
    
    fn blocking_recv_one(&mut self) -> Option<NetworkMessage<Out>> {
        log::warn!("USING BLOCKING START BLOCK!");
        todo!();
        // let receiver = self.receiver.as_mut().unwrap();
        // receiver.blocking_recv_one()
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
        assert!(if let RecvState::Closed = self.state {true} else {false});
        Self {
            state: RecvState::Closed,
            pending: ReusableBoxFuture::new(make_recv_future(None)),
            previous_block_id: self.previous_block_id,
            previous_replicas: self.previous_replicas.clone(),
        }
    }
}
