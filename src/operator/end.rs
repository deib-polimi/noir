use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Poll, Context};
use std::time::Duration;

use futures::ready;

use crate::block::{
    BatchMode, Batcher, BlockStructure, Connection, NextStrategy, OperatorStructure, SenderList,
    FlushError,
};
use crate::coord;
use crate::network::ReceiverEndpoint;
use crate::operator::{ExchangeData, KeyerFn, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::BlockId;

use super::AsyncOperator;

#[pin_project::pin_project]
#[derive(Derivative)]
#[derivative(Clone, Debug)]
pub struct EndBlock<Out: ExchangeData, OperatorChain, IndexFn>
where
    IndexFn: KeyerFn<usize, Out>,
    OperatorChain: Operator<Out>,
{
    #[pin]
    prev: OperatorChain,
    metadata: Option<ExecutionMetadata>,
    next_strategy: NextStrategy<Out, IndexFn>,
    batch_mode: BatchMode,
    sender_groups: Vec<SenderList>,
    #[derivative(Debug = "ignore", Clone(clone_with = "clone_default"))]
    senders: HashMap<ReceiverEndpoint, Batcher<Out>, ahash::RandomState>, // TODO: try to remove the boxes
    feedback_id: Option<BlockId>,
    ignore_block_ids: Vec<BlockId>,
    terminating: bool,
}

impl<Out: ExchangeData, OperatorChain, IndexFn> EndBlock<Out, OperatorChain, IndexFn>
where
    IndexFn: KeyerFn<usize, Out>,
    OperatorChain: Operator<Out>,
{
    pub(crate) fn new(
        prev: OperatorChain,
        next_strategy: NextStrategy<Out, IndexFn>,
        batch_mode: BatchMode,
    ) -> Self {
        Self {
            prev,
            metadata: None,
            next_strategy,
            batch_mode,
            sender_groups: Default::default(),
            senders: Default::default(),
            feedback_id: None,
            ignore_block_ids: Default::default(),
            terminating: false,
        }
    }

    /// Mark this `EndBlock` as the end of a feedback loop.
    ///
    /// This will avoid this block from sending `Terminate` in the feedback loop, the destination
    /// should be already gone.
    pub(crate) fn mark_feedback(&mut self, block_id: BlockId) {
        self.feedback_id = Some(block_id);
    }

    pub(crate) fn ignore_destination(&mut self, block_id: BlockId) {
        self.ignore_block_ids.push(block_id);
    }

    fn poll_all_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), FlushError>> {
        for (_, sender) in self.senders.iter_mut() {
            let result = ready!(Pin::new(sender).poll_ready(cx));

            match result {
                Ok(_) => {}
                Err(e) => {
                    log::debug!("Couldn't flush {} -> {}", self.metadata.as_ref().unwrap().coord, sender.coord());
                    return Poll::Ready(Err(e))
                }
            }
        }
        Poll::Ready(Ok(()))
    }
}

impl<Out: ExchangeData, OperatorChain, IndexFn> futures::Stream
    for EndBlock<Out, OperatorChain, IndexFn>
where
    IndexFn: KeyerFn<usize, Out>,
    OperatorChain: AsyncOperator<Out>,
{
    type Item = StreamElement<()>;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
        match self.poll_all_ready(cx) {
            Poll::Ready(Ok(_)) => {}
            Poll::Ready(Err(e)) => {
                panic!("Broken channel {e}"); // TODO: Check
            }
            Poll::Pending => return Poll::Pending,
        }

        if self.terminating {
            self.senders.drain();
        }

        let mut proj = self.project();

        let message = ready!(proj.prev.poll_next(cx));

        let message = message.unwrap_or_else(|| StreamElement::Terminate); // TODO: Change

        let to_return = message.take();

        match &message {
            StreamElement::Watermark(_)
            | StreamElement::Terminate
            | StreamElement::FlushAndRestart => {
                for senders in self.sender_groups.iter() {
                    for &sender in senders.0.iter() {
                        // if this block is the end of the feedback loop it should not forward
                        // `Terminate` since the destination is before us in the termination chain,
                        // and therefore has already left
                        if matches!(message, StreamElement::Terminate)
                            && Some(sender.coord.block_id) == self.feedback_id
                        {
                            continue;
                        }
                        let sender = self.senders.get_mut(&sender).unwrap();
                        sender.enqueue(message.clone());

                        if matches!(message, StreamElement::FlushAndRestart) {
                            sender.request_flush();
                        }
                    }
                }
            }
            StreamElement::Item(item) | StreamElement::Timestamped(item, _) => {
                let index = self.next_strategy.index(item);
                for sender in self.sender_groups.iter() {
                    let index = index % sender.0.len();
                    self.senders
                        .get_mut(&sender.0[index])
                        .unwrap()
                        .enqueue(message.clone())
                }
            }
            StreamElement::Yield => {
                log::error!("{} Received Yield from downstream", coord!(self));
            }
            StreamElement::FlushBatch => {
                log::debug!("{} Received FlushBatch from downstream, marking for flush", coord!(self));
                for sender in self.senders.values() {
                    sender.request_flush();
                }
            }
        };

        if matches!(to_return, StreamElement::Terminate) {
            let metadata = self.metadata.as_ref().unwrap();
            debug!(
                "EndBlock at {} received Terminate, closing {} channels",
                metadata.coord,
                self.senders.len()
            );
            for sender in self.senders.values() {
                sender.request_flush();
            }
            self.terminating = true;
        }

        Poll::Ready(Some(to_return))
    }
}

impl<Out: ExchangeData, OperatorChain, IndexFn> Operator<()>
    for EndBlock<Out, OperatorChain, IndexFn>
where
    IndexFn: KeyerFn<usize, Out>,
    OperatorChain: Operator<Out>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata.clone());

        let senders = metadata.network.lock().get_senders(metadata.coord);
        // remove the ignored destinations
        let senders = senders
            .into_iter()
            .filter(|(endpoint, _)| !self.ignore_block_ids.contains(&endpoint.coord.block_id))
            .collect();
        // group the senders based on the strategy
        self.sender_groups = self
            .next_strategy
            .group_senders(&senders, Some(metadata.coord.block_id));
        self.senders = senders
            .into_iter()
            .map(|(coord, sender)| (coord, Batcher::new(sender, self.batch_mode, metadata.coord)))
            .collect();
        self.metadata = Some(metadata);
    }

    fn next(&mut self) -> StreamElement<()> {
        let message = self.prev.next();
        let to_return = message.take();
        match &message {
            StreamElement::Watermark(_)
            | StreamElement::Terminate
            | StreamElement::FlushAndRestart => {
                if matches!(message, StreamElement::FlushAndRestart) {
                }
                for senders in self.sender_groups.iter() {
                    for &sender in senders.0.iter() {
                        // if this block is the end of the feedback loop it should not forward
                        // `Terminate` since the destination is before us in the termination chain,
                        // and therefore has already left
                        if matches!(message, StreamElement::Terminate)
                            && Some(sender.coord.block_id) == self.feedback_id
                        {
                            continue;
                        }
                        let sender = self.senders.get_mut(&sender).unwrap();
                        match sender.blocking_send_one(message.clone()) {
                            Ok(()) => {}
                            Err(FlushError::Disconnected) => panic!(),
                        }
                    }
                }
            }
            StreamElement::Item(item) | StreamElement::Timestamped(item, _) => {
                let index = self.next_strategy.index(item);
                for sender in self.sender_groups.iter() {
                    let index = index % sender.0.len();
                    match self
                        .senders
                        .get_mut(&sender.0[index])
                        .unwrap()
                        .blocking_send_one(message.clone())
                    {
                        Ok(()) => {}
                        Err(FlushError::Disconnected) => panic!(),
                    }
                }
            }
            StreamElement::Yield => {
                log::debug!("{} BLOCKING END: Received Yield from downstream, marking for flush", coord!(self));
            }
            StreamElement::FlushBatch => {
                log::debug!("{} BLOCKING END: Received FlushBatch from downstream, marking for flush", coord!(self));
            }
        };

        if matches!(to_return, StreamElement::Terminate) {
            let metadata = self.metadata.as_ref().unwrap();
            debug!(
                "EndBlock at {} received Terminate, closing {} channels",
                metadata.coord,
                self.senders.len()
            );
            self.senders.drain();
        }
        to_return
    }

    fn to_string(&self) -> String {
        match self.next_strategy {
            NextStrategy::Random => format!("{} -> Shuffle", self.prev.to_string()),
            NextStrategy::OnlyOne => format!("{} -> OnlyOne", self.prev.to_string()),
            _ => self.prev.to_string(),
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("EndBlock");
        for sender_group in &self.sender_groups {
            if !sender_group.0.is_empty() {
                let block_id = sender_group.0[0].coord.block_id;
                operator
                    .connections
                    .push(Connection::new::<Out, _>(block_id, &self.next_strategy));
            }
        }
        self.prev.structure().add_operator(operator)
    }
}

fn clone_default<T>(_: &T) -> T
where
    T: Default,
{
    T::default()
}
