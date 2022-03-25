use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Poll, Context};
use std::time::Duration;

use futures::{ready, StreamExt};

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

#[derive(Derivative)]
#[derivative(Clone, Debug)]
pub struct EndBlock<Out: ExchangeData, OperatorChain, IndexFn>
where
    IndexFn: KeyerFn<usize, Out>,
    OperatorChain: Operator<Out>,
{
    prev: OperatorChain,
    metadata: Option<ExecutionMetadata>,
    next_strategy: NextStrategy<Out, IndexFn>,
    batch_mode: BatchMode,
    #[derivative(Debug = "ignore", Clone(clone_with = "clone_default"))]
    senders: Senders<Out>,
    feedback_id: Option<BlockId>,
    ignore_block_ids: Vec<BlockId>,
    flush_requested: bool,
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
            senders: Default::default(),
            feedback_id: None,
            ignore_block_ids: Default::default(),
            flush_requested: false,
            terminating: false,
        }
    }

    /// Mark this `EndBlock` as the end of a feedback loop.
    ///
    /// This will avoid this block from sending `Terminate` in the feedback loop, the destination
    /// should be already gone.
    pub(crate) fn mark_feedback(&mut self, block_id: BlockId) {
        todo!();
        self.feedback_id = Some(block_id);
    }

    pub(crate) fn ignore_destination(&mut self, block_id: BlockId) {
        self.ignore_block_ids.push(block_id);
    }
}

impl<Out: ExchangeData, OperatorChain, IndexFn> futures::Stream
    for EndBlock<Out, OperatorChain, IndexFn>
where
    IndexFn: KeyerFn<usize, Out>,
    OperatorChain: AsyncOperator<Out>,
{
    type Item = StreamElement<()>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match this.senders.poll_ready(cx) {
                Poll::Ready(Ok(_)) => {}
                Poll::Ready(Err(e)) => {
                    panic!("Broken channel {e}"); // TODO: Check
                }
                Poll::Pending => return Poll::Pending,
            }

            if this.terminating {
                this.senders.close();
                return Poll::Ready(None);
            }

            let message = match this.prev.poll_next_unpin(cx) {
                Poll::Ready(m) => m,
                Poll::Pending => {
                    this.senders.flush_all();
                    continue;
                }
            };

            let message = message.unwrap_or_else(|| StreamElement::Terminate); // TODO: Change
            let to_return = message.take();

            log::warn!("end\t{}: {:?}", coord!(this), &to_return);

            match &message {
                StreamElement::Watermark(_) => 
                    this.senders.enqueue_all_groups(message.clone()),
                StreamElement::FlushAndRestart => {
                    this.senders.enqueue_all_groups(message.clone());
                    this.senders.flush_all();
                }
                StreamElement::Terminate => {
                    log::warn!("Broadcasting Terminate {}", this.metadata.as_ref().unwrap().coord);
                    this.senders.broadcast_terminate();
                    this.terminating = true;
                }
                StreamElement::Item(item) | StreamElement::Timestamped(item, _) => {
                    let index = this.next_strategy.index(item);
                    this.senders.enqueue_indexed(index, message);
                }
                StreamElement::Yield => {
                    log::error!("{} Received Yield from downstream", coord!(this));
                }
                StreamElement::FlushBatch => {
                    log::debug!("{} Received FlushBatch from downstream, marking for flush", coord!(this));
                    this.senders.flush_all();
                }
            };

            return Poll::Ready(Some(to_return));
        }
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
        self.senders.set_send_groups(self
            .next_strategy
            .group_senders(&senders, Some(metadata.coord.block_id)));
        self.senders.set_senders(senders
            .into_iter()
            .map(|(coord, sender)| (coord, Batcher::new(sender, self.batch_mode, metadata.coord)))
            .collect());
        self.metadata = Some(metadata);
    }

    fn next(&mut self) -> StreamElement<()> {
        todo!();
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
        for endpoints in self.senders.send_groups() {
            if !endpoints.is_empty() {
                let block_id = endpoints[0].coord.block_id;
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

struct Senders<Out: ExchangeData> {
    send_groups: Vec<Vec<ReceiverEndpoint>>,
    senders: HashMap<ReceiverEndpoint, Batcher<Out>, ahash::RandomState>,
    pending_flush: Vec<ReceiverEndpoint>,
}

macro_rules! enqueue {
    ($self:ident, $endpoint:expr, $msg:expr) => {
        if $self.senders.get_mut($endpoint).unwrap().enqueue($msg) {
            $self.pending_flush.push(*$endpoint);
        }
    }
}

impl<Out: ExchangeData> Senders<Out> {
    // fn enqueue(&mut self, endpoint: &ReceiverEndpoint, msg: StreamElement<Out>) {
    //     if self.senders.get_mut(endpoint).unwrap().enqueue(msg) {
    //         self.pending_flush.push(*endpoint);
    //     }
    // }

    fn enqueue_all_groups(&mut self, msg: StreamElement<Out>) {
        for endpoint in self.send_groups.iter().flat_map(|g| g.iter()) {
            enqueue!(self, endpoint, msg.clone());
        }
    }

    fn enqueue_indexed(&mut self, idx: usize, msg: StreamElement<Out>) {
        for endpoint in self.send_groups.iter().map(|group| &group[idx % group.len()]) {
            enqueue!(self, endpoint, msg.clone());
        }
    }

    fn broadcast_terminate(&mut self) {
        // MUST FLUSH
        for endpoint in self.send_groups.iter().flat_map(|g| g.iter()) {
            // TODO: Check feedback
            enqueue!(self, endpoint, StreamElement::Terminate);
        }
        self.flush_all();
    }

    fn flush_all(&mut self) {
        self.senders.values_mut().for_each(|s| s.request_flush());
        self.pending_flush.extend(self.senders.keys().copied());
    }

    fn close(&mut self) {
        assert!(self.pending_flush.is_empty());
        self.senders.drain();
    }

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), FlushError>> {
        let mut i = 0;
        while i < self.pending_flush.len() {
            let sender = self.senders.get_mut(&self.pending_flush[i]).unwrap();
            let coord = sender.coord();

            match Pin::new(sender).poll_ready(cx) {
                Poll::Ready(Ok(_)) => {
                    self.pending_flush.swap_remove(i);
                }
                Poll::Ready(Err(e)) => {
                    log::error!("Couldn't flush channel, destination: {}", coord);
                    return Poll::Ready(Err(e))
                }
                Poll::Pending => {
                    i += 1;
                }
            }
        }
        if self.pending_flush.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    /// Set the senders's send groups.
    fn set_send_groups(&mut self, send_groups: Vec<Vec<ReceiverEndpoint>>) {
        self.send_groups = send_groups;
    }

    /// Set the senders's senders.
    fn set_senders(&mut self, senders: HashMap<ReceiverEndpoint, Batcher<Out>, ahash::RandomState>) {
        self.senders = senders;
    }

    /// Get a reference to the senders's send groups.
    fn send_groups(&self) -> &[Vec<ReceiverEndpoint>] {
        self.send_groups.as_ref()
    }
}

impl<Out: ExchangeData> Default for Senders<Out> {
    fn default() -> Self {
        Self { send_groups: Default::default(), senders: Default::default(), pending_flush: Default::default() }
    }
}