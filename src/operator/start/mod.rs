use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

pub(crate) use multiple::*;
pub(crate) use single::*;

use crate::block::BlockStructure;
use crate::channel::{RecvTimeoutError, TryRecvError};
use crate::network::{Coord, NetworkDataIterator, NetworkMessage};
use crate::operator::iteration::IterationStateLock;
use crate::operator::source::Source;
use crate::operator::start::watermark_frontier::WatermarkFrontier;
use crate::operator::{timestamp_max, ExchangeData, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::BlockId;

mod multiple;
mod single;
mod watermark_frontier;

/// Trait that abstract the receiving part of the `StartBlock`.
pub(crate) trait StartBlockReceiver<Out>: Clone {
    /// Setup the internal state of the receiver.
    fn setup(&mut self, metadata: ExecutionMetadata);

    /// Obtain the list of all the previous replicas.
    ///
    /// This list should contain all the replicas this receiver will receive data from.
    fn prev_replicas(&self) -> Vec<Coord>;

    /// The number of those replicas which are behind a cache, and therefore never will emit a
    /// `StreamElement::Terminate` message.
    fn cached_replicas(&self) -> usize;

    /// Try to receive a batch from the previous blocks, or fail with an error if the timeout
    /// expires.
    fn recv_timeout(&mut self, timeout: Duration) -> Result<NetworkMessage<Out>, RecvTimeoutError>;

    /// Receive a batch from the previous blocks waiting indefinitely.
    fn recv(&mut self) -> NetworkMessage<Out>;

    /// Try receiving a batch without blocking.
    fn try_recv(&mut self) -> Result<NetworkMessage<Out>, TryRecvError>;

    /// Like `Operator::structure`.
    fn structure(&self) -> BlockStructure;
}

pub(crate) type MultipleStartBlockReceiverOperator<OutL, OutR> =
    StartBlock<TwoSidesItem<OutL, OutR>, MultipleStartBlockReceiver<OutL, OutR>>;

pub(crate) type SingleStartBlockReceiverOperator<Out> =
    StartBlock<Out, SingleStartBlockReceiver<Out>>;

/// Each block should start with a `StartBlock` operator, whose task is to read from the network,
/// receive from the previous operators and handle the watermark frontier.
///
/// There are different kinds of `StartBlock`, the main difference is in the number of previous
/// blocks. With a `SingleStartBlockReceiver` the block is able to receive from the replicas of a
/// single block of the job graph. If the block needs the data from multiple blocks it should use
/// `MultipleStartBlockReceiver` which is able to handle 2 previous blocks.
///
/// Following operators will receive the messages in an unspecified order but the watermark property
/// is followed. Note that the timestamps of the messages are not sorted, it's only guaranteed that
/// when a watermark is emitted, all the previous messages are already been emitted (in some order).
#[derive(Clone, Debug)]
pub(crate) struct StartBlock<Out: ExchangeData, Receiver: StartBlockReceiver<Out> + Send> {
    /// Execution metadata of this block.
    metadata: Option<ExecutionMetadata>,

    /// The actual receiver able to fetch messages from the network.
    receiver: Receiver,

    /// Inner iterator over batch items, contains coordinate of the sender
    batch_iter: Option<(Coord, NetworkDataIterator<StreamElement<Out>>)>,

    /// The number of `StreamElement::Terminate` messages yet to be received. When this value
    /// reaches zero this operator will emit the terminate.
    missing_terminate: usize,
    /// The number of `StreamElement::FlushAndRestart` messages yet to be received.
    missing_flush_and_restart: usize,
    /// The total number of replicas in the previous blocks. This is used for resetting
    /// `missing_flush_and_restart`.
    num_previous_replicas: usize,

    /// Whether the previous blocks timed out and the last batch has been flushed.
    ///
    /// The next time `next()` is called it will not wait the timeout asked by the batch mode.
    already_timed_out: bool,

    /// The current frontier of the watermarks from the previous replicas.
    watermark_frontier: WatermarkFrontier,

    /// Whether the iteration has ended and the current block has to wait for the local iteration
    /// leader to update the iteration state before letting the messages pass.
    wait_for_state: bool,
    state_lock: Option<Arc<IterationStateLock>>,
    state_generation: usize,
}

impl<Out: ExchangeData> StartBlock<Out, SingleStartBlockReceiver<Out>> {
    /// Create a `StartBlock` able to receive data only from a single previous block.
    pub(crate) fn single(
        previous_block_id: BlockId,
        state_lock: Option<Arc<IterationStateLock>>,
    ) -> SingleStartBlockReceiverOperator<Out> {
        StartBlock::new(SingleStartBlockReceiver::new(previous_block_id), state_lock)
    }
}

impl<OutL: ExchangeData, OutR: ExchangeData>
    StartBlock<TwoSidesItem<OutL, OutR>, MultipleStartBlockReceiver<OutL, OutR>>
{
    /// Create a `StartBlock` able to receive data from 2 previous blocks, setting up the cache.
    pub(crate) fn multiple(
        previous_block_id1: BlockId,
        previous_block_id2: BlockId,
        left_cache: bool,
        right_cache: bool,
        state_lock: Option<Arc<IterationStateLock>>,
    ) -> MultipleStartBlockReceiverOperator<OutL, OutR> {
        StartBlock::new(
            MultipleStartBlockReceiver::new(
                previous_block_id1,
                previous_block_id2,
                left_cache,
                right_cache,
            ),
            state_lock,
        )
    }
}

impl<Out: ExchangeData, Receiver: StartBlockReceiver<Out> + Send> StartBlock<Out, Receiver> {
    fn new(receiver: Receiver, state_lock: Option<Arc<IterationStateLock>>) -> Self {
        Self {
            metadata: Default::default(),

            receiver,
            batch_iter: None,

            missing_terminate: Default::default(),
            missing_flush_and_restart: Default::default(),
            num_previous_replicas: 0,

            already_timed_out: Default::default(),

            watermark_frontier: Default::default(),

            wait_for_state: Default::default(),
            state_lock,
            state_generation: Default::default(),
        }
    }

    pub(crate) fn receiver(&self) -> &Receiver {
        &self.receiver
    }
}

impl<Out: ExchangeData, Receiver: StartBlockReceiver<Out> + Send> Operator<Out>
    for StartBlock<Out, Receiver>
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.receiver.setup(metadata.clone());

        let prev_replicas = self.receiver.prev_replicas();
        self.num_previous_replicas = prev_replicas.len();
        self.missing_terminate = self.num_previous_replicas;
        self.missing_flush_and_restart = self.num_previous_replicas;
        self.watermark_frontier = WatermarkFrontier::new(prev_replicas);

        debug!(
            "StartBlock {} of {} initialized",
            metadata.coord,
            std::any::type_name::<Out>()
        );
        self.metadata = Some(metadata);
    }

    fn next(&mut self) -> StreamElement<Out> {
        let metadata = self.metadata.as_ref().unwrap();

        // all the previous blocks sent an end: we're done
        if self.missing_terminate == 0 {
            info!("StartBlock for {} has ended", metadata.coord);
            return StreamElement::Terminate;
        }
        if self.missing_flush_and_restart == 0 {
            info!(
                "StartBlock for {} is emitting flush and restart",
                metadata.coord
            );

            self.missing_flush_and_restart = self.num_previous_replicas;
            self.watermark_frontier.reset();
            // this iteration has ended, before starting the next one wait for the state update
            self.wait_for_state = true;
            self.state_generation += 2;
            return StreamElement::FlushAndRestart;
        }

        // let max_delay = metadata.batch_mode.max_delay();

        if let Some((sender, ref mut inner)) = self.batch_iter {
            let msg = match inner.next() {
                None => {
                    // Current batch is finished
                    self.batch_iter = None;
                    return self.next();
                }
                Some(item) => {
                    match item {
                        StreamElement::Watermark(ts) => {
                            // update the frontier and return a watermark if necessary
                            match self.watermark_frontier.update(sender, ts) {
                                Some(ts) => StreamElement::Watermark(ts), // ts is safe
                                None => return self.next(),
                            }
                        }
                        StreamElement::FlushAndRestart => {
                            // mark this replica as ended and let the frontier ignore it from now on
                            self.watermark_frontier.update(sender, timestamp_max());
                            self.missing_flush_and_restart -= 1;
                            return self.next();
                        }
                        StreamElement::Terminate => {
                            self.missing_terminate -= 1;
                            debug!(
                                "{} received a Terminate, {} more to come",
                                metadata.coord, self.missing_terminate
                            );
                            return self.next();
                        }
                        StreamElement::Yield => {
                            panic!("Start block received Yield! This should never happen!");
                        }
                        _ => item,
                    }
                }
            };

            // the previous iteration has ended, this message refers to the new iteration: we need to be
            // sure the state is set before we let this message pass
            if self.wait_for_state {
                if let Some(lock) = self.state_lock.as_ref() {
                    lock.wait_for_update(self.state_generation);
                }
                self.wait_for_state = false;
            }
            return msg;
        }

        let mut cnt = 0;
        loop {
            match self.receiver.try_recv() {
                Ok(net_msg) => {
                    self.batch_iter = Some((net_msg.sender(), net_msg.into_iter()));
                    return self.next();
                }
                Err(TryRecvError::Empty) if cnt < 8 => cnt += 1, // TODO: park
                Err(TryRecvError::Empty) => {
                    log::debug!("Empty input, yielding {}", self.metadata.as_ref().unwrap().coord);
                    return StreamElement::Yield
                }, // TODO: park
                Err(TryRecvError::Disconnected) => unimplemented!(),
            }
        }
    }

    fn to_string(&self) -> String {
        format!("[{}]", std::any::type_name::<Out>())
    }

    fn structure(&self) -> BlockStructure {
        self.receiver.structure()
    }
}

impl<Out: ExchangeData, Receiver: StartBlockReceiver<Out> + Send> Source<Out>
    for StartBlock<Out, Receiver>
{
    fn get_max_parallelism(&self) -> Option<usize> {
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::network::NetworkMessage;
    use crate::operator::{Operator, StartBlock, StreamElement, Timestamp, TwoSidesItem};
    use crate::test::FakeNetworkTopology;

    fn ts(millis: u64) -> Timestamp {
        Timestamp::from_millis(millis)
    }

    #[test]
    fn test_single() {
        let (metadata, mut senders) = FakeNetworkTopology::single_replica(1, 2);
        let (from1, sender1) = senders[0].pop().unwrap();
        let (from2, sender2) = senders[0].pop().unwrap();

        let mut start_block =
            StartBlock::<i32, _>::single(sender1.receiver_endpoint.prev_block_id, None);
        start_block.setup(metadata);

        sender1
            .send(NetworkMessage::new_batch(
                vec![StreamElement::Item(42), StreamElement::FlushAndRestart],
                from1,
            ))
            .unwrap();

        assert_eq!(StreamElement::Item(42), start_block.next());
        assert_eq!(StreamElement::FlushBatch, start_block.next());

        sender2
            .send(NetworkMessage::new_batch(
                vec![StreamElement::FlushAndRestart],
                from2,
            ))
            .unwrap();

        assert_eq!(StreamElement::FlushAndRestart, start_block.next());

        sender1
            .send(NetworkMessage::new_single(StreamElement::Terminate, from1))
            .unwrap();
        sender2
            .send(NetworkMessage::new_single(StreamElement::Terminate, from2))
            .unwrap();

        assert_eq!(StreamElement::Terminate, start_block.next());
    }

    #[test]
    fn test_single_watermark() {
        let (metadata, mut senders) = FakeNetworkTopology::single_replica(1, 2);
        let (from1, sender1) = senders[0].pop().unwrap();
        let (from2, sender2) = senders[0].pop().unwrap();

        let mut start_block =
            StartBlock::<i32, _>::single(sender1.receiver_endpoint.prev_block_id, None);
        start_block.setup(metadata);

        sender1
            .send(NetworkMessage::new_batch(
                vec![
                    StreamElement::Timestamped(42, ts(10)),
                    StreamElement::Watermark(ts(20)),
                ],
                from1,
            ))
            .unwrap();

        assert_eq!(StreamElement::Timestamped(42, ts(10)), start_block.next());
        assert_eq!(StreamElement::FlushBatch, start_block.next());

        sender2
            .send(NetworkMessage::new_batch(
                vec![StreamElement::Watermark(ts(100))],
                from2,
            ))
            .unwrap();

        assert_eq!(StreamElement::Watermark(ts(20)), start_block.next());
        assert_eq!(StreamElement::FlushBatch, start_block.next());

        sender1
            .send(NetworkMessage::new_batch(
                vec![StreamElement::FlushAndRestart],
                from1,
            ))
            .unwrap();
        sender2
            .send(NetworkMessage::new_batch(
                vec![StreamElement::Watermark(ts(110))],
                from2,
            ))
            .unwrap();
        assert_eq!(StreamElement::Watermark(ts(110)), start_block.next());
    }

    #[test]
    fn test_multiple_no_cache() {
        let (metadata, mut senders) = FakeNetworkTopology::single_replica(2, 1);
        let (from1, sender1) = senders[0].pop().unwrap();
        let (from2, sender2) = senders[1].pop().unwrap();

        let mut start_block = StartBlock::<TwoSidesItem<i32, i32>, _>::multiple(
            from1.block_id,
            from2.block_id,
            false,
            false,
            None,
        );
        start_block.setup(metadata);

        sender1
            .send(NetworkMessage::new_batch(
                vec![
                    StreamElement::Timestamped(42, ts(10)),
                    StreamElement::Watermark(ts(20)),
                ],
                from1,
            ))
            .unwrap();

        assert_eq!(
            StreamElement::Timestamped(TwoSidesItem::Left(42), ts(10)),
            start_block.next()
        );
        assert_eq!(StreamElement::FlushBatch, start_block.next());

        sender2
            .send(NetworkMessage::new_batch(
                vec![
                    StreamElement::Timestamped(69, ts(10)),
                    StreamElement::Watermark(ts(20)),
                ],
                from2,
            ))
            .unwrap();

        assert_eq!(
            StreamElement::Timestamped(TwoSidesItem::Right(69), ts(10)),
            start_block.next()
        );
        assert_eq!(StreamElement::Watermark(ts(20)), start_block.next());

        sender1
            .send(NetworkMessage::new_batch(
                vec![StreamElement::FlushAndRestart],
                from1,
            ))
            .unwrap();
        assert_eq!(
            StreamElement::Item(TwoSidesItem::LeftEnd),
            start_block.next()
        );
        sender2
            .send(NetworkMessage::new_batch(
                vec![StreamElement::FlushAndRestart],
                from2,
            ))
            .unwrap();
        assert_eq!(
            StreamElement::Item(TwoSidesItem::RightEnd),
            start_block.next()
        );
        assert_eq!(StreamElement::FlushAndRestart, start_block.next());
    }

    #[test]
    fn test_multiple_cache() {
        let (metadata, mut senders) = FakeNetworkTopology::single_replica(2, 1);
        let (from1, sender1) = senders[0].pop().unwrap();
        let (from2, sender2) = senders[1].pop().unwrap();

        let mut start_block = StartBlock::<TwoSidesItem<i32, i32>, _>::multiple(
            from1.block_id,
            from2.block_id,
            true,
            false,
            None,
        );
        start_block.setup(metadata);

        sender1
            .send(NetworkMessage::new_single(StreamElement::Item(42), from1))
            .unwrap();
        sender1
            .send(NetworkMessage::new_single(StreamElement::Item(43), from1))
            .unwrap();
        sender2
            .send(NetworkMessage::new_single(StreamElement::Item(69), from2))
            .unwrap();

        let mut recv = [start_block.next(), start_block.next(), start_block.next()];
        recv.sort(); // those messages can arrive unordered
        assert_eq!(StreamElement::Item(TwoSidesItem::Left(42)), recv[0]);
        assert_eq!(StreamElement::Item(TwoSidesItem::Left(43)), recv[1]);
        assert_eq!(StreamElement::Item(TwoSidesItem::Right(69)), recv[2]);

        sender1
            .send(NetworkMessage::new_batch(
                vec![StreamElement::FlushAndRestart, StreamElement::Terminate],
                from1,
            ))
            .unwrap();
        sender2
            .send(NetworkMessage::new_batch(
                vec![StreamElement::FlushAndRestart],
                from2,
            ))
            .unwrap();

        let mut recv = [start_block.next(), start_block.next()];
        recv.sort(); // those messages can arrive unordered
        assert_eq!(StreamElement::Item(TwoSidesItem::LeftEnd), recv[0]);
        assert_eq!(StreamElement::Item(TwoSidesItem::RightEnd), recv[1]);

        assert_eq!(StreamElement::FlushAndRestart, start_block.next());

        sender2
            .send(NetworkMessage::new_batch(
                vec![StreamElement::Item(6969), StreamElement::FlushAndRestart],
                from2,
            ))
            .unwrap();

        let mut recv = [
            start_block.next(),
            start_block.next(),
            start_block.next(),
            start_block.next(),
            start_block.next(),
        ];
        recv.sort(); // those messages can arrive unordered
        assert_eq!(StreamElement::Item(TwoSidesItem::Left(42)), recv[0]);
        assert_eq!(StreamElement::Item(TwoSidesItem::Left(43)), recv[1]);
        assert_eq!(StreamElement::Item(TwoSidesItem::Right(6969)), recv[2]);
        assert_eq!(StreamElement::Item(TwoSidesItem::LeftEnd), recv[3]);
        assert_eq!(StreamElement::Item(TwoSidesItem::RightEnd), recv[4]);

        assert_eq!(StreamElement::FlushAndRestart, start_block.next());

        sender2
            .send(NetworkMessage::new_single(StreamElement::Terminate, from2))
            .unwrap();

        assert_eq!(StreamElement::Terminate, start_block.next());
    }

    #[test]
    fn test_multiple_cache_other_side() {
        let (metadata, mut senders) = FakeNetworkTopology::single_replica(2, 1);
        let (from1, sender1) = senders[0].pop().unwrap();
        let (from2, sender2) = senders[1].pop().unwrap();

        let mut start_block = StartBlock::<TwoSidesItem<i32, i32>, _>::multiple(
            from1.block_id,
            from2.block_id,
            false,
            true,
            None,
        );
        start_block.setup(metadata);

        sender1
            .send(NetworkMessage::new_single(StreamElement::Item(42), from1))
            .unwrap();
        sender1
            .send(NetworkMessage::new_single(StreamElement::Item(43), from1))
            .unwrap();
        sender2
            .send(NetworkMessage::new_single(StreamElement::Item(69), from2))
            .unwrap();

        let mut recv = [start_block.next(), start_block.next(), start_block.next()];
        recv.sort(); // those messages can arrive unordered
        assert_eq!(StreamElement::Item(TwoSidesItem::Left(42)), recv[0]);
        assert_eq!(StreamElement::Item(TwoSidesItem::Left(43)), recv[1]);
        assert_eq!(StreamElement::Item(TwoSidesItem::Right(69)), recv[2]);

        sender1
            .send(NetworkMessage::new_batch(
                vec![StreamElement::FlushAndRestart, StreamElement::Terminate],
                from1,
            ))
            .unwrap();
        sender2
            .send(NetworkMessage::new_batch(
                vec![StreamElement::FlushAndRestart],
                from2,
            ))
            .unwrap();

        let mut recv = [start_block.next(), start_block.next()];
        recv.sort(); // those messages can arrive unordered
        assert_eq!(StreamElement::Item(TwoSidesItem::LeftEnd), recv[0]);
        assert_eq!(StreamElement::Item(TwoSidesItem::RightEnd), recv[1]);

        assert_eq!(StreamElement::FlushAndRestart, start_block.next());

        sender2
            .send(NetworkMessage::new_single(StreamElement::Terminate, from2))
            .unwrap();

        assert_eq!(StreamElement::Terminate, start_block.next());
    }
}
