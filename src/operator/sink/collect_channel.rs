use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::{ready, StreamExt};
use futures::SinkExt;
use tokio::time::Interval;
use tokio_util::sync::ReusableBoxFuture;
use tracing::instrument;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::operator::sink::Sink;
use crate::operator::{ExchangeData, ExchangeDataKey, Operator, StreamElement, AsyncOperator};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, Stream};

use crate::channel::{Receiver, Sender, channel, SendError};

#[derive(Debug)]
enum SendState<T: ExchangeData> {
    Idle(Sender<T>),
    Staged,
    Closed,
}

impl<T: ExchangeData> SendState<T> {
    pub fn is_idle(&self) -> bool {
        match &self {
            SendState::Idle(_) => true,
            _ => false,
        }
    }

    pub fn take(&mut self) -> Self {
        std::mem::replace(self, Self::Closed)
    }

    pub fn take_sender(&mut self) -> Sender<T> {
        let prev = std::mem::replace(self, SendState::Closed);
        match prev {
            SendState::Idle(t) => t,
            _ => panic!("Trying to take sender from invalid FlushState")
        }
    }
}


// By reusing the same async fn for both `Some` and `None`, we make sure every future passed to
// ReusableBoxFuture has the same underlying type, and hence the same size and alignment.
async fn make_send_future<T: ExchangeData>(
    data: Option<(Sender<T>, T)>,
) -> Result<Sender<T>, SendError<T>> {
    match data {
        Some((sender, msg)) => sender
                .send(msg)
                .await
                .map(move |_| sender),
        None => unreachable!("this future should not be pollable in this state"),
    }
}


#[derive(Debug)]
pub struct CollectChannelSink<Out: ExchangeData, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    metadata: Option<ExecutionMetadata>,
    interval: Interval,
    state: SendState<Out>,
    pending: ReusableBoxFuture<'static, Result<Sender<Out>, SendError<Out>>>,
    terminated: bool,
}

impl<Out: ExchangeData, PreviousOperators> CollectChannelSink<Out, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    pub fn new(prev: PreviousOperators, sender: Sender<Out>) -> Self {
        Self {
            prev,
            state: SendState::Idle(sender),
            pending: ReusableBoxFuture::new(make_send_future(None)),
            metadata: None,
            terminated: false,
            interval: tokio::time::interval(Duration::from_micros(100000)),
        }
    }
}

impl<Out: ExchangeData, PreviousOperators> Operator<()>
    for CollectChannelSink<Out, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.metadata = Some(metadata.clone());
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<()> {
        todo!();
        // match self.prev.next() {
        //     StreamElement::Item(t) | StreamElement::Timestamped(t, _) => {
        //         log::warn!("USING BLOCKING CHANNEL SINK");
        //         let tx = self.tx.get_ref().cloned().unwrap();
        //         match tx.blocking_send(t) {
        //             Ok(()) => StreamElement::Item(()),
        //             Err(_) => panic!(),
        //         }
        //     }
        //     StreamElement::Watermark(w) => StreamElement::Watermark(w),
        //     StreamElement::Terminate => StreamElement::Terminate,
        //     StreamElement::FlushBatch => StreamElement::FlushBatch,
        //     StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
        //     StreamElement::Yield => StreamElement::Yield,
        // }
    }

    fn to_string(&self) -> String {
        format!("{} -> CollectChannelSink", self.prev.to_string())
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("CollectChannelSink");
        operator.kind = OperatorKind::Sink;
        self.prev.structure().add_operator(operator)
    }
}


impl<Out: ExchangeData, PreviousOperators> futures::Stream
    for CollectChannelSink<Out, PreviousOperators>
where
    PreviousOperators: AsyncOperator<Out>,
{
    type Item = StreamElement<()>;

    #[instrument(name = "collect_channel_poll", skip_all)]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut proj = self.get_mut();
        // let _ = proj.interval.poll_tick(cx); //TODO: Remove and try to properly handle the waker
        if proj.terminated {
            return Poll::Ready(None);
        }

        macro_rules! try_send {
            () => {
                match proj.pending.poll(cx) {
                    Poll::Ready(Ok(sender)) => {
                        tracing::trace!("sent");
                        // cx.waker().wake_by_ref();
                        (Poll::Ready(Some(StreamElement::Item(()))), SendState::Idle(sender))
                    }
                    Poll::Ready(Err(e)) => {
                        tracing::error!("Cannot flush to channel sink! {}", e);
                        (Poll::Ready(None), SendState::Closed)
                    }
                    Poll::Pending => {
                        tracing::debug!("channel sink full!");
                        (Poll::Pending, SendState::Staged)
                    }
                }
            }
        }

        let (result, next_state) = match proj.state.take() {
            SendState::Idle(sender) => {
                tracing::trace!("poll_prev");
                match proj.prev.poll_next_unpin(cx) {
                    Poll::Ready(Some(item)) => match item {
                        StreamElement::Item(t) | StreamElement::Timestamped(t, _) => {
                            proj.pending.set(make_send_future(Some((sender, t))));
                            tracing::trace!("queued_send");
                            try_send!()
                        }
                        StreamElement::Terminate => {
                            proj.terminated = true;
                            (Poll::Ready(Some(StreamElement::Terminate)), SendState::Idle(sender))
                        }
                        el => (Poll::Ready(Some(el.take())), SendState::Idle(sender)),
                    }
                    Poll::Ready(None) => {
                        log::error!("Chan sink received ready none from prev");
                        proj.terminated = true;
                        (Poll::Ready(None), SendState::Closed)
                    }
                    Poll::Pending => (Poll::Pending, SendState::Idle(sender)),
                }
            }
            SendState::Staged => try_send!(),
            SendState::Closed => panic!("Trying to poll channel sink in illegal closed state!"),
        };

        proj.state = next_state;
        result

        // if proj.pending_item.is_some() {
        //     tracing::trace!("poll_tx_pending");
        //     match proj.tx.poll_reserve(cx) {
        //         Poll::Ready(Ok(_)) => {
        //             proj.tx.send_item(proj.pending_item.take().unwrap()).ok().unwrap(); // TODO: check
        //             tracing::trace!("sent");
        //             return Poll::Ready(Some(StreamElement::Item(())))
        //         }
        //         Poll::Ready(Err(_)) => {
        //             panic!("Channel disconnected");
        //         }
        //         Poll::Pending => {
        //             return Poll::Pending;
        //         }
        //     }
        // }

        // assert!(proj.pending_item.is_none());
        // tracing::trace!("poll_prev");
        // let r = match ready!(proj.prev.poll_next_unpin(cx)).unwrap_or_else(|| StreamElement::Terminate) {
        //     StreamElement::Item(t) | StreamElement::Timestamped(t, _) => {
        //         proj.pending_item = Some(t);
        //         continue;
        //     }
        //     StreamElement::Watermark(w) => StreamElement::Watermark(w),
        //     StreamElement::Terminate => {
        //         proj.terminated = true;
        //         StreamElement::Terminate
        //     }
        //     StreamElement::FlushBatch => StreamElement::FlushBatch,
        //     StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
        //     StreamElement::Yield => panic!("Should never receive a yield!"),
        // };

        // return Poll::Ready(Some(r));
    }
}

impl<Out: ExchangeData, PreviousOperators> Sink for CollectChannelSink<Out, PreviousOperators> where
    PreviousOperators: Operator<Out>
{
}

impl<Out: ExchangeData, PreviousOperators> Clone for CollectChannelSink<Out, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    fn clone(&self) -> Self {
        panic!("CollectChannelSink cannot be cloned, max_parallelism should be 1");
    }
}

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Close the stream and store all the resulting items into a [`Vec`] on a single host.
    ///
    /// If the stream is distributed among multiple replicas, a bottleneck is placed where all the
    /// replicas sends the items to.
    ///
    /// **Note**: the order of items and keys is unspecified.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..10u32)));
    /// let rx = s.collect_channel();
    ///
    /// env.execute();
    /// let mut v = Vec::new();
    /// while let Ok(x) = rx.recv() {
    ///     v.push(x)
    /// }
    /// assert_eq!(v, (0..10u32).collect::<Vec<_>>());
    /// ```
    pub fn collect_channel(self, capacity: usize) -> Receiver<Out> {
        let (tx, rx) = channel(capacity);
        self.max_parallelism(1)
            .add_operator(|prev| CollectChannelSink::new(prev, tx))
            .finalize_block();
        rx
    }
}

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: AsyncOperator<Out> + 'static,
{
    /// Close the stream and store all the resulting items into a [`Vec`] on a single host.
    ///
    /// If the stream is distributed among multiple replicas, a bottleneck is placed where all the
    /// replicas sends the items to.
    ///
    /// **Note**: the order of items and keys is unspecified.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..10u32)));
    /// let rx = s.collect_channel();
    ///
    /// env.execute();
    /// let mut v = Vec::new();
    /// while let Ok(x) = rx.recv() {
    ///     v.push(x)
    /// }
    /// assert_eq!(v, (0..10u32).collect::<Vec<_>>());
    /// ```
    pub fn collect_channel_async(self, capacity: usize) -> Receiver<Out> {
        let (tx, rx) = channel(capacity);
        self.max_parallelism_async(1)
            .add_async_operator(|prev| CollectChannelSink::new(prev, tx))
            .finalize_block_async();
        rx
    }
}

impl<Key: ExchangeDataKey, Out: ExchangeData, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Close the stream and store all the resulting items into a [`Vec`] on a single host.
    ///
    /// If the stream is distributed among multiple replicas, a bottleneck is placed where all the
    /// replicas sends the items to.
    ///
    /// **Note**: the collected items are the pairs `(key, value)`.
    ///
    /// **Note**: the order of items and keys is unspecified.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..3))).group_by(|&n| n % 2);
    /// let rx = s.collect_channel();
    ///
    /// env.execute();
    /// let mut v = Vec::new();
    /// while let Ok(x) = rx.recv() {
    ///     v.push(x)
    /// }
    /// v.sort_unstable(); // the output order is nondeterministic
    /// assert_eq!(v, vec![(0, 0), (0, 2), (1, 1)]);
    /// ```
    pub fn collect_channel(self, capacity: usize) -> Receiver<(Key, Out)> {
        self.unkey().collect_channel(capacity)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source;

    #[test]
    fn collect_channel() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source = source::IteratorSource::new(0..10u8);
        let rx = env.stream(source).collect_channel(200);
        env.execute();
        let mut v = Vec::new();
        while let Ok(x) = rx.recv() {
            v.push(x)
        }
        assert_eq!(v, (0..10).collect_vec());
    }
}
