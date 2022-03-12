use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::operator::sink::Sink;
use crate::operator::{ExchangeData, ExchangeDataKey, Operator, StreamElement, AsyncOperator};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, Stream};

use crate::channel::{Sender, Receiver, PollSender, channel};

#[pin_project::pin_project]
#[derive(Debug)]
pub struct CollectChannelSink<Out: ExchangeData, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    #[pin]
    prev: PreviousOperators,
    metadata: Option<ExecutionMetadata>,
    pending_item: Option<Out>,
    #[pin]
    tx: PollSender<Out>,
    terminated: bool,
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
        match self.prev.next() {
            StreamElement::Item(t) | StreamElement::Timestamped(t, _) => {
                log::warn!("USING BLOCKING CHANNEL SINK");
                let tx = self.tx.get_ref().cloned().unwrap();
                match tx.blocking_send(t) {
                    Ok(()) => StreamElement::Item(()),
                    Err(_) => panic!(),
                }
            }
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::Terminate => StreamElement::Terminate,
            StreamElement::FlushBatch => StreamElement::FlushBatch,
            StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
            StreamElement::Yield => StreamElement::Yield,
        }
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

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut proj = self.project();
        if *proj.terminated {
            return Poll::Ready(None);
        }
        loop {
            if let Some(item) = proj.pending_item.take() {
                match proj.tx.as_mut().poll_reserve(cx) {
                    Poll::Ready(Ok(_)) => {
                        proj.tx.send_item(item).ok().unwrap(); // TODO: check
                        return Poll::Ready(Some(StreamElement::Item(())))
                    }
                    Poll::Ready(Err(_)) => {
                        panic!("Channel disconnected");
                    }
                    Poll::Pending => {
                        *proj.pending_item = Some(item);
                        return Poll::Pending;
                    }
                }
            }

            assert!(proj.pending_item.is_none());
            let r = match ready!(proj.prev.as_mut().poll_next(cx)).unwrap_or_else(|| StreamElement::Terminate) {
                StreamElement::Item(t) | StreamElement::Timestamped(t, _) => {
                    *proj.pending_item = Some(t);
                    continue;
                }
                StreamElement::Watermark(w) => StreamElement::Watermark(w),
                StreamElement::Terminate => {
                    *proj.terminated = true;
                    return Poll::Ready(None);
                }
                StreamElement::FlushBatch => StreamElement::FlushBatch,
                StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
                StreamElement::Yield => panic!("Should never receive a yield!"),
            };

            return Poll::Ready(Some(r));
        }
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
            .add_operator(|prev| CollectChannelSink {
                prev,
                tx: PollSender::new(tx),
                metadata: None,
                pending_item: None,
                terminated: false,
            })
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
            .add_async_operator(|prev| CollectChannelSink {
                prev,
                tx: PollSender::new(tx),
                metadata: None,
                pending_item: None,
                terminated: false,
            })
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
