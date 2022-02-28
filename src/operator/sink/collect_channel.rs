use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::operator::sink::Sink;
use crate::operator::{ExchangeData, ExchangeDataKey, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, Stream};

#[cfg(feature = "crossbeam")]
use crossbeam_channel::{unbounded, Receiver, Sender};
use flume::TrySendError;
#[cfg(not(feature = "crossbeam"))]
use flume::{unbounded, Receiver, Sender};

#[derive(Debug)]
pub struct CollectChannelSink<Out: ExchangeData, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    metadata: Option<ExecutionMetadata>,
    pending_item: Option<Out>,
    tx: Option<Sender<Out>>,
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
        if let Some(item) = self.pending_item.take() {
            let tx = self.tx.as_ref().unwrap();
            match tx.try_send(item) {
                Ok(()) => {}
                Err(TrySendError::Full(t)) => {
                    log::debug!("Output channel full, yeielding {}", self.metadata.as_ref().unwrap().coord);
                    self.pending_item = Some(t);
                    return StreamElement::Yield;
                }
                Err(TrySendError::Disconnected(_)) => panic!(),
            }
        }

        match self.prev.next() {
            StreamElement::Item(t) | StreamElement::Timestamped(t, _) => {
                let tx = self.tx.as_ref().unwrap();
                match tx.try_send(t) {
                    Ok(()) => StreamElement::Item(()),
                    Err(TrySendError::Full(t)) => {
                        log::debug!("Output channel full, yeielding {}", self.metadata.as_ref().unwrap().coord);
                        self.pending_item = Some(t);
                        StreamElement::Yield
                    }
                    Err(TrySendError::Disconnected(_)) => panic!(),
                }
            }
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::Terminate => {
                self.tx = None;
                StreamElement::Terminate
            }
            StreamElement::FlushBatch => StreamElement::FlushBatch,
            StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
            StreamElement::Yield => StreamElement::Yield, //TODO: Check
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
    pub fn collect_channel(self) -> Receiver<Out> {
        let (tx, rx) = unbounded();
        self.max_parallelism(1)
            .add_operator(|prev| CollectChannelSink {
                prev,
                tx: Some(tx),
                metadata: None,
                pending_item: None,
            })
            .finalize_block();
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
    pub fn collect_channel(self) -> Receiver<(Key, Out)> {
        self.unkey().collect_channel()
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
        let rx = env.stream(source).collect_channel();
        env.execute();
        let mut v = Vec::new();
        while let Ok(x) = rx.recv() {
            v.push(x)
        }
        assert_eq!(v, (0..10).collect_vec());
    }
}
