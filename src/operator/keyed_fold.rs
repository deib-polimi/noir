use core::iter::Iterator;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{ready, StreamExt};

use crate::block::{BlockStructure, NextStrategy, OperatorStructure};
use crate::operator::end::EndBlock;
use crate::operator::key_by::KeyBy;
use crate::operator::{
    Data, DataKey, ExchangeData, ExchangeDataKey, Operator, StreamElement, Timestamp,
};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, Stream};

use super::AsyncOperator;

#[derive(Derivative)]
#[derivative(Debug, Clone)]
pub struct KeyedFold<Key: DataKey, Out: Data, NewOut: Data, F, PreviousOperators>
where
    F: Fn(&mut NewOut, Out) + Send + Clone,
    PreviousOperators: Operator<KeyValue<Key, Out>>,
{
    prev: PreviousOperators,
    #[derivative(Debug = "ignore")]
    fold: F,
    init: NewOut,
    accumulators: HashMap<Key, NewOut, ahash::RandomState>,
    timestamps: HashMap<Key, Timestamp, ahash::RandomState>,
    ready: Vec<StreamElement<KeyValue<Key, NewOut>>>,
    max_watermark: Option<Timestamp>,
    received_end: bool,
    received_end_iter: bool,
    _out: PhantomData<Out>,
}

impl<Key: DataKey, Out: Data, NewOut: Data, F, PreviousOperators: Operator<KeyValue<Key, Out>>>
    KeyedFold<Key, Out, NewOut, F, PreviousOperators>
where
    F: Fn(&mut NewOut, Out) + Send + Clone,
{
    fn new(prev: PreviousOperators, init: NewOut, fold: F) -> Self {
        KeyedFold {
            prev,
            fold,
            init,
            accumulators: Default::default(),
            timestamps: Default::default(),
            ready: Default::default(),
            max_watermark: None,
            received_end: false,
            received_end_iter: false,
            _out: Default::default(),
        }
    }

    /// Process a new item, folding it with the accumulator inside the hashmap.
    fn process_item(&mut self, key: Key, value: Out) {
        match self.accumulators.entry(key) {
            Entry::Vacant(entry) => {
                let mut acc = self.init.clone();
                (self.fold)(&mut acc, value);
                entry.insert(acc);
            }
            Entry::Occupied(mut entry) => {
                (self.fold)(entry.get_mut(), value);
            }
        }
    }
}

impl<Key: DataKey, Out: Data, NewOut: Data, F, PreviousOperators> Operator<KeyValue<Key, NewOut>>
    for KeyedFold<Key, Out, NewOut, F, PreviousOperators>
where
    F: Fn(&mut NewOut, Out) + Send + Clone,
    PreviousOperators: Operator<KeyValue<Key, Out>>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<KeyValue<Key, NewOut>> {
        while !self.received_end {
            match self.prev.next() {
                StreamElement::Terminate => self.received_end = true,
                StreamElement::FlushAndRestart => {
                    self.received_end = true;
                    self.received_end_iter = true;
                }
                StreamElement::Watermark(ts) => {
                    self.max_watermark = Some(self.max_watermark.unwrap_or(ts).max(ts))
                }
                StreamElement::Item((k, v)) => {
                    self.process_item(k, v);
                }
                StreamElement::Timestamped((k, v), ts) => {
                    self.process_item(k.clone(), v);
                    self.timestamps
                        .entry(k)
                        .and_modify(|entry| *entry = (*entry).max(ts))
                        .or_insert(ts);
                }
                // this block won't sent anything until the stream ends
                StreamElement::FlushBatch => {}
                StreamElement::Yield => return StreamElement::Yield, //TODO: Check
            }
        }

        // move all the accumulators into a faster vec
        if !self.accumulators.is_empty() {
            // take a reference to move into the closure, avoiding moving "self"
            let timestamps = &mut self.timestamps;
            self.ready
                .extend(self.accumulators.drain().map(|(key, value)| {
                    if let Some(ts) = timestamps.remove(&key) {
                        StreamElement::Timestamped((key, value), ts)
                    } else {
                        StreamElement::Item((key, value))
                    }
                }));
        }

        // consume the ready items
        if let Some(elem) = self.ready.pop() {
            return elem;
        }

        if let Some(ts) = self.max_watermark.take() {
            return StreamElement::Watermark(ts);
        }

        // the end was not really the end... just the end of one iteration!
        if self.received_end_iter {
            self.received_end_iter = false;
            self.received_end = false;
            return StreamElement::FlushAndRestart;
        }

        StreamElement::Terminate
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> KeyedFold<{} -> {}>",
            self.prev.to_string(),
            std::any::type_name::<KeyValue<Key, Out>>(),
            std::any::type_name::<KeyValue<Key, NewOut>>()
        )
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<KeyValue<Key, NewOut>, _>(
                "KeyedFold",
            ))
    }
}

impl<Key: DataKey + Unpin, Out: Data + Unpin, NewOut: Data + Unpin, F, PreviousOperators> futures::Stream
    for KeyedFold<Key, Out, NewOut, F, PreviousOperators>
where
    F: Fn(&mut NewOut, Out) + Send + Clone + Unpin,
    PreviousOperators: AsyncOperator<KeyValue<Key, Out>> + Unpin,
{
    type Item = StreamElement<KeyValue<Key, NewOut>>;

    #[tracing::instrument(name = "keyed_fold", skip_all)]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        while !self.received_end {
            let next = ready!(self.prev.poll_next_unpin(cx));
            match next.unwrap_or(StreamElement::Terminate) {
                StreamElement::Terminate => {
                    tracing::trace!("recv_terminate");
                    self.received_end = true;
                }
                StreamElement::FlushAndRestart => {
                    tracing::trace!("recv_flush_and_restart");
                    self.received_end = true;
                    self.received_end_iter = true;
                }
                StreamElement::Watermark(ts) => {
                    self.max_watermark = Some(self.max_watermark.unwrap_or(ts).max(ts))
                }
                StreamElement::Item((k, v)) => {
                    self.process_item(k, v);
                }
                StreamElement::Timestamped((k, v), ts) => {
                    self.process_item(k.clone(), v);
                    self.timestamps
                        .entry(k)
                        .and_modify(|entry| *entry = (*entry).max(ts))
                        .or_insert(ts);
                }
                // this block won't sent anything until the stream ends
                StreamElement::FlushBatch => {}
                StreamElement::Yield => panic!("ASDASD"), //TODO: Check
            }
        }

        // move all the accumulators into a faster vec
        if !self.accumulators.is_empty() {
            tracing::trace!("drain");
            for (key, value) in self.accumulators.drain().collect::<Vec<_>>() {
                let e = if let Some(ts) = self.timestamps.remove(&key) {
                    StreamElement::Timestamped((key, value), ts)
                } else {
                    StreamElement::Item((key, value))
                };
                self.ready.push(e);
            }
        }

        // consume the ready items
        if let Some(elem) = self.ready.pop() {
            tracing::trace!("send_pending");
            return Poll::Ready(Some(elem));
        }

        if let Some(ts) = self.max_watermark.take() {
            tracing::trace!("send_watermark");
            return Poll::Ready(Some(StreamElement::Watermark(ts)));
        }

        // the end was not really the end... just the end of one iteration!
        if self.received_end_iter {
            tracing::trace!("flush_and_restart");
            self.received_end_iter = false;
            self.received_end = false;
            return Poll::Ready(Some(StreamElement::FlushAndRestart));
        }

        tracing::trace!("terminate");
        Poll::Ready(Some(StreamElement::Terminate))
    }
}


impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Perform the folding operation separately for each key.
    ///
    /// This is equivalent of partitioning the stream using the `keyer` function, and then applying
    /// [`Stream::fold_assoc`] to each partition separately.
    ///
    /// Note however that there is a difference between `stream.group_by(keyer).fold(...)` and
    /// `stream.group_by_fold(keyer, ...)`. The first performs the network shuffle of every item in
    /// the stream, and **later** performs the folding (i.e. nearly all the elements will be sent to
    /// the network). The latter avoids sending the items by performing first a local reduction on
    /// each host, and then send only the locally folded results (i.e. one message per replica, per
    /// key); then the global step is performed aggregating the results.
    ///
    /// The resulting stream will still be keyed and will contain only a single message per key (the
    /// final result).
    ///
    /// Note that the output type may be different from the input type, therefore requireing
    /// different function for the aggregation. Consider using [`Stream::group_by_reduce`] if the
    /// output type is the same as the input type.
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s
    ///     .group_by_fold(|&n| n % 2, 0, |acc, value| *acc += value, |acc, value| *acc += value)
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0 + 2 + 4), (1, 1 + 3)]);
    /// ```
    pub fn group_by_fold<Key: ExchangeDataKey, NewOut: ExchangeData, Keyer, Local, Global>(
        self,
        keyer: Keyer,
        init: NewOut,
        local: Local,
        global: Global,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        Keyer: Fn(&Out) -> Key + Send + Clone + 'static,
        Local: Fn(&mut NewOut, Out) + Send + Clone + 'static,
        Global: Fn(&mut NewOut, NewOut) + Send + Clone + 'static,
    {
        // GroupBy based on key
        let next_strategy = NextStrategy::GroupBy(
            move |(key, _out): &(Key, NewOut)| {
                let mut s = ahash::AHasher::default();
                key.hash(&mut s);
                s.finish() as usize
            },
            Default::default(),
        );

        let new_stream = self
            // key_by with given keyer
            .add_operator(|prev| KeyBy::new(prev, keyer.clone()))
            // local fold
            .add_operator(|prev| KeyedFold::new(prev, init.clone(), local))
            // group by key
            .add_block(EndBlock::new, next_strategy)
            // global fold
            .add_operator(|prev| KeyedFold::new(prev, init.clone(), global));

        KeyedStream(new_stream)
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Perform the folding operation separately for each key.
    ///
    /// Note that there is a difference between `stream.group_by(keyer).fold(...)` and
    /// `stream.group_by_fold(keyer, ...)`. The first performs the network shuffle of every item in
    /// the stream, and **later** performs the folding (i.e. nearly all the elements will be sent to
    /// the network). The latter avoids sending the items by performing first a local reduction on
    /// each host, and then send only the locally folded results (i.e. one message per replica, per
    /// key); then the global step is performed aggregating the results.
    ///
    /// The resulting stream will still be keyed and will contain only a single message per key (the
    /// final result).
    ///
    /// Note that the output type may be different from the input type. Consider using
    /// [`KeyedStream::reduce`] if the output type is the same as the input type.
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5))).group_by(|&n| n % 2);
    /// let res = s
    ///     .fold(0, |acc, value| *acc += value)
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0 + 2 + 4), (1, 1 + 3)]);
    /// ```
    pub fn fold<NewOut: Data, F>(
        self,
        init: NewOut,
        f: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        F: Fn(&mut NewOut, Out) + Send + Clone + 'static,
    {
        self.add_operator(|prev| KeyedFold::new(prev, init, f))
    }
}

impl<Out: Data + Unpin, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: AsyncOperator<Out> + Unpin + 'static,
{
    /// Perform the folding operation separately for each key.
    ///
    /// This is equivalent of partitioning the stream using the `keyer` function, and then applying
    /// [`Stream::fold_assoc`] to each partition separately.
    ///
    /// Note however that there is a difference between `stream.group_by(keyer).fold(...)` and
    /// `stream.group_by_fold(keyer, ...)`. The first performs the network shuffle of every item in
    /// the stream, and **later** performs the folding (i.e. nearly all the elements will be sent to
    /// the network). The latter avoids sending the items by performing first a local reduction on
    /// each host, and then send only the locally folded results (i.e. one message per replica, per
    /// key); then the global step is performed aggregating the results.
    ///
    /// The resulting stream will still be keyed and will contain only a single message per key (the
    /// final result).
    ///
    /// Note that the output type may be different from the input type, therefore requireing
    /// different function for the aggregation. Consider using [`Stream::group_by_reduce`] if the
    /// output type is the same as the input type.
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s
    ///     .group_by_fold(|&n| n % 2, 0, |acc, value| *acc += value, |acc, value| *acc += value)
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0 + 2 + 4), (1, 1 + 3)]);
    /// ```
    pub fn group_by_fold_async<Key: ExchangeDataKey, NewOut: ExchangeData, Keyer, Local, Global>(
        self,
        keyer: Keyer,
        init: NewOut,
        local: Local,
        global: Global,
    ) -> KeyedStream<Key, NewOut, impl AsyncOperator<KeyValue<Key, NewOut>>>
    where
        Keyer: Fn(&Out) -> Key + Send + Clone + Unpin + 'static,
        Local: Fn(&mut NewOut, Out) + Send + Clone + Unpin + 'static,
        Global: Fn(&mut NewOut, NewOut) + Send + Clone + Unpin + 'static,
    {
        // GroupBy based on key
        // let next_strategy = NextStrategy::group_by(|(key, _out): &(Key, NewOut)| key.clone());

        let next_strategy = NextStrategy::GroupBy(
            move |(key, _out): &(Key, NewOut)| {
                let mut s = ahash::AHasher::default();
                key.hash(&mut s);
                s.finish() as usize
            },
            Default::default(),
        );

        let new_stream = self
            // key_by with given keyer
            .add_async_operator(|prev| KeyBy::new(prev, keyer.clone()))
            // local fold
            .add_async_operator(|prev| KeyedFold::new(prev, init.clone(), local))
            // group by key
            .add_async_block(EndBlock::new, next_strategy)
            // global fold
            .add_async_operator(|prev| KeyedFold::new(prev, init.clone(), global));

        KeyedStream(new_stream)
    }
}

impl<Key: DataKey + Unpin, Out: Data + Unpin, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: AsyncOperator<KeyValue<Key, Out>> + Unpin + 'static,
{
    /// Perform the folding operation separately for each key.
    ///
    /// Note that there is a difference between `stream.group_by(keyer).fold(...)` and
    /// `stream.group_by_fold(keyer, ...)`. The first performs the network shuffle of every item in
    /// the stream, and **later** performs the folding (i.e. nearly all the elements will be sent to
    /// the network). The latter avoids sending the items by performing first a local reduction on
    /// each host, and then send only the locally folded results (i.e. one message per replica, per
    /// key); then the global step is performed aggregating the results.
    ///
    /// The resulting stream will still be keyed and will contain only a single message per key (the
    /// final result).
    ///
    /// Note that the output type may be different from the input type. Consider using
    /// [`KeyedStream::reduce`] if the output type is the same as the input type.
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5))).group_by(|&n| n % 2);
    /// let res = s
    ///     .fold(0, |acc, value| *acc += value)
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0 + 2 + 4), (1, 1 + 3)]);
    /// ```
    pub fn fold_async<NewOut: Data + Unpin, F>(
        self,
        init: NewOut,
        f: F,
    ) -> KeyedStream<Key, NewOut, impl AsyncOperator<KeyValue<Key, NewOut>>>
    where
        F: Fn(&mut NewOut, Out) + Send + Clone + Unpin + 'static,
    {
        self.add_async_operator(|prev| KeyedFold::new(prev, init, f))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use itertools::Itertools;

    use crate::operator::keyed_fold::KeyedFold;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    #[allow(clippy::identity_op)]
    fn test_keyed_fold_no_timestamp() {
        let data = (0..10u8).map(|x| (x % 2, x)).collect_vec();
        let fake_operator = FakeOperator::new(data.into_iter());
        let mut keyed_fold = KeyedFold::new(fake_operator, 0, |a, b| *a += b);

        let mut res = vec![];
        for _ in 0..2 {
            let item = keyed_fold.next();
            match item {
                StreamElement::Item(x) => res.push(x),
                other => panic!("Expecting StreamElement::Item, got {}", other.variant()),
            }
        }

        assert_eq!(keyed_fold.next(), StreamElement::Terminate);

        res.sort_unstable();
        assert_eq!(res[0].1, 0 + 2 + 4 + 6 + 8);
        assert_eq!(res[1].1, 1 + 3 + 5 + 7 + 9);
    }

    #[test]
    #[allow(clippy::identity_op)]
    fn test_keyed_fold_timestamp() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Timestamped((0, 0), Duration::from_secs(1)));
        fake_operator.push(StreamElement::Timestamped((1, 1), Duration::from_secs(2)));
        fake_operator.push(StreamElement::Timestamped((0, 2), Duration::from_secs(3)));
        fake_operator.push(StreamElement::Watermark(Duration::from_secs(4)));

        let mut keyed_fold = KeyedFold::new(fake_operator, 0, |a, b| *a += b);

        let mut res = vec![];
        for _ in 0..2 {
            let item = keyed_fold.next();
            match item {
                StreamElement::Timestamped(x, ts) => res.push((x, ts)),
                other => panic!(
                    "Expecting StreamElement::Timestamped, got {}",
                    other.variant()
                ),
            }
        }

        assert_eq!(
            keyed_fold.next(),
            StreamElement::Watermark(Duration::from_secs(4))
        );
        assert_eq!(keyed_fold.next(), StreamElement::Terminate);

        res.sort_unstable();
        assert_eq!(res[0].0 .1, 0 + 2);
        assert_eq!(res[0].1, Duration::from_secs(3));
        assert_eq!(res[1].0 .1, 1);
        assert_eq!(res[1].1, Duration::from_secs(2));
    }

    #[test]
    #[allow(clippy::identity_op)]
    fn test_keyed_fold_end_iter() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Item((0, 0)));
        fake_operator.push(StreamElement::Item((0, 2)));
        fake_operator.push(StreamElement::FlushAndRestart);
        fake_operator.push(StreamElement::Item((1, 1)));
        fake_operator.push(StreamElement::Item((1, 3)));
        fake_operator.push(StreamElement::FlushAndRestart);

        let mut keyed_fold = KeyedFold::new(fake_operator, 0, |a, b| *a += b);

        assert_eq!(keyed_fold.next(), StreamElement::Item((0, 0 + 2)));
        assert_eq!(keyed_fold.next(), StreamElement::FlushAndRestart);
        assert_eq!(keyed_fold.next(), StreamElement::Item((1, 1 + 3)));
        assert_eq!(keyed_fold.next(), StreamElement::FlushAndRestart);
        assert_eq!(keyed_fold.next(), StreamElement::Terminate);
    }
}
