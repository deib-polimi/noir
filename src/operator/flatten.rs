use core::iter::{IntoIterator, Iterator};
use std::marker::PhantomData;
use std::time::Duration;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Data, DataKey, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, Stream};

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Flatten<In, Out, InnerIterator, PreviousOperators>
where
    PreviousOperators: Operator<In>,
    In: Data + IntoIterator<Item = Out>,
    Out: Data,
    InnerIterator: Iterator,
{
    prev: PreviousOperators,
    // used to store elements that have not been returned by next() yet
    // buffer: VecDeque<StreamElement<NewOut>>,
    // Make an element of type `Out` iterable
    // This is used to make `Flatten` behave differently when applied to `Stream` or `KeyedStream`
    // Takes `Out` as input, returns an `Iterator` with items of type `NewOut`
    #[derivative(Debug = "ignore")]
    frontiter: Option<InnerIterator>,
    timestamp: Option<Duration>,
    _out: PhantomData<In>,
    _iter_out: PhantomData<Out>,
}

impl<In, Out, InnerIterator, PreviousOperators> Flatten<In, Out, InnerIterator, PreviousOperators>
where
    PreviousOperators: Operator<In>,
    In: Data + IntoIterator<IntoIter = InnerIterator, Item = InnerIterator::Item>,
    Out: Data + Clone,
    InnerIterator: Iterator<Item = Out> + Clone + Send,
{
    fn new(prev: PreviousOperators) -> Self {
        Self {
            prev,
            frontiter: None,
            timestamp: None,
            _out: Default::default(),
            _iter_out: Default::default(),
        }
    }
}

impl<In, Out, InnerIterator, PreviousOperators> Operator<Out>
    for Flatten<In, Out, InnerIterator, PreviousOperators>
where
    PreviousOperators: Operator<In>,
    In: Data + IntoIterator<IntoIter = InnerIterator, Item = InnerIterator::Item>,
    Out: Data + Clone,
    InnerIterator: Iterator<Item = Out> + Clone + Send,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<Out> {
        loop {
            if let Some(ref mut inner) = self.frontiter {
                match inner.next() {
                    None => self.frontiter = None,
                    Some(item) => match self.timestamp {
                        None => return StreamElement::Item(item),
                        Some(ts) => return StreamElement::Timestamped(item, ts),
                    },
                }
            }
            match self.prev.next() {
                StreamElement::Item(inner) => {
                    self.frontiter = Some(inner.into_iter());
                    self.timestamp = None;
                }
                StreamElement::Timestamped(inner, ts) => {
                    self.frontiter = Some(inner.into_iter());
                    self.timestamp = Some(ts);
                }
                StreamElement::Watermark(ts) => return StreamElement::Watermark(ts),
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::Terminate => return StreamElement::Terminate,
                StreamElement::FlushAndRestart => return StreamElement::FlushAndRestart,
                StreamElement::Yield => return StreamElement::Yield, //TODO: Check
            }
        }
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> Flatten<{} -> {}>",
            self.prev.to_string(),
            std::any::type_name::<In>(),
            std::any::type_name::<Out>()
        )
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Out, _>("Flatten"))
    }
}

impl<In, Out, InnerIterator, OperatorChain> Stream<In, OperatorChain>
where
    OperatorChain: Operator<In> + 'static,
    InnerIterator: Iterator<Item = Out> + Clone + Send + 'static,
    In: Data + IntoIterator<IntoIter = InnerIterator, Item = InnerIterator::Item>,
    Out: Data + Clone,
{
    /// Transform this stream of containers into a stream of all the contained values.
    ///
    /// **Note**: this is very similar to [`Iteartor::flatten`](std::iter::Iterator::flatten)
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new(vec![
    ///     vec![1, 2, 3],
    ///     vec![],
    ///     vec![4, 5],
    /// ].into_iter()));
    /// let res = s.flatten().collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![1, 2, 3, 4, 5]);
    /// ```
    pub fn flatten(self) -> Stream<Out, impl Operator<Out>> {
        self.add_operator(|prev| Flatten::new(prev))
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Apply a mapping operation to each element of the stream, the resulting stream will be the
    /// flattened values of the result of the mapping.
    ///
    /// **Note**: this is very similar to [`Iteartor::flat_map`](std::iter::Iterator::flat_map)
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..3)));
    /// let res = s.flat_map(|n| vec![n, n]).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0, 0, 1, 1, 2, 2]);
    /// ```
    pub fn flat_map<MapOut: Data, NewOut: Data, F>(
        self,
        f: F,
    ) -> Stream<NewOut, impl Operator<NewOut>>
    where
        MapOut: IntoIterator<Item = NewOut>,
        <MapOut as IntoIterator>::IntoIter: Clone + Send + 'static,
        F: Fn(Out) -> MapOut + Send + Clone + 'static,
    {
        self.map(f).flatten()
    }
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct KeyedFlatten<Key, In, Out, InnerIterator, PreviousOperators>
where
    Key: DataKey,
    PreviousOperators: Operator<KeyValue<Key, In>>,
    In: Data + IntoIterator<Item = Out>,
    Out: Data,
    InnerIterator: Iterator,
{
    prev: PreviousOperators,
    // used to store elements that have not been returned by next() yet
    // buffer: VecDeque<StreamElement<NewOut>>,
    // Make an element of type `Out` iterable
    // This is used to make `Flatten` behave differently when applied to `Stream` or `KeyedStream`
    // Takes `Out` as input, returns an `Iterator` with items of type `NewOut`
    #[derivative(Debug = "ignore")]
    frontiter: Option<(Key, InnerIterator)>,
    timestamp: Option<Duration>,
    _key: PhantomData<Key>,
    _in: PhantomData<In>,
    _iter_out: PhantomData<Out>,
}

impl<Key, In, Out, InnerIterator, PreviousOperators>
    KeyedFlatten<Key, In, Out, InnerIterator, PreviousOperators>
where
    Key: DataKey,
    PreviousOperators: Operator<KeyValue<Key, In>>,
    In: Data + IntoIterator<IntoIter = InnerIterator, Item = InnerIterator::Item>,
    Out: Data + Clone,
    InnerIterator: Iterator<Item = Out> + Clone + Send,
{
    fn new(prev: PreviousOperators) -> Self {
        Self {
            prev,
            frontiter: None,
            timestamp: None,
            _key: Default::default(),
            _in: Default::default(),
            _iter_out: Default::default(),
        }
    }
}

impl<Key, In, Out, InnerIterator, PreviousOperators> Operator<KeyValue<Key, Out>>
    for KeyedFlatten<Key, In, Out, InnerIterator, PreviousOperators>
where
    Key: DataKey,
    PreviousOperators: Operator<KeyValue<Key, In>>,
    In: Data + IntoIterator<IntoIter = InnerIterator, Item = InnerIterator::Item>,
    Out: Data + Clone,
    InnerIterator: Iterator<Item = Out> + Clone + Send,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<KeyValue<Key, Out>> {
        loop {
            if let Some((ref key, ref mut inner)) = self.frontiter {
                match inner.next() {
                    None => self.frontiter = None,
                    Some(item) => match self.timestamp {
                        None => return StreamElement::Item((key.clone(), item)),
                        Some(ts) => return StreamElement::Timestamped((key.clone(), item), ts),
                    },
                }
            }
            match self.prev.next() {
                StreamElement::Item((key, inner)) => {
                    self.frontiter = Some((key, inner.into_iter()));
                    self.timestamp = None;
                }
                StreamElement::Timestamped((key, inner), ts) => {
                    self.frontiter = Some((key, inner.into_iter()));
                    self.timestamp = Some(ts);
                }
                StreamElement::Watermark(ts) => return StreamElement::Watermark(ts),
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::Terminate => return StreamElement::Terminate,
                StreamElement::FlushAndRestart => return StreamElement::FlushAndRestart,
                StreamElement::Yield => return StreamElement::Yield, //TODO: Check
            }
        }
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> KeyedFlatten<{} -> {}>",
            self.prev.to_string(),
            std::any::type_name::<In>(),
            std::any::type_name::<Out>()
        )
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Out, _>("KeyedFlatten"))
    }
}

impl<Key: DataKey, In, Out, InnerIterator, OperatorChain> KeyedStream<Key, In, OperatorChain>
where
    Key: DataKey,
    OperatorChain: Operator<KeyValue<Key, In>> + 'static,
    InnerIterator: Iterator<Item = Out> + Clone + Send + 'static,
    In: Data + IntoIterator<IntoIter = InnerIterator, Item = InnerIterator::Item>,
    Out: Data + Clone,
{
    /// Transform this stream of containers into a stream of all the contained values.
    ///
    /// **Note**: this is very similar to [`Iteartor::flatten`](std::iter::Iterator::flatten)
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env
    ///     .stream(IteratorSource::new(vec![
    ///         vec![0, 1, 2],
    ///         vec![3, 4, 5],
    ///         vec![6, 7]
    ///     ].into_iter()))
    ///     .group_by(|v| v[0] % 2);
    /// let res = s.flatten().collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0), (0, 1), (0, 2), (0, 6), (0, 7), (1, 3), (1, 4), (1, 5)]);
    /// ```
    pub fn flatten(self) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>> {
        self.add_operator(|prev| KeyedFlatten::new(prev))
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Apply a mapping operation to each element of the stream, the resulting stream will be the
    /// flattened values of the result of the mapping.
    ///
    /// **Note**: this is very similar to [`Iteartor::flat_map`](std::iter::Iterator::flat_map).
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..3))).group_by(|&n| n % 2);
    /// let res = s.flat_map(|(_key, n)| vec![n, n]).collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0), (0, 0), (0, 2), (0, 2), (1, 1), (1, 1)]);
    /// ```
    pub fn flat_map<NewOut: Data, MapOut: Data, F>(
        self,
        f: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        MapOut: IntoIterator<Item = NewOut>,
        <MapOut as IntoIterator>::IntoIter: Clone + Send + 'static,
        F: Fn(KeyValue<&Key, Out>) -> MapOut + Send + Clone + 'static,
    {
        self.map(f).flatten()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::operator::flatten::Flatten;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    fn test_flatten_no_timestamps() {
        let fake_operator = FakeOperator::new(
            vec![
                vec![],
                vec![0, 1, 2, 3],
                vec![],
                vec![4],
                vec![5, 6, 7],
                vec![],
            ]
            .into_iter(),
        );
        let mut flatten = Flatten::new(fake_operator);
        for i in 0..=7 {
            assert_eq!(flatten.next(), StreamElement::Item(i));
        }
        assert_eq!(flatten.next(), StreamElement::Terminate);
    }

    #[test]
    fn test_flatten_timestamped() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Timestamped(vec![], Duration::from_secs(0)));
        fake_operator.push(StreamElement::Timestamped(
            vec![1, 2, 3],
            Duration::from_secs(1),
        ));
        fake_operator.push(StreamElement::Timestamped(vec![4], Duration::from_secs(2)));
        fake_operator.push(StreamElement::Timestamped(vec![], Duration::from_secs(3)));
        fake_operator.push(StreamElement::Watermark(Duration::from_secs(4)));

        let mut flatten = Flatten::new(fake_operator);

        assert_eq!(
            flatten.next(),
            StreamElement::Timestamped(1, Duration::from_secs(1))
        );
        assert_eq!(
            flatten.next(),
            StreamElement::Timestamped(2, Duration::from_secs(1))
        );
        assert_eq!(
            flatten.next(),
            StreamElement::Timestamped(3, Duration::from_secs(1))
        );
        assert_eq!(
            flatten.next(),
            StreamElement::Timestamped(4, Duration::from_secs(2))
        );
        assert_eq!(
            flatten.next(),
            StreamElement::Watermark(Duration::from_secs(4))
        );
        assert_eq!(flatten.next(), StreamElement::Terminate);
    }
}
