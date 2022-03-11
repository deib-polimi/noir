use std::marker::PhantomData;
use std::task::Poll;

use futures::{ready, StreamExt};

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Data, DataKey, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, Stream};

use super::AsyncOperator;

#[pin_project::pin_project]
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Map<Out: Data, NewOut: Data, F, PreviousOperators>
where
    F: Fn(Out) -> NewOut + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    #[pin]
    prev: PreviousOperators,
    #[derivative(Debug = "ignore")]
    f: F,
    _out: PhantomData<Out>,
    _new_out: PhantomData<NewOut>,
}

impl<Out: Data, NewOut: Data, F, PreviousOperators> Map<Out, NewOut, F, PreviousOperators>
where
    F: Fn(Out) -> NewOut + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    fn new(prev: PreviousOperators, f: F) -> Self {
        Self {
            prev,
            f,
            _out: Default::default(),
            _new_out: Default::default(),
        }
    }
}

impl<Out: Data, NewOut: Data, F, PreviousOperators> Operator<NewOut>
    for Map<Out, NewOut, F, PreviousOperators>
where
    F: Fn(Out) -> NewOut + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<NewOut> {
        self.prev.next().map(&self.f)
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> Map<{} -> {}>",
            self.prev.to_string(),
            std::any::type_name::<Out>(),
            std::any::type_name::<NewOut>()
        )
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<NewOut, _>("Map"))
    }
}


impl<Out: Data, NewOut: Data, F, PreviousOperators> futures::Stream
    for Map<Out, NewOut, F, PreviousOperators>
where
    F: Fn(Out) -> NewOut + Send + Clone,
    PreviousOperators: AsyncOperator<Out>,
{
    type Item = StreamElement<NewOut>;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();
        let prev = ready!(this.prev.poll_next(cx));
        Poll::Ready(prev.map(|e| e.map(this.f)))
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Map the elements of the stream into new elements.
    ///
    /// **Note**: this is very similar to [`Iteartor::map`](std::iter::Iterator::map).
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s.map(|n| n * 10).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0, 10, 20, 30, 40]);
    /// ```
    pub fn map<NewOut: Data, F>(self, f: F) -> Stream<NewOut, impl Operator<NewOut>>
    where
        F: Fn(Out) -> NewOut + Send + Clone + 'static,
    {
        self.add_operator(|prev| Map::new(prev, f))
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: AsyncOperator<Out> + 'static,
{
    /// Map the elements of the stream into new elements.
    ///
    /// **Note**: this is very similar to [`Iteartor::map`](std::iter::Iterator::map).
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s.map(|n| n * 10).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0, 10, 20, 30, 40]);
    /// ```
    pub fn map_async<NewOut: Data, F>(self, f: F) -> Stream<NewOut, impl AsyncOperator<NewOut>>
    where
        F: Fn(Out) -> NewOut + Send + Clone + 'static,
    {
        self.add_async_operator(|prev| Map::new(prev, f))
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Map the elements of the stream into new elements.
    ///
    /// **Note**: this is very similar to [`Iteartor::map`](std::iter::Iterator::map).
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5))).group_by(|&n| n % 2);
    /// let res = s.map(|(_key, n)| 10 * n).collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0), (0, 20), (0, 40), (1, 10), (1, 30)]);
    /// ```
    pub fn map<NewOut: Data, F>(
        self,
        f: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        F: Fn(KeyValue<&Key, Out>) -> NewOut + Send + Clone + 'static,
    {
        self.add_operator(|prev| {
            Map::new(prev, move |(k, v)| {
                let mapped_value = f((&k, v));
                (k, mapped_value)
            })
        })
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: AsyncOperator<KeyValue<Key, Out>> + 'static,
{
    /// Map the elements of the stream into new elements.
    ///
    /// **Note**: this is very similar to [`Iteartor::map`](std::iter::Iterator::map).
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5))).group_by(|&n| n % 2);
    /// let res = s.map(|(_key, n)| 10 * n).collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0), (0, 20), (0, 40), (1, 10), (1, 30)]);
    /// ```
    pub fn map_async<NewOut: Data, F>(
        self,
        f: F,
    ) -> KeyedStream<Key, NewOut, impl AsyncOperator<KeyValue<Key, NewOut>>>
    where
        F: Fn(KeyValue<&Key, Out>) -> NewOut + Send + Clone + 'static,
    {
        self.add_async_operator(|prev| {
            Map::new(prev, move |(k, v)| {
                let mapped_value = f((&k, v));
                (k, mapped_value)
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::time::Duration;

    use crate::operator::map::Map;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    fn map_stream() {
        let mut fake_operator = FakeOperator::new(0..10u8);
        for i in 0..10 {
            fake_operator.push(StreamElement::Timestamped(i, Duration::from_secs(i as u64)));
        }
        fake_operator.push(StreamElement::Watermark(Duration::from_secs(100)));

        let map = Map::new(fake_operator, |x| x.to_string());
        let map = Map::new(map, |x| x + "000");
        let mut map = Map::new(map, |x| u32::from_str(&x).unwrap());

        for i in 0..10 {
            let elem = map.next();
            assert_eq!(elem, StreamElement::Item(i * 1000));
        }
        for i in 0..10 {
            let elem = map.next();
            assert_eq!(
                elem,
                StreamElement::Timestamped(i * 1000, Duration::from_secs(i as u64))
            );
        }
        assert_eq!(
            map.next(),
            StreamElement::Watermark(Duration::from_secs(100))
        );
        assert_eq!(map.next(), StreamElement::Terminate);
    }
}
