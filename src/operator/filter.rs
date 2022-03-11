use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Data, DataKey, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, Stream};

use super::AsyncOperator;

#[pin_project::pin_project]
#[derive(Clone)]
struct Filter<Out: Data, PreviousOperator, Predicate>
where
    Predicate: Fn(&Out) -> bool + Send + Clone + 'static,
    PreviousOperator: Operator<Out> + 'static,
{
    #[pin]
    prev: PreviousOperator,
    predicate: Predicate,
    _out: PhantomData<Out>,
}

impl<Out: Data, PreviousOperator, Predicate> Filter<Out, PreviousOperator, Predicate>
where
    Predicate: Fn(&Out) -> bool + Clone + Send + 'static,
    PreviousOperator: Operator<Out> + 'static,
{
    fn new(prev: PreviousOperator, predicate: Predicate) -> Self {
        Self {
            prev,
            predicate,
            _out: Default::default(),
        }
    }
}

impl<Out: Data, PreviousOperator, Predicate> Operator<Out>
    for Filter<Out, PreviousOperator, Predicate>
where
    Predicate: Fn(&Out) -> bool + Clone + Send + 'static,
    PreviousOperator: Operator<Out> + 'static,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<Out> {
        loop {
            match self.prev.next() {
                StreamElement::Item(ref item) | StreamElement::Timestamped(ref item, _)
                    if !(self.predicate)(item) => {}
                element => return element,
            }
        }
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> Filter<{}>",
            self.prev.to_string(),
            std::any::type_name::<Out>()
        )
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Out, _>("Filter"))
    }
}


impl<Out: Data, PreviousOperator, Predicate> futures::Stream
    for Filter<Out, PreviousOperator, Predicate>
where
    Predicate: Fn(&Out) -> bool + Clone + Send + 'static,
    PreviousOperator: AsyncOperator<Out> + 'static,
{
    type Item = StreamElement<Out>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut proj = self.project();
        loop {
            match ready!(proj.prev.as_mut().poll_next(cx)).unwrap_or_else(|| StreamElement::Terminate) {
                StreamElement::Item(ref item) | StreamElement::Timestamped(ref item, _)
                    if !(proj.predicate)(item) => {}
                element => return Poll::Ready(Some(element)),
            }
        }
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Remove from the stream all the elements for which the provided predicate returns `false`.
    ///
    /// **Note**: this is very similar to [`Iteartor::filter`](std::iter::Iterator::filter)
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..10)));
    /// let res = s.filter(|&n| n % 2 == 0).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0, 2, 4, 6, 8])
    /// ```
    pub fn filter<Predicate>(self, predicate: Predicate) -> Stream<Out, impl Operator<Out>>
    where
        Predicate: Fn(&Out) -> bool + Clone + Send + 'static,
    {
        self.add_operator(|prev| Filter::new(prev, predicate))
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: AsyncOperator<Out> + 'static,
{
    /// Remove from the stream all the elements for which the provided predicate returns `false`.
    ///
    /// **Note**: this is very similar to [`Iteartor::filter`](std::iter::Iterator::filter)
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..10)));
    /// let res = s.filter(|&n| n % 2 == 0).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0, 2, 4, 6, 8])
    /// ```
    pub fn filter_async<Predicate>(self, predicate: Predicate) -> Stream<Out, impl Operator<Out>>
    where
        Predicate: Fn(&Out) -> bool + Clone + Send + 'static,
    {
        self.add_async_operator(|prev| Filter::new(prev, predicate))
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Remove from the stream all the elements for which the provided predicate returns `false`.
    ///
    /// **Note**: this is very similar to [`Iteartor::filter`](std::iter::Iterator::filter)
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..10))).group_by(|&n| n % 2);
    /// let res = s.filter(|&(_key, n)| n % 3 == 0).collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0), (0, 6), (1, 3), (1, 9)]);
    /// ```
    pub fn filter<Predicate>(
        self,
        predicate: Predicate,
    ) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>>
    where
        Predicate: Fn(&KeyValue<Key, Out>) -> bool + Clone + Send + 'static,
    {
        self.add_operator(|prev| Filter::new(prev, predicate))
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: AsyncOperator<KeyValue<Key, Out>> + 'static,
{
    /// Remove from the stream all the elements for which the provided predicate returns `false`.
    ///
    /// **Note**: this is very similar to [`Iteartor::filter`](std::iter::Iterator::filter)
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..10))).group_by(|&n| n % 2);
    /// let res = s.filter(|&(_key, n)| n % 3 == 0).collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0), (0, 6), (1, 3), (1, 9)]);
    /// ```
    pub fn filter_async<Predicate>(
        self,
        predicate: Predicate,
    ) -> KeyedStream<Key, Out, impl AsyncOperator<KeyValue<Key, Out>>>
    where
        Predicate: Fn(&KeyValue<Key, Out>) -> bool + Clone + Send + 'static,
    {
        self.add_async_operator(|prev| Filter::new(prev, predicate))
    }
}

#[cfg(test)]
mod tests {
    use crate::operator::filter::Filter;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    fn test_filter() {
        let fake_operator = FakeOperator::new(0..10u8);
        let mut filter = Filter::new(fake_operator, |n| n % 2 == 0);

        assert_eq!(filter.next(), StreamElement::Item(0));
        assert_eq!(filter.next(), StreamElement::Item(2));
        assert_eq!(filter.next(), StreamElement::Item(4));
        assert_eq!(filter.next(), StreamElement::Item(6));
        assert_eq!(filter.next(), StreamElement::Item(8));
        assert_eq!(filter.next(), StreamElement::Terminate);
    }
}
