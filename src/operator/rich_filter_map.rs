use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyValue, KeyedStream, Stream};

use super::AsyncOperator;

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Remove from the stream all the elements for which the provided function returns `None` and
    /// keep the elements that returned `Some(_)`. The mapping function can be stateful.
    ///
    /// This is equivalent to [`Stream::filter_map`] but with a stateful function.
    ///
    /// Since the mapping function can be stateful, it is a `FnMut`. This allows expressing simple
    /// algorithms with very few lines of code (see examples).
    ///
    /// The mapping function is _cloned_ inside each replica, and they will not share state between
    /// each other. If you want that only a single replica handles all the items you may want to
    /// change the parallelism of this operator with [`Stream::max_parallelism`].
    ///
    /// ## Examples
    ///
    /// This will emit only the _positive prefix-sums_.
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new(std::array::IntoIter::new([1, 2, -5, 3, 1])));
    /// let res = s.rich_filter_map({
    ///     let mut sum = 0;
    ///     move |x| {
    ///         sum += x;
    ///         if sum >= 0 {
    ///             Some(sum)
    ///         } else {
    ///             None
    ///         }
    ///     }
    /// }).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![1, 1 + 2, /* 1 + 2 - 5, */ 1 + 2 - 5 + 3, 1 + 2 - 5 + 3 + 1]);
    /// ```
    pub fn rich_filter_map<NewOut: Data, F>(self, f: F) -> Stream<NewOut, impl Operator<NewOut>>
    where
        F: FnMut(Out) -> Option<NewOut> + Send + Clone + 'static,
    {
        self.rich_map(f).filter(|x| x.is_some()).map(|x| x.unwrap())
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Remove from the stream all the elements for which the provided function returns `None` and
    /// keep the elements that returned `Some(_)`. The mapping function can be stateful.
    ///
    /// This is exactly like [`Stream::rich_filter_map`], but the function is cloned for each key.
    /// This means that each key will have a unique mapping function (and therefore a unique state).
    pub fn rich_filter_map<NewOut: Data, F>(
        self,
        f: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        F: FnMut(KeyValue<&Key, Out>) -> Option<NewOut> + Send + Clone + 'static,
    {
        self.rich_map(f)
            .filter(|(_, x)| x.is_some())
            .map(|(_, x)| x.unwrap())
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: AsyncOperator<KeyValue<Key, Out>> + 'static,
{
    /// Remove from the stream all the elements for which the provided function returns `None` and
    /// keep the elements that returned `Some(_)`. The mapping function can be stateful.
    ///
    /// This is exactly like [`Stream::rich_filter_map`], but the function is cloned for each key.
    /// This means that each key will have a unique mapping function (and therefore a unique state).
    pub fn rich_filter_map_async<NewOut: Data, F>(
        self,
        f: F,
    ) -> KeyedStream<Key, NewOut, impl AsyncOperator<KeyValue<Key, NewOut>>>
    where
        F: FnMut(KeyValue<&Key, Out>) -> Option<NewOut> + Send + Clone + 'static,
    {
        self.rich_map_async(f)
            .filter_async(|(_, x)| x.is_some())
            .map_async(|(_, x)| x.unwrap())
    }
}
