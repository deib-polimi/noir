use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyValue, KeyedStream, Stream};

use super::AsyncOperator;

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Make this [`KeyedStream`] a normal [`Stream`] of key-value pairs.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let stream = env.stream(IteratorSource::new((0..4))).group_by(|&n| n % 2);
    /// let res = stream.unkey().collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable(); // the output order is nondeterministic
    /// assert_eq!(res, vec![(0, 0), (0, 2), (1, 1), (1, 3)]);
    /// ```
    pub fn unkey(self) -> Stream<KeyValue<Key, Out>, impl Operator<KeyValue<Key, Out>>> {
        self.0
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: AsyncOperator<KeyValue<Key, Out>> + Unpin + 'static,
{
    /// Make this [`KeyedStream`] a normal [`Stream`] of key-value pairs.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let stream = env.stream(IteratorSource::new((0..4))).group_by(|&n| n % 2);
    /// let res = stream.unkey().collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable(); // the output order is nondeterministic
    /// assert_eq!(res, vec![(0, 0), (0, 2), (1, 1), (1, 3)]);
    /// ```
    pub fn unkey_async(self) -> Stream<KeyValue<Key, Out>, impl AsyncOperator<KeyValue<Key, Out>>> {
        self.0
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Forget about the key of this [`KeyedStream`] and return a [`Stream`] containing just the
    /// values.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let stream = env.stream(IteratorSource::new((0..4))).group_by(|&n| n % 2);
    /// let res = stream.drop_key().collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable(); // the output order is nondeterministic
    /// assert_eq!(res, (0..4).collect::<Vec<_>>());
    /// ```
    pub fn drop_key(self) -> Stream<Out, impl Operator<Out>> {
        self.0.map(|(_k, v)| v)
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: AsyncOperator<KeyValue<Key, Out>> + 'static,
{
    /// Forget about the key of this [`KeyedStream`] and return a [`Stream`] containing just the
    /// values.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let stream = env.stream(IteratorSource::new((0..4))).group_by(|&n| n % 2);
    /// let res = stream.drop_key().collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable(); // the output order is nondeterministic
    /// assert_eq!(res, (0..4).collect::<Vec<_>>());
    /// ```
    pub fn drop_key_async(self) -> Stream<Out, impl AsyncOperator<Out>> {
        self.0.map_async(|(_k, v)| v)
    }
}
