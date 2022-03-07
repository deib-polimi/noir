use crate::block::NextStrategy;
use crate::operator::end::EndBlock;
use crate::operator::key_by::KeyBy;
use crate::operator::{DataKey, ExchangeData, Operator};
use crate::stream::{KeyValue, KeyedStream, Stream};

use super::AsyncOperator;

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Given a stream, make a [`KeyedStream`] partitioning the values according to a key generated
    /// by the `keyer` function provided.
    ///
    /// The returned [`KeyedStream`] is partitioned by key, and all the operators added to it will
    /// be evaluated _after_ the network shuffle. Therefore all the items are sent to the network
    /// (if their destination is not the local host). In many cases this behaviour can be avoided by
    /// using the associative variant of the operators (e.g. [`Stream::group_by_reduce`],
    /// [`Stream::group_by_sum`], ...).
    ///
    /// **Note**: the keys are not sent to the network, they are built on the sending side, and
    /// rebuilt on the receiving side.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let keyed = s.group_by(|&n| n % 2); // partition even and odd elements
    /// ```
    pub fn group_by<Key: DataKey, Keyer>(
        self,
        keyer: Keyer,
    ) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>>
    where
        Keyer: Fn(&Out) -> Key + Send + Clone + 'static,
    {
        let next_strategy = NextStrategy::group_by(keyer.clone());
        let new_stream = self
            .add_block(EndBlock::new, next_strategy)
            .add_operator(|prev| KeyBy::new(prev, keyer));
        KeyedStream(new_stream)
    }

}

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: AsyncOperator<Out> + 'static,
{
    pub fn group_by_async<Key: DataKey, Keyer>(
        self,
        keyer: Keyer,
    ) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>>
    where
        Keyer: Fn(&Out) -> Key + Send + Clone + 'static,
    {
        let next_strategy = NextStrategy::group_by(keyer.clone());
        let new_stream = self
            .add_async_block(EndBlock::new, next_strategy)
            .add_async_operator(|prev| KeyBy::new(prev, keyer));
        KeyedStream(new_stream)
    }
}
