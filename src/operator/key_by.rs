use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Data, DataKey};
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, Stream};

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct KeyBy<Key: DataKey, Out: Data, Keyer, OperatorChain>
where
    Keyer: Fn(&Out) -> Key + Send + Clone,
    OperatorChain: Operator<Out>,
{
    prev: OperatorChain,
    #[derivative(Debug = "ignore")]
    keyer: Keyer,
    _key: PhantomData<Key>,
    _out: PhantomData<Out>,
}

impl<Key: DataKey, Out: Data, Keyer, OperatorChain> KeyBy<Key, Out, Keyer, OperatorChain>
where
    Keyer: Fn(&Out) -> Key + Send + Clone,
    OperatorChain: Operator<Out>,
{
    pub fn new(prev: OperatorChain, keyer: Keyer) -> Self {
        Self {
            prev,
            keyer,
            _key: Default::default(),
            _out: Default::default(),
        }
    }
}

impl<Key: DataKey, Out: Data, Keyer, OperatorChain> Operator<KeyValue<Key, Out>>
    for KeyBy<Key, Out, Keyer, OperatorChain>
where
    Keyer: Fn(&Out) -> Key + Send + Clone,
    OperatorChain: Operator<Out>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<KeyValue<Key, Out>> {
        match self.prev.next() {
            StreamElement::Item(t) => StreamElement::Item(((self.keyer)(&t), t)),
            StreamElement::Timestamped(t, ts) => {
                StreamElement::Timestamped(((self.keyer)(&t), t), ts)
            }
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::Terminate => StreamElement::Terminate,
            StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
            StreamElement::FlushBatch => StreamElement::FlushBatch,
        }
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> KeyBy<{}>",
            self.prev.to_string(),
            std::any::type_name::<Key>(),
        )
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<KeyValue<Key, Out>, _>("KeyBy"))
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    pub fn key_by<Key: DataKey, Keyer>(
        self,
        keyer: Keyer,
    ) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>>
    where
        Keyer: Fn(&Out) -> Key + Send + Clone + 'static,
    {
        KeyedStream(self.add_operator(|prev| KeyBy::new(prev, keyer)))
    }
}

#[cfg(test)]
mod tests {
    use crate::operator::{KeyBy, Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    fn test_key_by() {
        let fake_operator = FakeOperator::new(0..10u8);
        let mut key_by = KeyBy::new(fake_operator, |&n| n);

        for i in 0..10u8 {
            match key_by.next() {
                StreamElement::Item((a, b)) => {
                    assert_eq!(a, i);
                    assert_eq!(b, i);
                }
                item => panic!("Expected StreamElement::Item, got {}", item.variant()),
            }
        }
        assert_eq!(key_by.next(), StreamElement::Terminate);
    }
}
