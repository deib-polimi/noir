use super::super::*;
use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyValue, KeyedStream, WindowedStream};

#[derive(Clone)]
pub(crate) struct Count<T>(usize, PhantomData<T>);

impl<T: Data> WindowAccumulator for Count<T> {
    type In = T;
    type Out = usize;

    fn process(&mut self, _: Self::In) {
        self.0 += 1;
    }

    fn output(self) -> Self::Out {
        self.0
    }
}

impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowBuilder,
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
    Key: DataKey,
    Out: Data,
{
    pub fn count(self) -> KeyedStream<Key, usize, impl Operator<KeyValue<Key, usize>>> {
        let acc = Count(0, PhantomData);
        self.add_window_operator("WindowCount", acc)
    }
}