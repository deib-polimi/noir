use crate::operator::window::generic_operator::GenericWindowOperator;
use crate::operator::{Data, DataKey, Operator, WindowDescription};
use crate::stream::{KeyValue, KeyedStream};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub enum JoinElement<A, B> {
    Left(A),
    Right(B),
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub fn window_join<Out2: Data, OperatorChain2, WindowDescr>(
        self,
        right: KeyedStream<Key, Out2, OperatorChain2>,
        descr: WindowDescr,
    ) -> KeyedStream<Key, (Out, Out2), impl Operator<KeyValue<Key, (Out, Out2)>>>
    where
        WindowDescr: WindowDescription<Key, JoinElement<Out, Out2>> + Clone + 'static,
        OperatorChain2: Operator<KeyValue<Key, Out2>> + Send + 'static,
    {
        // map the left and right streams to the same type
        let left = self.map(|(_, x)| JoinElement::<_, Out2>::Left(x));
        let right = right.map(|(_, x)| JoinElement::<Out, _>::Right(x));

        // concatenate the two streams and apply the window
        let windowed_stream = left.concat(right).window(descr);
        let stream = windowed_stream.inner;
        let descr = windowed_stream.descr;

        stream
            .add_operator(|prev| {
                GenericWindowOperator::new("Join", prev, descr, move |window| {
                    // divide the elements coming from the left stream from the elements
                    // coming from the right stream
                    let (left, right) = window
                        .items()
                        .partition::<Vec<_>, _>(|x| matches!(x, JoinElement::Left(_)));

                    // calculate all the pairs of elements in the current window
                    let mut res = Vec::new();
                    for l in left.into_iter() {
                        for r in right.iter() {
                            match (l, r) {
                                (JoinElement::Left(l), JoinElement::Right(r)) => {
                                    res.push((l.clone(), r.clone()))
                                }
                                _ => {
                                    unreachable!("Items of left and right streams are partitioned")
                                }
                            }
                        }
                    }
                    res
                })
            })
            .flatten()
    }
}