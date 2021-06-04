#![allow(clippy::type_complexity)]

use std::marker::PhantomData;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::join::ship::{ShipBroadcastRight, ShipHash, ShipStrategy};
use crate::operator::join::start::{JoinElement, JoinStartBlock};
use crate::operator::{
    Data, ExchangeData, InnerJoinTuple, JoinVariant, KeyerFn, LeftJoinTuple, Operator,
    OuterJoinTuple, StreamElement,
};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, Stream};
use std::collections::VecDeque;

/// This operator performs the join using the local sort-merge strategy.
///
/// The previous operator should be a `JoinStartBlock` that emits the `JoinElement` of the two
/// incoming streams.
///
/// This operator is able to produce the outer join tuples (the most general type of join), but it
/// can be asked to skip generating the `None` tuples if the join was actually inner.
#[derive(Clone, Debug)]
struct JoinLocalSortMerge<
    Key: Data + Ord,
    Out1: ExchangeData,
    Out2: ExchangeData,
    OperatorChain: Operator<JoinElement<Key, Out1, Out2>>,
> {
    prev: OperatorChain,
    /// Whether the left side has ended.
    left_ended: bool,
    /// Whether the right side has ended.
    right_ended: bool,
    /// Elements of the left side.
    left: Vec<KeyValue<Key, Out1>>,
    /// Elements of the right side.
    right: Vec<KeyValue<Key, Out2>>,
    /// Buffer with elements ready to be sent downstream.
    buffer: VecDeque<KeyValue<Key, OuterJoinTuple<Out1, Out2>>>,
    /// Join variant.
    variant: JoinVariant,
    /// The last key of the last element processed by `advance()` coming from the left side.
    /// This is used to check whether an element of the right side was matched with an element
    /// of the left side or not.
    last_left_key: Option<Key>,
}
impl<
        Key: Data + Ord,
        Out1: ExchangeData,
        Out2: ExchangeData,
        OperatorChain: Operator<JoinElement<Key, Out1, Out2>>,
    > JoinLocalSortMerge<Key, Out1, Out2, OperatorChain>
{
    fn new(prev: OperatorChain, variant: JoinVariant) -> Self {
        Self {
            prev,
            left_ended: false,
            right_ended: false,
            left: Default::default(),
            right: Default::default(),
            buffer: Default::default(),
            variant,
            last_left_key: None,
        }
    }

    /// Discard the last element in the buffer containing elements of the right side.
    /// If needed, generate the right-outer join tuple.
    fn discard_right(&mut self) {
        let (rkey, rvalue) = self.right.pop().unwrap();

        // check if the element has been matched with at least one left-side element
        let matched = matches!(&self.last_left_key, Some(lkey) if lkey == &rkey);

        if !matched && self.variant.right_outer() {
            self.buffer.push_back((rkey, (None, Some(rvalue))));
        }
    }

    /// Generate some join tuples. Since the number of join tuples can be quite high,
    /// this is used to generate the tuples incrementally, so that the memory usage is lower.
    fn advance(&mut self) {
        while self.buffer.is_empty() && (!self.left.is_empty() || !self.right.is_empty()) {
            // try matching one element of the left side with some elements of the right side
            if let Some((lkey, lvalue)) = self.left.pop() {
                // discard the elements of the right side with key bigger than the key of
                // the element of the left side
                let discarded = self
                    .right
                    .iter()
                    .rev()
                    .take_while(|(rkey, _)| rkey > &lkey)
                    .count();
                for _ in 0..discarded {
                    self.discard_right();
                }

                // check if there is at least one element matching in the right side
                let has_matches = matches!(self.right.last(), Some((rkey, _)) if rkey == &lkey);

                if has_matches {
                    let matches = self
                        .right
                        .iter()
                        .rev()
                        .take_while(|(rkey, _)| &lkey == rkey)
                        .map(|(_, rvalue)| {
                            (lkey.clone(), (Some(lvalue.clone()), Some(rvalue.clone())))
                        });
                    self.buffer.extend(matches);
                } else if self.variant.left_outer() {
                    self.buffer.push_back((lkey.clone(), (Some(lvalue), None)));
                }

                // set this key as the last key processed
                self.last_left_key = Some(lkey);
            } else {
                // there are no elements left in the left side,
                // so discard what is remaining in the right side
                while !self.right.is_empty() {
                    self.discard_right();
                }
            }
        }
    }
}

impl<
        Key: Data + Ord,
        Out1: ExchangeData,
        Out2: ExchangeData,
        OperatorChain: Operator<JoinElement<Key, Out1, Out2>>,
    > Operator<KeyValue<Key, OuterJoinTuple<Out1, Out2>>>
    for JoinLocalSortMerge<Key, Out1, Out2, OperatorChain>
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<(Key, (Option<Out1>, Option<Out2>))> {
        loop {
            if self.buffer.is_empty() && self.left_ended && self.right_ended {
                // try to generate some join tuples
                self.advance();
            }

            if let Some(item) = self.buffer.pop_front() {
                return StreamElement::Item(item);
            }

            match self.prev.next() {
                StreamElement::Item(JoinElement::Left(item)) => {
                    self.left.push(item);
                }
                StreamElement::Item(JoinElement::Right(item)) => {
                    self.right.push(item);
                }
                StreamElement::Item(JoinElement::LeftEnd) => {
                    self.left_ended = true;
                    self.left.sort_unstable_by(|(k1, _), (k2, _)| k1.cmp(k2));
                }
                StreamElement::Item(JoinElement::RightEnd) => {
                    self.right_ended = true;
                    self.right.sort_unstable_by(|(k1, _), (k2, _)| k1.cmp(k2));
                }
                StreamElement::Timestamped(_, _) | StreamElement::Watermark(_) => {
                    panic!("Cannot join timestamp streams")
                }
                StreamElement::FlushAndRestart => {
                    assert!(self.left_ended);
                    assert!(self.right_ended);
                    assert!(self.left.is_empty());
                    assert!(self.right.is_empty());

                    // reset the state of the operator
                    self.left_ended = false;
                    self.right_ended = false;
                    self.last_left_key = None;

                    return StreamElement::FlushAndRestart;
                }
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::Terminate => return StreamElement::Terminate,
            }
        }
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> JoinLocalSortMerge<{}>",
            self.prev.to_string(),
            std::any::type_name::<Key>()
        )
    }

    fn structure(&self) -> BlockStructure {
        self.prev.structure().add_operator(OperatorStructure::new::<
            KeyValue<Key, OuterJoinTuple<Out1, Out2>>,
            _,
        >("JoinLocalSortMerge"))
    }
}

/// This is an intermediate type for building a join operator.
///
/// The ship strategy has already been selected and it's stored in `ShipStrat`, the local strategy
/// is hash and now the join variant has to be selected.
///
/// Note that `outer` join is not supported if the ship strategy is `broadcast_right`.
pub struct JoinStreamLocalSortMerge<
    Key: Data + Ord,
    Out1: ExchangeData,
    Out2: ExchangeData,
    Keyer1: KeyerFn<Key, Out1>,
    Keyer2: KeyerFn<Key, Out2>,
    ShipStrat: ShipStrategy,
> {
    stream: Stream<JoinElement<Key, Out1, Out2>, JoinStartBlock<Key, Out1, Out2, Keyer1, Keyer2>>,
    _s: PhantomData<ShipStrat>,
}

impl<Key: Data + Ord, Out1: ExchangeData, Out2: ExchangeData, Keyer1, Keyer2, ShipStrat>
    JoinStreamLocalSortMerge<Key, Out1, Out2, Keyer1, Keyer2, ShipStrat>
where
    Keyer1: KeyerFn<Key, Out1>,
    Keyer2: KeyerFn<Key, Out2>,
    ShipStrat: ShipStrategy,
{
    pub(crate) fn new(
        stream: Stream<
            JoinElement<Key, Out1, Out2>,
            JoinStartBlock<Key, Out1, Out2, Keyer1, Keyer2>,
        >,
    ) -> Self {
        Self {
            stream,
            _s: Default::default(),
        }
    }
}

impl<Key: Data + Ord, Out1: ExchangeData, Out2: ExchangeData, Keyer1, Keyer2>
    JoinStreamLocalSortMerge<Key, Out1, Out2, Keyer1, Keyer2, ShipHash>
where
    Keyer1: KeyerFn<Key, Out1>,
    Keyer2: KeyerFn<Key, Out2>,
{
    pub fn inner(
        self,
    ) -> KeyedStream<
        Key,
        InnerJoinTuple<Out1, Out2>,
        impl Operator<KeyValue<Key, InnerJoinTuple<Out1, Out2>>>,
    > {
        let inner = self
            .stream
            .add_operator(|prev| JoinLocalSortMerge::new(prev, JoinVariant::Inner));
        KeyedStream(inner.map(|(key, (lhs, rhs))| (key, (lhs.unwrap(), rhs.unwrap()))))
    }

    pub fn left(
        self,
    ) -> KeyedStream<
        Key,
        LeftJoinTuple<Out1, Out2>,
        impl Operator<KeyValue<Key, LeftJoinTuple<Out1, Out2>>>,
    > {
        let inner = self
            .stream
            .add_operator(|prev| JoinLocalSortMerge::new(prev, JoinVariant::Left));
        KeyedStream(inner.map(|(key, (lhs, rhs))| (key, (lhs.unwrap(), rhs))))
    }

    pub fn outer(
        self,
    ) -> KeyedStream<
        Key,
        OuterJoinTuple<Out1, Out2>,
        impl Operator<KeyValue<Key, OuterJoinTuple<Out1, Out2>>>,
    > {
        let inner = self
            .stream
            .add_operator(|prev| JoinLocalSortMerge::new(prev, JoinVariant::Outer));
        KeyedStream(inner)
    }
}

impl<Key: Data + Ord, Out1: ExchangeData, Out2: ExchangeData, Keyer1, Keyer2>
    JoinStreamLocalSortMerge<Key, Out1, Out2, Keyer1, Keyer2, ShipBroadcastRight>
where
    Keyer1: KeyerFn<Key, Out1>,
    Keyer2: KeyerFn<Key, Out2>,
{
    pub fn inner(
        self,
    ) -> Stream<
        KeyValue<Key, InnerJoinTuple<Out1, Out2>>,
        impl Operator<KeyValue<Key, InnerJoinTuple<Out1, Out2>>>,
    > {
        self.stream
            .add_operator(|prev| JoinLocalSortMerge::new(prev, JoinVariant::Inner))
            .map(|(key, (lhs, rhs))| (key, (lhs.unwrap(), rhs.unwrap())))
    }

    pub fn left(
        self,
    ) -> Stream<
        KeyValue<Key, LeftJoinTuple<Out1, Out2>>,
        impl Operator<KeyValue<Key, LeftJoinTuple<Out1, Out2>>>,
    > {
        self.stream
            .add_operator(|prev| JoinLocalSortMerge::new(prev, JoinVariant::Left))
            .map(|(key, (lhs, rhs))| (key, (lhs.unwrap(), rhs)))
    }
}