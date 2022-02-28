use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::concat::ConcatElement;
use crate::operator::reorder::Reorder;
use crate::operator::{
    timestamp_max, ExchangeData, ExchangeDataKey, Operator, StreamElement, Timestamp,
};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, Stream};

type OutputElement<Key, Out, Out2> = KeyValue<Key, (Out, Out2)>;

/// Operator that performs an interval join.
///
/// Given a `lower_bound` and an `upper_bound` duration, each element of the left side with
/// timestamp `ts` is matched with each element of the right side that has timestamp inside the
/// interval `ts - lower_bound` and `ts + upper_bound` (inclusive).
///
/// This operator assumes elements are received in increasing order of timestamp.
#[derive(Clone, Debug)]
struct IntervalJoin<Key, Out, Out2, OperatorChain>
where
    Key: ExchangeDataKey,
    Out: ExchangeData,
    Out2: ExchangeData,
    OperatorChain: Operator<KeyValue<Key, ConcatElement<Out, Out2>>>,
{
    prev: OperatorChain,
    /// Elements of the left side to be processed.
    left: VecDeque<(Timestamp, KeyValue<Key, Out>)>,
    /// Elements of the right side that might still be matched.
    right: HashMap<Key, VecDeque<(Timestamp, Out2)>, ahash::RandomState>,
    /// Elements ready to be sent downstream.
    buffer: VecDeque<(Timestamp, OutputElement<Key, Out, Out2>)>,
    /// Timestamp of the last element (item or watermark).
    last_seen: Timestamp,
    /// Upper bound duration of the interval.
    upper_bound: Duration,
    /// Lower bound duration of the interval.
    lower_bound: Duration,
    /// Whether the operator has received a `FlushAndRestart` message.
    received_restart: bool,
}

impl<Key, Out, Out2, OperatorChain> IntervalJoin<Key, Out, Out2, OperatorChain>
where
    Key: ExchangeDataKey,
    Out: ExchangeData,
    Out2: ExchangeData,
    OperatorChain: Operator<KeyValue<Key, ConcatElement<Out, Out2>>>,
{
    fn new(prev: OperatorChain, lower_bound: Duration, upper_bound: Duration) -> Self {
        Self {
            prev,
            left: Default::default(),
            right: Default::default(),
            buffer: Default::default(),
            last_seen: Default::default(),
            upper_bound,
            lower_bound,
            received_restart: false,
        }
    }

    /// Advance the operator, trying to generate some join tuples.
    fn advance(&mut self) {
        while let Some((left_ts, (lkey, lvalue))) = self.left.front() {
            // find lower and upper limits of the interval
            let lower = left_ts
                .checked_sub(self.lower_bound)
                .unwrap_or_else(|| Duration::new(0, 0));
            let upper = left_ts
                .checked_add(self.upper_bound)
                .unwrap_or_else(timestamp_max);

            if upper >= self.last_seen && !self.received_restart {
                // there could be some elements in the interval that have not been received yet
                break;
            }

            if let Some(right) = self.right.get_mut(lkey) {
                // Remove elements of the right side that are not going to be matched anymore.
                // This happens when the timestamp of the right element is less than the lower bound
                // of the current interval, since elements of the left side are ordered by ascending
                // timestamp and the lower bound can only increase.
                while let Some((right_ts, _)) = right.front() {
                    if *right_ts < lower {
                        right.pop_front();
                    } else {
                        break;
                    }
                }

                // generate all the join tuples
                let matches = right
                    .iter()
                    .take_while(|(right_ts, _)| *right_ts <= upper)
                    .map(|(right_ts, rvalue)| {
                        let ts = right_ts.max(left_ts);
                        let item = (lkey.clone(), (lvalue.clone(), rvalue.clone()));
                        (*ts, item)
                    });

                // add the generated tuples to the output buffer
                self.buffer.extend(matches);
            }

            // remove the element of the left side
            self.left.pop_front();
        }

        if self.left.is_empty() && self.received_restart {
            // the operator has received a `FlushAndRestart` message and there are no elements
            // remaining in the left side, so we can clear also the right side
            self.right.clear();
        }
    }
}

impl<Key, Out, Out2, OperatorChain> Operator<KeyValue<Key, (Out, Out2)>>
    for IntervalJoin<Key, Out, Out2, OperatorChain>
where
    Key: ExchangeDataKey,
    Out: ExchangeData,
    Out2: ExchangeData,
    OperatorChain: Operator<KeyValue<Key, ConcatElement<Out, Out2>>>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<(Key, (Out, Out2))> {
        while self.buffer.is_empty() {
            if self.received_restart {
                assert!(self.left.is_empty());
                assert!(self.right.is_empty());

                self.received_restart = false;
                self.last_seen = Default::default();

                return StreamElement::FlushAndRestart;
            }

            match self.prev.next() {
                StreamElement::Timestamped((key, item), ts) => {
                    assert!(ts >= self.last_seen);
                    self.last_seen = ts;
                    match item {
                        ConcatElement::Left(item) => self.left.push_back((ts, (key, item))),
                        ConcatElement::Right(item) => {
                            self.right.entry(key).or_default().push_back((ts, item))
                        }
                    }
                }
                StreamElement::Watermark(ts) => {
                    assert!(ts >= self.last_seen);
                    self.last_seen = ts;
                }
                StreamElement::FlushAndRestart => {
                    self.received_restart = true;
                }
                StreamElement::Item(_) => panic!("Interval Join only supports timestamped streams"),
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::Terminate => return StreamElement::Terminate,
                StreamElement::Yield => return StreamElement::Yield, //TODO: Check
            }

            self.advance();
        }

        let (ts, item) = self.buffer.pop_front().unwrap();
        StreamElement::Timestamped(item, ts)
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> IntervalJoin<{}, {:?}, {:?}>",
            self.prev.to_string(),
            std::any::type_name::<KeyValue<Key, (Out, Out2)>>(),
            self.lower_bound,
            self.upper_bound,
        )
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<KeyValue<Key, (Out, Out2)>, _>(
                "IntervalJoin",
            ))
    }
}

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Given two streams **with timestamps** join them according to an interval centered around the
    /// timestamp of the left side.
    ///
    /// This means that an element on the left side with timestamp T will be joined to all the
    /// elements on the right with timestamp Q such that `T - lower_bound <= Q <= T + upper_bound`.
    ///
    /// **Note**: this operator is not parallelized, all the elements are sent to a single node to
    /// perform the join.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// TODO: example
    pub fn interval_join<Out2, OperatorChain2>(
        self,
        right: Stream<Out2, OperatorChain2>,
        lower_bound: Duration,
        upper_bound: Duration,
    ) -> Stream<(Out, Out2), impl Operator<(Out, Out2)>>
    where
        Out2: ExchangeData,
        OperatorChain2: Operator<Out2> + 'static,
    {
        let left = self.max_parallelism(1);
        let right = right.max_parallelism(1);
        left.map_concat(right)
            .key_by(|_| ())
            .add_operator(Reorder::new)
            .add_operator(|prev| IntervalJoin::new(prev, lower_bound, upper_bound))
            .drop_key()
    }
}

impl<Key: ExchangeDataKey, Out: ExchangeData, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Given two streams **with timestamps** join them according to an interval centered around the
    /// timestamp of the left side.
    ///
    /// This means that an element on the left side with timestamp T will be joined to all the
    /// elements on the right with timestamp Q such that `T - lower_bound <= Q <= T + upper_bound`.
    /// Only items with the same key can be joined together.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// TODO: example
    pub fn interval_join<Out2, OperatorChain2>(
        self,
        right: KeyedStream<Key, Out2, OperatorChain2>,
        lower_bound: Duration,
        upper_bound: Duration,
    ) -> KeyedStream<Key, (Out, Out2), impl Operator<KeyValue<Key, (Out, Out2)>>>
    where
        Out2: ExchangeData,
        OperatorChain2: Operator<KeyValue<Key, Out2>> + 'static,
    {
        self.map_concat(right)
            .add_operator(Reorder::new)
            .add_operator(|prev| IntervalJoin::new(prev, lower_bound, upper_bound))
    }
}
