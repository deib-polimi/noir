use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Data, Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;

#[derive(Clone)]
struct HeapElement<Out> {
    item: Out,
    timestamp: Timestamp,
}

impl<Out> Ord for HeapElement<Out> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Returns the opposite of the actual order of the timestamps
        // This is needed to have a min-heap
        other.timestamp.cmp(&self.timestamp)
    }
}

impl<Out> PartialOrd for HeapElement<Out> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<Out> Eq for HeapElement<Out> {}

impl<Out> PartialEq for HeapElement<Out> {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp
    }
}

#[derive(Clone)]
pub(crate) struct Reorder<Out: Data, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    buffer: BinaryHeap<HeapElement<Out>>,
    last_watermark: Option<Timestamp>,
    prev: PreviousOperators,
    received_end: bool,
}

impl<Out: Data, PreviousOperators> Reorder<Out, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    pub(crate) fn new(prev: PreviousOperators) -> Self {
        Self {
            buffer: Default::default(),
            last_watermark: None,
            prev,
            received_end: false,
        }
    }
}

impl<Out: Data, PreviousOperators> Operator<Out> for Reorder<Out, PreviousOperators>
where
    PreviousOperators: Operator<Out>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<Out> {
        while !self.received_end && self.last_watermark.is_none() {
            match self.prev.next() {
                element @ StreamElement::Item(_) => return element,
                StreamElement::Timestamped(item, timestamp) => {
                    self.buffer.push(HeapElement { item, timestamp })
                }
                StreamElement::Watermark(ts) => self.last_watermark = Some(ts),
                StreamElement::FlushBatch => return StreamElement::FlushBatch,
                StreamElement::FlushAndRestart => self.received_end = true,
                StreamElement::Terminate => return StreamElement::Terminate,
                StreamElement::Yield => return StreamElement::Yield,
            }
        }

        if let Some(ts) = self.last_watermark {
            match self.buffer.peek() {
                Some(top) if top.timestamp <= ts => {
                    let element = self.buffer.pop().unwrap();
                    StreamElement::Timestamped(element.item, element.timestamp)
                }
                _ => StreamElement::Watermark(self.last_watermark.take().unwrap()),
            }
        } else {
            // here we know that received_end must be true, otherwise we wouldn't have exited the loop
            match self.buffer.pop() {
                // pop remaining elements in the heap
                Some(element) => StreamElement::Timestamped(element.item, element.timestamp),
                None => {
                    self.received_end = false;
                    StreamElement::FlushAndRestart
                }
            }
        }
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> Reorder<{}>",
            self.prev.to_string(),
            std::any::type_name::<Out>(),
        )
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Out, _>("Reorder"))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::operator::reorder::Reorder;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    fn reorder() {
        let mut fake = FakeOperator::empty();
        for i in &[1, 3, 2] {
            fake.push(StreamElement::Timestamped(*i, Duration::new(*i, 0)));
        }
        fake.push(StreamElement::Watermark(Duration::new(3, 0)));
        for i in &[6, 4, 5] {
            fake.push(StreamElement::Timestamped(*i, Duration::new(*i, 0)));
        }
        fake.push(StreamElement::FlushAndRestart);
        fake.push(StreamElement::Terminate);

        let mut reorder = Reorder::new(fake);

        for i in 1..=6 {
            assert_eq!(
                reorder.next(),
                StreamElement::Timestamped(i, Duration::new(i, 0))
            );
            if i == 3 {
                assert_eq!(
                    reorder.next(),
                    StreamElement::Watermark(Duration::new(3, 0))
                );
            }
        }

        assert_eq!(reorder.next(), StreamElement::FlushAndRestart);
        assert_eq!(reorder.next(), StreamElement::Terminate);
    }
}
