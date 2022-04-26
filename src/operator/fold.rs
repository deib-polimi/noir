use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{ready, StreamExt};

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Data, ExchangeData, Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;
use crate::stream::Stream;

use super::AsyncOperator;

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Fold<Out: Data, NewOut: Data, F, PreviousOperators>
where
    F: Fn(&mut NewOut, Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    prev: PreviousOperators,
    #[derivative(Debug = "ignore")]
    fold: F,
    init: NewOut,
    accumulator: Option<NewOut>,
    timestamp: Option<Timestamp>,
    max_watermark: Option<Timestamp>,
    received_end: bool,
    received_end_iter: bool,
    _out: PhantomData<Out>,
}

impl<Out: Data, NewOut: Data, F, PreviousOperators: Operator<Out>>
    Fold<Out, NewOut, F, PreviousOperators>
where
    F: Fn(&mut NewOut, Out) + Send + Clone,
{
    fn new(prev: PreviousOperators, init: NewOut, fold: F) -> Self {
        Fold {
            prev,
            fold,
            init,
            accumulator: None,
            timestamp: None,
            max_watermark: None,
            received_end: false,
            received_end_iter: false,
            _out: Default::default(),
        }
    }
}

impl<Out: Data, NewOut: Data, F, PreviousOperators> Operator<NewOut>
    for Fold<Out, NewOut, F, PreviousOperators>
where
    F: Fn(&mut NewOut, Out) + Send + Clone,
    PreviousOperators: Operator<Out>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<NewOut> {
        while !self.received_end {
            match self.prev.next() {
                StreamElement::Terminate => self.received_end = true,
                StreamElement::FlushAndRestart => {
                    self.received_end = true;
                    self.received_end_iter = true;
                }
                StreamElement::Watermark(ts) => {
                    self.max_watermark = Some(self.max_watermark.unwrap_or(ts).max(ts))
                }
                StreamElement::Item(item) => {
                    if self.accumulator.is_none() {
                        self.accumulator = Some(self.init.clone());
                    }
                    (self.fold)(self.accumulator.as_mut().unwrap(), item);
                }
                StreamElement::Timestamped(item, ts) => {
                    self.timestamp = Some(self.timestamp.unwrap_or(ts).max(ts));
                    if self.accumulator.is_none() {
                        self.accumulator = Some(self.init.clone());
                    }
                    (self.fold)(self.accumulator.as_mut().unwrap(), item);
                }
                // this block wont sent anything until the stream ends
                StreamElement::FlushBatch => {}
                StreamElement::Yield => return StreamElement::Yield, //TODO: Check
            }
        }

        // If there is an accumulated value, return it
        if let Some(acc) = self.accumulator.take() {
            if let Some(ts) = self.timestamp.take() {
                return StreamElement::Timestamped(acc, ts);
            } else {
                return StreamElement::Item(acc);
            }
        }

        // If watermark were received, send one downstream
        if let Some(ts) = self.max_watermark.take() {
            return StreamElement::Watermark(ts);
        }

        // the end was not really the end... just the end of one iteration!
        if self.received_end_iter {
            self.received_end_iter = false;
            self.received_end = false;
            return StreamElement::FlushAndRestart;
        }

        StreamElement::Terminate
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> Fold<{} -> {}>",
            self.prev.to_string(),
            std::any::type_name::<Out>(),
            std::any::type_name::<NewOut>()
        )
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<NewOut, _>("Fold"))
    }
}

impl<Out: Data + Unpin, NewOut: Data + Unpin, F, PreviousOperators> futures::Stream
    for Fold<Out, NewOut, F, PreviousOperators>
where
    F: Fn(&mut NewOut, Out) + Send + Clone + Unpin,
    PreviousOperators: AsyncOperator<Out>,
{
    type Item = StreamElement<NewOut>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        while !self.received_end {
            let next = ready!(self.prev.poll_next_unpin(cx)).unwrap_or(StreamElement::Terminate);
            match next {
                StreamElement::Terminate => self.received_end = true,
                StreamElement::FlushAndRestart => {
                    self.received_end = true;
                    self.received_end_iter = true;
                }
                StreamElement::Watermark(ts) => {
                    self.max_watermark = Some(self.max_watermark.unwrap_or(ts).max(ts))
                }
                StreamElement::Item(item) => {
                    if self.accumulator.is_none() {
                        self.accumulator = Some(self.init.clone());
                    }
                    let mut acc = self.accumulator.take().unwrap();
                    (self.fold)(&mut acc, item);
                    self.accumulator = Some(acc);
                }
                StreamElement::Timestamped(item, ts) => {
                    self.timestamp = Some(self.timestamp.unwrap_or(ts).max(ts));
                    if self.accumulator.is_none() {
                        self.accumulator = Some(self.init.clone());
                    }
                    let mut acc = self.accumulator.take().unwrap();
                    (self.fold)(&mut acc, item);
                    self.accumulator = Some(acc);
                }
                // this block wont sent anything until the stream ends
                StreamElement::FlushBatch => {}
                StreamElement::Yield => panic!("ASDASD"), //TODO: Check
            }
        }

        // If there is an accumulated value, return it
        if let Some(acc) = self.accumulator.take() {
            if let Some(ts) = self.timestamp.take() {
                return Poll::Ready(Some(StreamElement::Timestamped(acc, ts)));
            } else {
                return Poll::Ready(Some(StreamElement::Item(acc)));
            }
        }

        // If watermark were received, send one downstream
        if let Some(ts) = self.max_watermark.take() {
            return Poll::Ready(Some(StreamElement::Watermark(ts)));
        }

        // the end was not really the end... just the end of one iteration!
        if self.received_end_iter {
            self.received_end_iter = false;
            self.received_end = false;
            return Poll::Ready(Some(StreamElement::FlushAndRestart));
        }

        Poll::Ready(Some(StreamElement::Terminate))
    }
}



impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Fold the stream into a stream that emits a single value.
    ///
    /// The folding operator consists in adding to the current accumulation value (initially the
    /// value provided as `init`) the value of the current item in the stream.
    ///
    /// The folding function is provided with a mutable reference to the current accumulator and the
    /// owned item of the stream. The function should modify the accumulator without returning
    /// anything.
    ///
    /// Note that the output type may be different from the input type. Consider using
    /// [`Stream::reduce`] if the output type is the same as the input type.
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this operator is not parallelized, it creates a bottleneck where all the stream
    /// elements are sent to and the folding is done using a single thread.
    ///
    /// **Note**: this is very similar to [`Iteartor::fold`](std::iter::Iterator::fold).
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s.fold(0, |acc, value| *acc += value).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0 + 1 + 2 + 3 + 4]);
    /// ```
    pub fn fold<NewOut: Data, F>(self, init: NewOut, f: F) -> Stream<NewOut, impl Operator<NewOut>>
    where
        Out: ExchangeData,
        F: Fn(&mut NewOut, Out) + Send + Clone + 'static,
    {
        self.max_parallelism(1)
            .add_operator(|prev| Fold::new(prev, init, f))
    }

    /// Fold the stream into a stream that emits a single value.
    ///
    /// The folding operator consists in adding to the current accumulation value (initially the
    /// value provided as `init`) the value of the current item in the stream.
    ///
    /// This method is very similary to [`Stream::fold`], but performs the folding distributely. To
    /// do so the folding function must be _associative_, in particular the folding process is
    /// performed in 2 steps:
    ///
    /// - `local`: the local function is used to fold the elements present in each replica of the
    ///   stream independently. All those replicas will start with the same `init` value.
    /// - `global`: all the partial results (the elements produced by the `local` step) have to be
    ///   aggregated into a single result. This is done using the `global` folding function.
    ///
    /// Note that the output type may be different from the input type, therefore requireing
    /// different function for the aggregation. Consider using [`Stream::reduce_assoc`] if the
    /// output type is the same as the input type.
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s.fold_assoc(0, |acc, value| *acc += value, |acc, value| *acc += value).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0 + 1 + 2 + 3 + 4]);
    /// ```
    pub fn fold_assoc<NewOut: ExchangeData, Local, Global>(
        self,
        init: NewOut,
        local: Local,
        global: Global,
    ) -> Stream<NewOut, impl Operator<NewOut>>
    where
        Local: Fn(&mut NewOut, Out) + Send + Clone + 'static,
        Global: Fn(&mut NewOut, NewOut) + Send + Clone + 'static,
    {
        self.add_operator(|prev| Fold::new(prev, init.clone(), local))
            .max_parallelism(1)
            .add_operator(|prev| Fold::new(prev, init, global))
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: AsyncOperator<Out> + Unpin + 'static,
{
    /// Fold the stream into a stream that emits a single value.
    ///
    /// The folding operator consists in adding to the current accumulation value (initially the
    /// value provided as `init`) the value of the current item in the stream.
    ///
    /// The folding function is provided with a mutable reference to the current accumulator and the
    /// owned item of the stream. The function should modify the accumulator without returning
    /// anything.
    ///
    /// Note that the output type may be different from the input type. Consider using
    /// [`Stream::reduce`] if the output type is the same as the input type.
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this operator is not parallelized, it creates a bottleneck where all the stream
    /// elements are sent to and the folding is done using a single thread.
    ///
    /// **Note**: this is very similar to [`Iteartor::fold`](std::iter::Iterator::fold).
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s.fold(0, |acc, value| *acc += value).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0 + 1 + 2 + 3 + 4]);
    /// ```
    pub fn fold_async<NewOut: Data + Unpin, F>(self, init: NewOut, f: F) -> Stream<NewOut, impl AsyncOperator<NewOut>>
    where
        Out: ExchangeData,
        F: Fn(&mut NewOut, Out) + Send + Clone + 'static + Unpin,
    {
        self.max_parallelism_async(1)
            .add_async_operator(|prev| Fold::new(prev, init, f))
    }

    /// Fold the stream into a stream that emits a single value.
    ///
    /// The folding operator consists in adding to the current accumulation value (initially the
    /// value provided as `init`) the value of the current item in the stream.
    ///
    /// This method is very similary to [`Stream::fold`], but performs the folding distributely. To
    /// do so the folding function must be _associative_, in particular the folding process is
    /// performed in 2 steps:
    ///
    /// - `local`: the local function is used to fold the elements present in each replica of the
    ///   stream independently. All those replicas will start with the same `init` value.
    /// - `global`: all the partial results (the elements produced by the `local` step) have to be
    ///   aggregated into a single result. This is done using the `global` folding function.
    ///
    /// Note that the output type may be different from the input type, therefore requireing
    /// different function for the aggregation. Consider using [`Stream::reduce_assoc`] if the
    /// output type is the same as the input type.
    ///
    /// **Note**: this operator will retain all the messages of the stream and emit the values only
    /// when the stream ends. Therefore this is not properly _streaming_.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s.fold_assoc(0, |acc, value| *acc += value, |acc, value| *acc += value).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![0 + 1 + 2 + 3 + 4]);
    /// ```
    pub fn fold_assoc_async<NewOut: ExchangeData, Local, Global>(
        self,
        init: NewOut,
        local: Local,
        global: Global,
    ) -> Stream<NewOut, impl Operator<NewOut>>
    where
        Local: Fn(&mut NewOut, Out) + Send + Clone + 'static,
        Global: Fn(&mut NewOut, NewOut) + Send + Clone + 'static,
    {
        self.add_operator(|prev| Fold::new(prev, init.clone(), local))
            .max_parallelism(1)
            .add_operator(|prev| Fold::new(prev, init, global))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::operator::fold::Fold;
    use crate::operator::{Operator, StreamElement};
    use crate::test::FakeOperator;

    #[test]
    fn test_fold_without_timestamps() {
        let fake_operator = FakeOperator::new(0..10u8);
        let mut fold = Fold::new(fake_operator, 0, |a, b| *a += b);

        assert_eq!(fold.next(), StreamElement::Item((0..10u8).sum()));
        assert_eq!(fold.next(), StreamElement::Terminate);
    }

    #[test]
    #[allow(clippy::identity_op)]
    fn test_fold_timestamped() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Timestamped(0, Duration::from_secs(1)));
        fake_operator.push(StreamElement::Timestamped(1, Duration::from_secs(2)));
        fake_operator.push(StreamElement::Timestamped(2, Duration::from_secs(3)));
        fake_operator.push(StreamElement::Watermark(Duration::from_secs(4)));

        let mut fold = Fold::new(fake_operator, 0, |a, b| *a += b);

        assert_eq!(
            fold.next(),
            StreamElement::Timestamped(0 + 1 + 2, Duration::from_secs(3))
        );
        assert_eq!(
            fold.next(),
            StreamElement::Watermark(Duration::from_secs(4))
        );
        assert_eq!(fold.next(), StreamElement::Terminate);
    }

    #[test]
    #[allow(clippy::identity_op)]
    fn test_fold_iter_end() {
        let mut fake_operator = FakeOperator::empty();
        fake_operator.push(StreamElement::Item(0));
        fake_operator.push(StreamElement::Item(1));
        fake_operator.push(StreamElement::Item(2));
        fake_operator.push(StreamElement::FlushAndRestart);
        fake_operator.push(StreamElement::Item(3));
        fake_operator.push(StreamElement::Item(4));
        fake_operator.push(StreamElement::Item(5));
        fake_operator.push(StreamElement::FlushAndRestart);

        let mut fold = Fold::new(fake_operator, 0, |a, b| *a += b);

        assert_eq!(fold.next(), StreamElement::Item(0 + 1 + 2));
        assert_eq!(fold.next(), StreamElement::FlushAndRestart);
        assert_eq!(fold.next(), StreamElement::Item(3 + 4 + 5));
        assert_eq!(fold.next(), StreamElement::FlushAndRestart);
        assert_eq!(fold.next(), StreamElement::Terminate);
    }
}
