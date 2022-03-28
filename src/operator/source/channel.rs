use std::task::Poll;

// #[cfg(all(feature = "crossbeam", not(feature = "flume")))]
// use crossbeam_channel::{bounded, Receiver, RecvError, Sender, TryRecvError};
// #[cfg(all(feature = "flume", not(feature = "crossbeam")))]
// use flume::{bounded, Receiver, RecvError, Sender, TryRecvError};

use futures::ready;
use tokio::sync::mpsc::*;
use tokio::sync::mpsc::error::*;
use tracing::instrument;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::operator::source::Source;
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

const MAX_RETRY: u8 = 8;

/// Source that consumes an iterator and emits all its elements into the stream.
///
/// The iterator will be consumed **only from one replica**, therefore this source is not parallel.

#[derive(Debug, Clone, Copy)]
enum State {
    Running,
    Terminated,
    Stopped,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ChannelSource<Out: Data> {
    #[derivative(Debug = "ignore")]
    rx: Receiver<Out>,
    state: State,
    retry_count: u8,
}

impl<Out: Data> ChannelSource<Out> {
    /// Create a new source that reads the items from the iterator provided as input.
    ///
    /// **Note**: this source is **not parallel**, the iterator will be consumed only on a single
    /// replica, on all the others no item will be read from the iterator. If you want to achieve
    /// parallelism you need to add an operator that shuffles the data (e.g.
    /// [`Stream::shuffle`](crate::Stream::shuffle)).
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::ChannelSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let (tx_channel, source) = ChannelSource::new(4);
    /// let R = env.stream(source);
    /// tx_channel.send(1);
    /// tx_channel.send(2);
    /// ```
    pub fn new(channel_size: usize) -> (Sender<Out>, Self) {
        let (tx, rx) = channel(channel_size);
        let s = Self {
            rx,
            state: State::Running,
            retry_count: 0,
        };

        (tx, s)
    }
}
// TODO: remove Debug requirement
impl<Out: Data + core::fmt::Debug> Source<Out> for ChannelSource<Out> {
    fn get_max_parallelism(&self) -> Option<usize> {
        Some(1)
    }
}

impl<Out: Data + core::fmt::Debug> futures::Stream for ChannelSource<Out> {
    type Item = StreamElement<Out>;

    #[tracing::instrument(name = "channel_source", skip_all)]
    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
        match self.state {
            State::Terminated => {
                tracing::trace!("sending terminate");
                self.state = State::Stopped;
                return Poll::Ready(Some(StreamElement::Terminate));
            }
            State::Stopped => return Poll::Ready(None),
            State::Running => {}
        }

        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(t)) => {
                Poll::Ready(Some(StreamElement::Item(t)))
            }
            Poll::Ready(None) => {
                self.state = State::Terminated;
                log::info!("Stream disconnected");
                Poll::Ready(Some(StreamElement::FlushAndRestart))
            }
            Poll::Pending => {
                Poll::Pending
            }
        }
    }
}

impl<Out: Data + core::fmt::Debug> Operator<Out> for ChannelSource<Out> {
    fn setup(&mut self, _metadata: ExecutionMetadata) {}

    fn next(&mut self) -> StreamElement<Out> {
        match self.state {
            State::Terminated | State::Stopped => return StreamElement::Terminate,
            State::Running => {}
        }
        
        let result = self.rx.try_recv();

        match result {
            Ok(t) => {
                self.retry_count = 0;
                StreamElement::Item(t)
            }
            Err(TryRecvError::Empty) if self.retry_count < MAX_RETRY => {
                // Spin before blocking
                self.retry_count += 1;
                self.next()
            }
            Err(TryRecvError::Empty) if self.retry_count == MAX_RETRY => {
                log::debug!("no values ready after {MAX_RETRY} tries, sending flush");
                self.retry_count += 1;
                StreamElement::FlushBatch // TODO: if Yield implicitly flushes this is not needed
            }
            Err(TryRecvError::Empty) => {
                StreamElement::Yield
            }
            Err(TryRecvError::Disconnected) => {
                self.state = State::Terminated;
                log::info!("Stream disconnected");
                StreamElement::FlushAndRestart
            }
        }
    }

    fn to_string(&self) -> String {
        format!("ChannelSource<{}>", std::any::type_name::<Out>())
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("ChannelSource");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }
}

impl<Out: Data> Clone for ChannelSource<Out> {
    fn clone(&self) -> Self {
        // Since this is a non-parallel source, we don't want the other replicas to emit any value
        panic!("ChannelSource cannot be cloned, max_parallelism should be 1");
    }
}
