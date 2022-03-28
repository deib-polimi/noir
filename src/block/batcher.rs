use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use coarsetime::Instant;

use futures::sink::Send;
use futures::{ready, SinkExt, Sink, Future};
use tokio_util::sync::ReusableBoxFuture;
use crate::network::{Coord, NetworkMessage, NetworkSender, ReceiverEndpoint, NetworkSendError};
use crate::operator::{ExchangeData, StreamElement};

/// Which policy to use for batching the messages before sending them.
///
/// Avoid constructing directly this enumeration, please use [`BatchMode::fixed()`] and
/// [`BatchMode::adaptive()`] constructors.
///
/// The default batch mode is `Adaptive(1024, 50ms)`, meaning that a batch is flushed either when
/// it has at least 1024 messages, or no message has been received in the last 50ms.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum BatchMode {
    /// A batch is flushed only when the specified number of messages is present.
    Fixed(NonZeroUsize),
    /// A batch is flushed only when the specified number of messages is present or a timeout
    /// expires.
    Adaptive(NonZeroUsize, Duration),

    /// Send each message infdividually
    Single,
}

enum FlushState<T: ExchangeData> {
    Idle(NetworkSender<T>),
    Staged,
    Closed,
}

impl<T: ExchangeData> FlushState<T> {
    pub fn is_idle(&self) -> bool {
        match &self {
            FlushState::Idle(_) => true,
            _ => false,
        }
    }

    pub fn take(&mut self) -> Self {
        std::mem::replace(self, Self::Closed)
    }

    pub fn take_sender(&mut self) -> NetworkSender<T> {
        let prev = std::mem::replace(self, FlushState::Closed);
        match prev {
            FlushState::Idle(t) => t,
            _ => panic!("Trying to take sender from invalid FlushState")
        }
    }


    pub fn clone_sender(&mut self) -> NetworkSender<T> {
        match self {
            FlushState::Idle(s) => s.clone(),
            _ => panic!("Trying to clone batcher sender, but batcher is not idle!"),
        }
    }
    // pub fn take_pending(&mut self) -> T {
    //     let prev = std::mem::replace(self, FlushState::MustFlush);
    //     match prev {
    //         FlushState::Pending(t) => t,
    //         _ => panic!("Trying to take from invalid FlushState")
    //     }
    // }
}

// By reusing the same async fn for both `Some` and `None`, we make sure every future passed to
// ReusableBoxFuture has the same underlying type, and hence the same size and alignment.
async fn make_send_future<T: ExchangeData>(
    data: Option<(NetworkSender<T>, NetworkMessage<T>)>,
) -> Result<NetworkSender<T>, NetworkSendError<T>> {
    match data {
        Some((mut sender, msg)) => sender
                .send(msg)
                .await
                .map(move |_| sender),
        None => unreachable!("this future should not be pollable in this state"),
    }
}

/// A `Batcher` wraps a sender and sends the messages in batches to reduce the network overhead.
///
/// Internally it spawns a new task to handle the timeouts and join it at the end.
pub(crate) struct Batcher<Out: ExchangeData> {
    /// Sender used to communicate with the other replicas
    // remote_sender: Option<NetworkSender<Out>>,
    /// Batching mode used by the batcher
    mode: BatchMode,
    /// Buffer used to keep messages ready to be sent
    buffer: Vec<StreamElement<Out>>,
    /// Time of the last flush of the buffer.    
    last_send: Instant,
    /// The coordinate of this block, used for marking the sender of the batch.
    coord: Coord,

    pending: ReusableBoxFuture<'static, Result<NetworkSender<Out>, NetworkSendError<Out>>>,

    state: FlushState<Out>,

    remote_endpoint: ReceiverEndpoint,
}

impl<Out: ExchangeData> Batcher<Out> {
    pub fn new(remote_sender: NetworkSender<Out>, mode: BatchMode, coord: Coord) -> Self {
        let remote_endpoint = remote_sender.receiver_endpoint;
        Self {
            // remote_sender: Some(remote_sender),
            mode,
            buffer: Default::default(),
            last_send: Instant::now(),
            coord,
            pending: ReusableBoxFuture::new(make_send_future(None)),
            state: FlushState::Idle(remote_sender),
            remote_endpoint,
        }
    }
    
    pub fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), FlushError>> {        
        // TODO: Could possibly be done better
        let (result, next_state) = match self.state.take() {
            FlushState::Idle(s) => {
                (Poll::Ready(Ok(())), FlushState::Idle(s))
            }
            FlushState::Staged => {
                match self.pending.poll(cx) {
                    Poll::Ready(Ok(sender)) => {
                        self.pending.set(make_send_future(None)); // TODO REMOVE: To assert that it is never polled more than necessary
                        (Poll::Ready(Ok(())), FlushState::Idle(sender))
                    }
                    Poll::Pending => 
                        (Poll::Pending, FlushState::Staged),

                    Poll::Ready(Err(e)) => {
                        log::error!("{e}");
                        panic!(); // TODO: REMOVE
                        (Poll::Ready(Err(FlushError::Disconnected)), FlushState::Closed)
                    }
                }
            }
            FlushState::Closed => {
                unreachable!("Illegal batcher state!");
            }
        };

        self.state = next_state;
        result
    }

    /// Put a message in the batch queue, it won't be sent immediately.
    #[must_use]
    pub fn enqueue(&mut self, msg: StreamElement<Out>) -> bool {
        assert!(self.state.is_idle());
        match self.mode {
            BatchMode::Adaptive(n, max_delay) => {
                self.buffer.push(msg);
                let timeout_elapsed = self.last_send.elapsed() > max_delay.into();
                if self.buffer.len() >= n.get() || timeout_elapsed {
                    self.stage_batch()
                } else {
                    false
                }
            }
            BatchMode::Fixed(n) => {
                self.buffer.push(msg);
                if self.buffer.len() >= n.get() {
                    self.stage_batch()
                } else {
                    false
                }
            }
            BatchMode::Single => {
                let msg = NetworkMessage::new_single(msg, self.coord);
                self.stage_message(msg);
                true
            }
        }
    }

    pub fn stage_batch(&mut self) -> bool {
        if !self.buffer.is_empty() {
            let cap = self.buffer.capacity();
            let new_cap = if self.buffer.len() < cap / 2 { cap / 2 } else { cap };
            let mut batch = Vec::with_capacity(new_cap);
            std::mem::swap(&mut self.buffer, &mut batch);
            let message = NetworkMessage::new_batch(batch, self.coord);
            self.stage_message(message);
            true
        } else {
            false
        }
    }

    pub fn blocking_send_one(&mut self, msg: StreamElement<Out>) -> Result<(), FlushError> {
        log::warn!("USING DEPRECATED BLOCKING CHANNELS TO SEND FOR {}", self.coord);
        match self.state.clone_sender().clone_inner().unwrap().blocking_send(NetworkMessage::new_single(msg, self.coord)) {
            Ok(_) => Ok(()),
            Err(e) => Err(FlushError::Disconnected),
        }
    }

    fn stage_message(&mut self, msg: NetworkMessage<Out>) {
        assert!(self.state.is_idle());
        self.pending.set(make_send_future(Some((self.state.take_sender(), msg))));
        self.state = FlushState::Staged;
    }

    pub fn coord(&self) -> Coord {
        self.coord
    }

    pub fn remote_endpoint(&self) -> ReceiverEndpoint {
        self.remote_endpoint
    }

    pub(crate) fn is_idle(&self) -> bool {
        self.state.is_idle()
    }
}

impl BatchMode {
    /// Construct a new `BatchMode::Fixed` with the given positive batch size.
    pub fn fixed(size: usize) -> BatchMode {
        BatchMode::Fixed(NonZeroUsize::new(size).expect("The batch size must be positive"))
    }

    /// Construct a new `BatchMode::Adaptive` with the given positive batch size and maximum delay.
    pub fn adaptive(size: usize, max_delay: Duration) -> BatchMode {
        BatchMode::Adaptive(
            NonZeroUsize::new(size).expect("The batch size must be positive"),
            max_delay,
        )
    }

    /// Construct a new `BatchMode::Single`.
    pub fn single() -> BatchMode {
        BatchMode::Single
    }

    pub fn max_delay(&self) -> Option<Duration> {
        match &self {
            BatchMode::Adaptive(_, max_delay) => Some(*max_delay),
            BatchMode::Fixed(_) | BatchMode::Single => None,
        }
    }
}

impl Default for BatchMode {
    fn default() -> Self {
        BatchMode::adaptive(1024, Duration::from_millis(50))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FlushError {
    // Pending, // TODO: handle distinction between timed and not timed
    #[error("Channel disconnected")]
    Disconnected,
}
