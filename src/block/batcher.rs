use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use coarsetime::Instant;

use futures::{ready, SinkExt, Sink};
use crate::network::{Coord, NetworkMessage, NetworkSender, ReceiverEndpoint};
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

enum FlushState<T> {
    Idle,
    Pending(T),
    MustFlush,
}

impl<T> FlushState<T> {
    pub fn is_idle(&self) -> bool {
        match &self {
            FlushState::Idle => true,
            _ => false,
        }
    }
    pub fn take_pending(&mut self) -> T {
        let prev = std::mem::replace(self, FlushState::MustFlush);
        match prev {
            FlushState::Pending(t) => t,
            _ => panic!("Trying to take from invalid FlushState")
        }
    }
}

/// A `Batcher` wraps a sender and sends the messages in batches to reduce the network overhead.
///
/// Internally it spawns a new task to handle the timeouts and join it at the end.
pub(crate) struct Batcher<Out: ExchangeData> {
    /// Sender used to communicate with the other replicas
    remote_sender: NetworkSender<Out>,
    /// Batching mode used by the batcher
    mode: BatchMode,
    /// Buffer used to keep messages ready to be sent
    buffer: Vec<StreamElement<Out>>,
    /// Time of the last flush of the buffer.    
    last_send: Instant,
    /// The coordinate of this block, used for marking the sender of the batch.
    coord: Coord,

    state: FlushState<NetworkMessage<Out>>,
}

impl<Out: ExchangeData> Batcher<Out> {
    pub fn new(remote_sender: NetworkSender<Out>, mode: BatchMode, coord: Coord) -> Self {
        Self {
            remote_sender,
            mode,
            buffer: Default::default(),
            last_send: Instant::now(),
            coord,
            state: FlushState::Idle,
        }
    }
    
    pub fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), FlushError>> {        
        // TODO: Could possibly be done better
        loop {
            if matches!(self.state, FlushState::Idle) {
                return Poll::Ready(Ok(()))
            } else if matches!(self.state, FlushState::Pending(_)) {
                let res = ready!(self.remote_sender.poll_ready_unpin(cx))
                    .and_then(|_| {
                        let q = self.state.take_pending();
                        self.remote_sender.start_send_unpin(q)
                    });
                match res {
                    Ok(_) => {
                        self.state = FlushState::MustFlush;
                        continue;
                    }
                    Err(_) => {
                        return Poll::Ready(Err(FlushError::Disconnected))
                    }
                }
            } else if matches!(self.state, FlushState::MustFlush) {
                let res = ready!(self.remote_sender.poll_flush_unpin(cx));
                match res {
                    Ok(_) => {
                        log::warn!("flushed\t{}", &self.remote_endpoint());
                        self.state = FlushState::Idle;
                        return Poll::Ready(Ok(()))
                    },
                    Err(_) => {
                        return Poll::Ready(Err(FlushError::Disconnected))
                    }
                }
            } else {
                unreachable!();
            }
        }
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
                    self.stage_batch();
                    true
                } else {
                    false
                }
            }
            BatchMode::Fixed(n) => {
                self.buffer.push(msg);
                if self.buffer.len() >= n.get() {
                    self.stage_batch();
                    true
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

    pub fn request_flush(&mut self) {
        self.stage_batch()
    }

    fn stage_batch(&mut self) {
        if !self.buffer.is_empty() {
            let cap = self.buffer.capacity();
            let new_cap = if self.buffer.len() < cap / 2 { cap / 2 } else { cap };
            let mut batch = Vec::with_capacity(new_cap);
            std::mem::swap(&mut self.buffer, &mut batch);
            let message = NetworkMessage::new_batch(batch, self.coord);
            self.stage_message(message)
        }
    }

    pub fn blocking_send_one(&mut self, msg: StreamElement<Out>) -> Result<(), FlushError> {
        log::warn!("USING DEPRECATED BLOCKING CHANNELS TO SEND FOR {}", self.coord);
        match self.remote_sender.clone_inner().unwrap().blocking_send(NetworkMessage::new_single(msg, self.coord)) {
            Ok(_) => Ok(()),
            Err(e) => Err(FlushError::Disconnected),
        }
    }

    fn stage_message(&mut self, msg: NetworkMessage<Out>) {
        assert!(self.state.is_idle());
        self.state = FlushState::Pending(msg);
    }

    pub fn coord(&self) -> Coord {
        self.coord
    }

    pub fn remote_endpoint(&self) -> ReceiverEndpoint {
        self.remote_sender.receiver_endpoint
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
