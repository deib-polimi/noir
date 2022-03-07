use std::num::NonZeroUsize;
use std::time::Duration;

use coarsetime::Instant;

use tokio::sync::mpsc::error::TrySendError;
use crate::network::{Coord, NetworkMessage, NetworkSender, SendTimeoutError};
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

pub enum FlushError {
    Pending, // TODO: handle distinction between timed and not timed
    Disconnected,
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

    pending_msg: Option<NetworkMessage<Out>>,
}

impl<Out: ExchangeData> Batcher<Out> {
    pub(crate) fn new(remote_sender: NetworkSender<Out>, mode: BatchMode, coord: Coord) -> Self {
        Self {
            remote_sender,
            mode,
            buffer: Default::default(),
            last_send: Instant::now(),
            coord,
            pending_msg: None,
        }
    }

    #[inline]
    pub(crate) fn try_send(&mut self, msg: NetworkMessage<Out>) -> Result<(), FlushError> {
        assert!(self.pending_msg.is_none());
        match self.remote_sender.try_send(msg) {
            Ok(()) => {
                self.last_send = Instant::now();
                Ok(())
            }
            Err(TrySendError::Closed(msg)) => {
                self.pending_msg = Some(msg);
                Err(FlushError::Disconnected)
            }
            Err(TrySendError::Full(msg)) => {
                self.pending_msg = Some(msg);
                Err(FlushError::Pending)
            }
        }
    }

    #[inline]
    pub(crate) fn send_async(&mut self, msg: NetworkMessage<Out>, cx: std::task::Context<'_>) -> Result<(), FlushError> {
        assert!(self.pending_msg.is_none());
        match self.remote_sender.send_async(msg) {
            Ok(()) => {
                self.last_send = Instant::now();
                Ok(())
            }
            Err(TrySendError::Closed(msg)) => {
                self.pending_msg = Some(msg);
                Err(FlushError::Disconnected)
            }
            Err(TrySendError::Full(msg)) => {
                self.pending_msg = Some(msg);
                Err(FlushError::Pending)
            }
        }
    }

    // #[inline]
    // pub(crate) fn try_send_timeout(&mut self, msg: NetworkMessage<Out>, timeout: Duration) -> Result<(), FlushError> {
    //     assert!(self.pending_msg.is_none());
    //     match self.remote_sender.send_timeout(msg, timeout) {
    //         Ok(()) => {
    //             self.last_send = Instant::now();
    //             Ok(())
    //         }
    //         Err(SendTimeoutError::Disconnected(msg)) => {
    //             self.pending_msg = Some(msg);
    //             Err(FlushError::Disconnected)
    //         }
    //         Err(SendTimeoutError::Timeout(msg)) => {
    //             self.pending_msg = Some(msg);
    //             Err(FlushError::Pending)
    //         }
    //     }
    // }

    /// Put a message in the batch queue, it won't be sent immediately.
    pub(crate) fn enqueue_async(&mut self, message: StreamElement<Out>, cx: std::task::Context<'_>) -> Result<(), FlushError> {
        match self.mode {
            BatchMode::Adaptive(n, max_delay) => {
                self.buffer.push(message);
                let timeout_elapsed = self.last_send.elapsed() > max_delay.into();
                if self.buffer.len() >= n.get() || timeout_elapsed {
                    self.try_flush()
                } else {
                    Ok(())
                }
            }
            BatchMode::Fixed(n) => {
                self.buffer.push(message);
                if self.buffer.len() >= n.get() {
                    self.try_flush()
                } else {
                    Ok(())
                }
            }
            BatchMode::Single => {
                let message = NetworkMessage::new_single(message, self.coord);
                self.try_send(message)
            }
        }
    }

    /// Put a message in the batch queue, it won't be sent immediately.
    pub(crate) fn enqueue(&mut self, message: StreamElement<Out>) -> Result<(), FlushError> {
        match self.mode {
            BatchMode::Adaptive(n, max_delay) => {
                self.buffer.push(message);
                let timeout_elapsed = self.last_send.elapsed() > max_delay.into();
                if self.buffer.len() >= n.get() || timeout_elapsed {
                    self.try_flush()
                } else {
                    Ok(())
                }
            }
            BatchMode::Fixed(n) => {
                self.buffer.push(message);
                if self.buffer.len() >= n.get() {
                    self.try_flush()
                } else {
                    Ok(())
                }
            }
            BatchMode::Single => {
                let message = NetworkMessage::new_single(message, self.coord);
                self.try_send(message)
            }
        }
    }

    /// Flush the internal buffer if it's not empty.
    pub(crate) fn flush_async(&mut self) -> Result<(), FlushError> {
        // log::warn!("Requesting flush. pending: {}\tbuf: {}", self.pending_msg.is_some(), self.buffer.len());
        if let Some(msg) = self.pending_msg.take() {
            self.try_send(msg)?;
        }

        if !self.buffer.is_empty() {
            let mut batch = Vec::with_capacity(self.buffer.capacity());
            std::mem::swap(&mut self.buffer, &mut batch);
            let message = NetworkMessage::new_batch(batch, self.coord);
            self.try_send(message)
        } else {
            Ok(())
        }
    }

    /// Flush the internal buffer if it's not empty.
    pub(crate) fn try_flush(&mut self) -> Result<(), FlushError> {
        // log::warn!("Requesting flush. pending: {}\tbuf: {}", self.pending_msg.is_some(), self.buffer.len());
        if let Some(msg) = self.pending_msg.take() {
            self.try_send(msg)?;
        }

        if !self.buffer.is_empty() {
            let mut batch = Vec::with_capacity(self.buffer.capacity());
            std::mem::swap(&mut self.buffer, &mut batch);
            let message = NetworkMessage::new_batch(batch, self.coord);
            self.try_send(message)
        } else {
            Ok(())
        }
    }

    // /// Flush the internal buffer if it's not empty.
    // pub(crate) fn flush_timeout(&mut self, timeout: Duration) -> Result<(), FlushError> {
    //     // log::warn!("Requesting flush. pending: {}\tbuf: {}", self.pending_msg.is_some(), self.buffer.len());
    //     if let Some(msg) = self.pending_msg.take() {
    //         self.try_send_timeout(msg, timeout)?;
    //     }

    //     if !self.buffer.is_empty() {
    //         let mut batch = Vec::with_capacity(self.buffer.capacity());
    //         std::mem::swap(&mut self.buffer, &mut batch);
    //         let message = NetworkMessage::new_batch(batch, self.coord);
    //         self.try_send_timeout(message, timeout)
    //     } else {
    //         Ok(())
    //     }
    // }

    /// Tell the batcher that the stream is ended, flush all the remaining messages.
    pub(crate) fn end(self) {
        // Send the remaining messages
        if !self.buffer.is_empty() {
            let message = NetworkMessage::new_batch(self.buffer, self.coord);
            self.remote_sender.send(message).unwrap();
        }
    }

    pub fn coord(&self) -> Coord {
        self.coord
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
