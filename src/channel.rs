//! Wrapper to in-memory channels.
//!
//! This module exists to ease the transition between channel libraries.
#![allow(dead_code)]

use std::time::Duration;

use anyhow::{anyhow, Result};

#[cfg(all(feature = "crossbeam", not(feature = "flume")))]
use crossbeam_channel::{
    bounded, select, unbounded, Receiver, RecvError as ExtRecvError,
    RecvTimeoutError as ExtRecvTimeoutError, Select, Sender, TryRecvError as ExtTryRecvError,
    TrySendError as ExtTrySendError,
};
#[cfg(all(not(feature = "crossbeam"), feature = "flume"))]
use flume::{
    bounded, unbounded, Receiver, RecvError as ExtRecvError,
    RecvTimeoutError as ExtRecvTimeoutError, Sender, TryRecvError as ExtTryRecvError,
    TrySendError as ExtTrySendError,
};

pub trait ChannelItem: Send + 'static {}
impl<T: Send + 'static> ChannelItem for T {}

pub type RecvError = ExtRecvError;
pub type RecvTimeoutError = ExtRecvTimeoutError;
pub type TryRecvError = ExtTryRecvError;
pub type TrySendError<T> = ExtTrySendError<T>;

/// An _either_ type with the result of a select on 2 channels.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SelectResult<In1, In2> {
    /// The result refers to the first selected channel.
    A(Result<In1, RecvError>),
    /// The result refers to the second selected channel.
    B(Result<In2, RecvError>),
}

#[cfg(all(feature = "crossbeam", not(feature = "flume")))]
#[macro_use]
mod select_impl {
    macro_rules! select_impl {
        ($self:expr, $other:expr) => {
            select! {
                recv($self.0) -> elem => SelectResult::A(elem),
                recv($other.0) -> elem => SelectResult::B(elem),
            }
        };
    }

    macro_rules! select_timeout_impl {
        ($self:expr, $other:expr, $timeout:expr) => {
            select! {
                recv($self.0) -> elem => Ok(SelectResult::A(elem)),
                recv($other.0) -> elem => Ok(SelectResult::B(elem)),
                default($timeout) => Err(RecvTimeoutError::Timeout),
            }
        };
    }
}

#[cfg(all(not(feature = "crossbeam"), feature = "flume"))]
#[macro_use]
mod select_impl {
    macro_rules! select_impl {
        ($self:expr, $other:expr) => {{
            flume::Selector::new()
                .recv(&$self.0, SelectResult::A)
                .recv(&$other.0, SelectResult::B)
                .wait()
        }};
    }

    macro_rules! select_timeout_impl {
        ($self:expr, $other:expr, $timeout:expr) => {
            flume::Selector::new()
                .recv(&$self.0, SelectResult::A)
                .recv(&$other.0, SelectResult::B)
                .wait_timeout($timeout)
                .map_err(|_| RecvTimeoutError::Timeout)
        };
    }
}

/// A wrapper on a bounded channel sender.
#[derive(Debug, Clone)]
pub(crate) struct BoundedChannelSender<T: ChannelItem>(Sender<T>);
/// A wrapper on a bounded channel receiver.
#[derive(Debug)]
pub(crate) struct BoundedChannelReceiver<T: ChannelItem>(Receiver<T>);

impl<T: ChannelItem> BoundedChannelSender<T> {
    /// Send a message in the channel, blocking if it's full.
    #[inline]
    pub fn send(&self, item: T) -> Result<()> {
        self.0
            .send(item)
            .map_err(|_| anyhow!("Error while sending"))
    }

    #[inline]
    pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        self.0.try_send(item)
    }
}

impl<T: ChannelItem> BoundedChannelReceiver<T> {
    /// Crate a new pair sender/receiver with limited capacity.
    pub fn new(cap: usize) -> (BoundedChannelSender<T>, BoundedChannelReceiver<T>) {
        let (sender, receiver) = bounded(cap);
        (
            BoundedChannelSender(sender),
            BoundedChannelReceiver(receiver),
        )
    }

    /// Block until a message is present in the channel and return it when ready.
    #[inline]
    pub fn recv(&self) -> Result<T, RecvError> {
        self.0.recv()
    }

    /// Like `recv`, but without blocking.
    #[inline]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.0.try_recv()
    }

    /// Block until a message is present in the channel and return it when ready.
    ///
    /// If the timeout expires an error is returned.
    #[inline]
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        self.0.recv_timeout(timeout)
    }

    /// Receive a message from any sender of this receiver of the other provided receiver.
    ///
    /// The first message of the two is returned. If both receivers are ready one of them is chosen
    /// randomly (with an unspecified probability). It's guaranteed this function has the eventual
    /// fairness property.
    #[inline]
    pub fn select<T2: ChannelItem>(
        &self,
        other: &BoundedChannelReceiver<T2>,
    ) -> SelectResult<T, T2> {
        select_impl!(self, other)
    }

    /// Same as `select`, with a timeout.
    #[inline]
    pub fn select_timeout<T2: ChannelItem>(
        &self,
        other: &BoundedChannelReceiver<T2>,
        timeout: Duration,
    ) -> Result<SelectResult<T, T2>, RecvTimeoutError> {
        select_timeout_impl!(self, other, timeout)
    }
}

/// A wrapper on an unbounded channel sender.
#[derive(Debug, Clone)]
pub(crate) struct UnboundedChannelSender<T: ChannelItem>(Sender<T>);
/// A wrapper on an unbounded channel receiver.
#[derive(Debug)]
pub(crate) struct UnboundedChannelReceiver<T: ChannelItem>(Receiver<T>);

impl<T: ChannelItem> UnboundedChannelSender<T> {
    /// Send a message in the channel.
    #[inline]
    pub fn send(&self, item: T) -> Result<()> {
        self.0
            .send(item)
            .map_err(|_| anyhow!("Error while sending"))
    }
}

impl<T: ChannelItem> UnboundedChannelReceiver<T> {
    /// Crate a new pair sender/receiver with unlimited capacity.
    pub fn new() -> (UnboundedChannelSender<T>, UnboundedChannelReceiver<T>) {
        let (sender, receiver) = unbounded();
        (
            UnboundedChannelSender(sender),
            UnboundedChannelReceiver(receiver),
        )
    }

    /// Block until a message is present in the channel and return it when ready.
    #[inline]
    pub fn recv(&self) -> Result<T, RecvError> {
        self.0.recv()
    }

    /// Block until a message is present in the channel and return it when ready.
    ///
    /// If the timeout expires an error is returned.
    #[inline]
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        self.0.recv_timeout(timeout)
    }

    /// Receive a message from any sender of this receiver of the other provided receiver.
    ///
    /// The first message of the two is returned. If both receivers are ready one of them is chosen
    /// randomly (with an unspecified probability). It's guaranteed this function has the eventual
    /// fairness property.
    #[inline]
    pub fn select<T2: ChannelItem>(
        &self,
        other: &UnboundedChannelReceiver<T2>,
    ) -> SelectResult<T, T2> {
        select_impl!(self, other)
    }

    /// Same as `select`, with a timeout.
    #[inline]
    pub fn select_timeout<T2: ChannelItem>(
        &self,
        other: &UnboundedChannelReceiver<T2>,
        timeout: Duration,
    ) -> Result<SelectResult<T, T2>, RecvTimeoutError> {
        select_timeout_impl!(self, other, timeout)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use itertools::Itertools;

    use crate::channel::{BoundedChannelReceiver, SelectResult};

    const CHANNEL_CAPACITY: usize = 10;

    #[test]
    fn test_recv_local() {
        let (sender, receiver) = BoundedChannelReceiver::new(CHANNEL_CAPACITY);

        sender.send(123).unwrap();
        sender.send(456).unwrap();

        drop(sender);

        assert_eq!(receiver.recv().unwrap(), 123);
        assert_eq!(receiver.recv().unwrap(), 456);
        // sender has dropped
        assert!(receiver.recv().is_err());
    }

    #[test]
    fn test_recv_timeout_local() {
        let (sender, receiver) = BoundedChannelReceiver::new(CHANNEL_CAPACITY);

        sender.send(123).unwrap();

        assert_eq!(
            receiver.recv_timeout(Duration::from_millis(1)).unwrap(),
            123
        );

        assert!(receiver.recv_timeout(Duration::from_millis(50)).is_err());

        sender.send(456).unwrap();
        assert_eq!(
            receiver.recv_timeout(Duration::from_millis(1)).unwrap(),
            456
        );
    }

    #[test]
    fn test_select_local() {
        let (sender1, receiver1) = BoundedChannelReceiver::new(CHANNEL_CAPACITY);
        let (sender2, receiver2) = BoundedChannelReceiver::new(CHANNEL_CAPACITY);

        sender1.send(123).unwrap();

        let elem1 = receiver1.select(&receiver2);
        assert_eq!(elem1, SelectResult::A(Ok(123)));

        sender2.send("test".to_string()).unwrap();

        let elem2 = receiver1.select(&receiver2);
        assert_eq!(elem2, SelectResult::B(Ok("test".to_string())));
    }

    #[test]
    fn test_select_timeout_local() {
        let (sender1, receiver1) = BoundedChannelReceiver::new(CHANNEL_CAPACITY);
        let (sender2, receiver2) = BoundedChannelReceiver::new(CHANNEL_CAPACITY);

        sender1.send(123).unwrap();

        let elem1 = receiver1
            .select_timeout(&receiver2, Duration::from_millis(1))
            .unwrap();
        assert_eq!(elem1, SelectResult::A(Ok(123)));

        let timeout = receiver1.select_timeout(&receiver2, Duration::from_millis(50));
        assert!(timeout.is_err());

        sender2.send("test".to_string()).unwrap();

        let elem2 = receiver1
            .select_timeout(&receiver2, Duration::from_millis(1))
            .unwrap();
        assert_eq!(elem2, SelectResult::B(Ok("test".to_string())));
    }

    /// This test checks if the `select` function selects randomly between the two channels if they
    /// are both ready. The actual distribution of probability does not really matters in practice,
    /// as long as eventually both channels are selected.
    #[test]
    fn test_select_fairness() {
        // this test has a probability of randomly failing of c!c! / (2c)! where c is
        // CHANNEL_CAPACITY. Repeating this test enough times makes sure to avoid any fluke.
        // If CHANNEL_CAPACITY == 10, with 100 tries the failure probability is ~3x10^-23 (the
        // single try has a failure probability of ~5x10^-6)
        let tries = 100;
        let mut failures = 0;
        for _ in 0..100 {
            let (sender1, receiver1) = BoundedChannelReceiver::new(CHANNEL_CAPACITY);
            let (sender2, receiver2) = BoundedChannelReceiver::new(CHANNEL_CAPACITY);

            for _ in 0..CHANNEL_CAPACITY {
                sender1.send(1).unwrap();
                sender2.send(2).unwrap();
            }

            let mut order = Vec::new();

            for _ in 0..2 * CHANNEL_CAPACITY {
                let elem = receiver1.select(&receiver2);
                match elem {
                    SelectResult::A(Ok(_)) => order.push(1),
                    SelectResult::B(Ok(_)) => order.push(2),
                    _ => {}
                }
            }

            let in_order1 = (0..CHANNEL_CAPACITY)
                .map(|_| 1)
                .chain((0..CHANNEL_CAPACITY).map(|_| 2))
                .collect_vec();
            let in_order2 = (0..CHANNEL_CAPACITY)
                .map(|_| 2)
                .chain((0..CHANNEL_CAPACITY).map(|_| 1))
                .collect_vec();

            if order == in_order1 || order == in_order2 {
                failures += 1;
            }
        }
        // if more than 5% of the tries failed, this test fails
        assert!(100 * failures < 5 * tries);
    }
}
