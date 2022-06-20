//! Wrapper to in-memory channels.
//!
//! This module exists to ease the transition between channel libraries.
#![allow(dead_code)]

use std::time::Duration;

#[cfg(all(feature = "crossbeam", not(feature = "flume")))]
use crossbeam_channel::{
    bounded as bounded_ext, select, unbounded as unbounded_ext, Receiver as ReceiverExt,
    RecvError as ExtRecvError, RecvTimeoutError as ExtRecvTimeoutError, Select,
    SendError as SendErrorExt, Sender as SenderExt, TryRecvError as ExtTryRecvError,
};
#[cfg(all(not(feature = "crossbeam"), feature = "flume"))]
use flume::{
    bounded as bounded_ext, unbounded as unbounded_ext, Receiver as ReceiverExt,
    RecvError as ExtRecvError, RecvTimeoutError as ExtRecvTimeoutError, SendError as SendErrorExt,
    Sender as SenderExt, TryRecvError as ExtTryRecvError,
};

pub trait ChannelItem: Send + 'static {}
impl<T: Send + 'static> ChannelItem for T {}

pub type SendError<T> = SendErrorExt<T>;
pub type RecvError = ExtRecvError;
pub type RecvTimeoutError = ExtRecvTimeoutError;
pub type TryRecvError = ExtTryRecvError;

/// Crate a new pair sender/receiver with limited capacity.
pub fn bounded<T: Send + 'static>(size: usize) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = bounded_ext(size);
    (Sender(tx), Receiver(rx))
}

/// Crate a new pair sender/receiver with unlimited capacity.
pub fn unbounded<T: Send + 'static>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let (tx, rx) = unbounded_ext();
    (UnboundedSender(tx), UnboundedReceiver(rx))
}

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
pub struct Sender<T: ChannelItem>(SenderExt<T>);
/// A wrapper on a bounded channel receiver.
#[derive(Debug)]
pub struct Receiver<T: ChannelItem>(ReceiverExt<T>);

impl<T: ChannelItem> Sender<T> {
    /// Send a message in the channel, blocking if it's full.
    #[inline]
    pub fn send(&self, item: T) -> Result<(), SendError<T>> {
        self.0.send(item)
    }

    #[inline]
    pub async fn send_async(&self, item: T) -> Result<(), SendError<T>> {
        self.0.send_async(item).await
    }

}

impl<T: ChannelItem> Receiver<T> {
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

    #[inline]
    pub async fn recv_async(&self) -> Result<T, RecvError> {
        self.0.recv_async().await
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
    pub fn select<T2: ChannelItem>(&self, other: &Receiver<T2>) -> SelectResult<T, T2> {
        select_impl!(self, other)
    }

    /// Same as `select`, with a timeout.
    #[inline]
    pub fn select_timeout<T2: ChannelItem>(
        &self,
        other: &Receiver<T2>,
        timeout: Duration,
    ) -> Result<SelectResult<T, T2>, RecvTimeoutError> {
        select_timeout_impl!(self, other, timeout)
    }
}

/// A wrapper on an unbounded channel sender.
#[derive(Debug, Clone)]
pub struct UnboundedSender<T: ChannelItem>(SenderExt<T>);
/// A wrapper on an unbounded channel receiver.
#[derive(Debug)]
pub struct UnboundedReceiver<T: ChannelItem>(pub(crate) ReceiverExt<T>);

impl<T: ChannelItem> UnboundedSender<T> {
    /// Send a message in the channel.
    #[inline]
    pub fn send(&self, item: T) -> Result<(), SendError<T>> {
        self.0.send(item)
    }
}

impl<T: ChannelItem> UnboundedReceiver<T> {
    /// Block until a message is present in the channel and return it when ready.
    #[inline]
    pub fn recv(&self) -> Result<T, RecvError> {
        self.0.recv()
    }

    #[inline]
    pub async fn recv_async(&self) -> Result<T, RecvError> {
        self.0.recv_async().await
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
    pub fn select<T2: ChannelItem>(&self, other: &UnboundedReceiver<T2>) -> SelectResult<T, T2> {
        select_impl!(self, other)
    }

    /// Same as `select`, with a timeout.
    #[inline]
    pub fn select_timeout<T2: ChannelItem>(
        &self,
        other: &UnboundedReceiver<T2>,
        timeout: Duration,
    ) -> Result<SelectResult<T, T2>, RecvTimeoutError> {
        select_timeout_impl!(self, other, timeout)
    }
}

pub struct Selector<T: Send + 'static, K: Clone + Eq> {
    rxs: Vec<(K, Receiver<T>)>,
    // inner: flume::Selector<'a, Result<(K, T), RecvError>>,
}

impl<T: Send + 'static, K: Clone + Eq> Selector<T, K> {
    pub fn new(receivers: Vec<(K, Receiver<T>)>) -> Self {
        let rxs = receivers;
        Self { rxs }
    }

    pub fn recv(&mut self) -> Result<(K, T), RecvError> {
        let mut selector = flume::Selector::new();

        if self.rxs.is_empty() {
            return Err(RecvError::Disconnected);
        }

        for (k, recv) in self.rxs.iter() {
            selector = selector.recv(&recv.0, move |r| {
                r.map(|i| (k.clone(), i)).map_err(|e| (k.clone(), e))
            })
        }

        match selector.wait() {
            Ok(i) => Ok(i),
            Err((k, RecvError::Disconnected)) => {
                self.rxs.retain(|r| r.0 != k);
                self.recv()
            }
        }
    }

    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<(K, T), RecvTimeoutError> {
        let mut selector = flume::Selector::new();

        if self.rxs.is_empty() {
            return Err(RecvTimeoutError::Disconnected);
        }

        for (k, recv) in self.rxs.iter() {
            selector = selector.recv(&recv.0, move |r| {
                r.map(|i| (k.clone(), i)).map_err(|e| (k.clone(), e))
            })
        }

        match selector
            .wait_timeout(timeout)
            .map_err(|_| RecvTimeoutError::Timeout)?
        {
            Ok(i) => Ok(i),
            Err((k, RecvError::Disconnected)) => {
                self.rxs.retain(|r| r.0 != k);
                self.recv_timeout(timeout)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use itertools::Itertools;

    use crate::channel::{bounded, SelectResult};

    const TEST_CAPACITY: usize = 10;

    #[test]
    fn test_recv_local() {
        let (sender, receiver) = bounded(TEST_CAPACITY);

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
        let (sender, receiver) = bounded(TEST_CAPACITY);

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
        let (sender1, receiver1) = bounded(TEST_CAPACITY);
        let (sender2, receiver2) = bounded(TEST_CAPACITY);

        sender1.send(123).unwrap();

        let elem1 = receiver1.select(&receiver2);
        assert_eq!(elem1, SelectResult::A(Ok(123)));

        sender2.send("test".to_string()).unwrap();

        let elem2 = receiver1.select(&receiver2);
        assert_eq!(elem2, SelectResult::B(Ok("test".to_string())));
    }

    #[test]
    fn test_select_timeout_local() {
        let (sender1, receiver1) = bounded(TEST_CAPACITY);
        let (sender2, receiver2) = bounded(TEST_CAPACITY);

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
        // TEST_CAPACITY. Repeating this test enough times makes sure to avoid any fluke.
        // If TEST_CAPACITY == 10, with 100 tries the failure probability is ~3x10^-23 (the
        // single try has a failure probability of ~5x10^-6)
        let tries = 100;
        let mut failures = 0;
        for _ in 0..100 {
            let (sender1, receiver1) = bounded(TEST_CAPACITY);
            let (sender2, receiver2) = bounded(TEST_CAPACITY);

            for _ in 0..TEST_CAPACITY {
                sender1.send(1).unwrap();
                sender2.send(2).unwrap();
            }

            let mut order = Vec::new();

            for _ in 0..2 * TEST_CAPACITY {
                let elem = receiver1.select(&receiver2);
                match elem {
                    SelectResult::A(Ok(_)) => order.push(1),
                    SelectResult::B(Ok(_)) => order.push(2),
                    _ => {}
                }
            }

            let in_order1 = (0..TEST_CAPACITY)
                .map(|_| 1)
                .chain((0..TEST_CAPACITY).map(|_| 2))
                .collect_vec();
            let in_order2 = (0..TEST_CAPACITY)
                .map(|_| 2)
                .chain((0..TEST_CAPACITY).map(|_| 1))
                .collect_vec();

            if order == in_order1 || order == in_order2 {
                failures += 1;
            }
        }
        // if more than 5% of the tries failed, this test fails
        assert!(100 * failures < 5 * tries);
    }
}
