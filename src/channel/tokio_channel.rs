//! Wrapper to in-memory channels.
//!
//! This module exists to ease the transition between channel libraries.
#![allow(dead_code)]
use tokio::sync::mpsc;

pub trait ChannelItem: Send + 'static {}
impl<T: Send + 'static> ChannelItem for T {}

pub type SendError<T> = mpsc::error::SendError<T>;
pub type RecvError = mpsc::error::TryRecvError;
pub type RecvTimeoutError = mpsc::error::TryRecvError;
pub type TryRecvError = mpsc::error::TryRecvError;
pub type TrySendError<T> = mpsc::error::TrySendError<T>;

pub type Sender<T> = mpsc::Sender<T>;
pub type Receiver<T> = mpsc::Receiver<T>;
pub type UnboundedSender<T> = mpsc::UnboundedSender<T>;
pub type UnboundedReceiver<T> = mpsc::UnboundedReceiver<T>;

pub type PollSender<T> = tokio_util::sync::PollSender<T>;
pub type PollSendError<T> = tokio_util::sync::PollSendError<T>;

pub use mpsc::channel;
pub use mpsc::unbounded_channel;


/// An _either_ type with the result of a select on 2 channels.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SelectResult<In1, In2> {
    /// The result refers to the first selected channel.
    A(Option<In1>),
    /// The result refers to the second selected channel.
    B(Option<In2>),
}



#[cfg(test)]
mod tests {
    use std::time::Duration;

    use itertools::Itertools;

    use crate::channel::{Receiver, SelectResult};

    const CHANNEL_CAPACITY: usize = 10;

    #[test]
    fn test_recv_local() {
        let (sender, receiver) = Receiver::new(CHANNEL_CAPACITY);

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
        let (sender, receiver) = Receiver::new(CHANNEL_CAPACITY);

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
        let (sender1, receiver1) = Receiver::new(CHANNEL_CAPACITY);
        let (sender2, receiver2) = Receiver::new(CHANNEL_CAPACITY);

        sender1.send(123).unwrap();

        let elem1 = receiver1.select(&receiver2);
        assert_eq!(elem1, SelectResult::A(Ok(123)));

        sender2.send("test".to_string()).unwrap();

        let elem2 = receiver1.select(&receiver2);
        assert_eq!(elem2, SelectResult::B(Ok("test".to_string())));
    }

    #[test]
    fn test_select_timeout_local() {
        let (sender1, receiver1) = Receiver::new(CHANNEL_CAPACITY);
        let (sender2, receiver2) = Receiver::new(CHANNEL_CAPACITY);

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
            let (sender1, receiver1) = Receiver::new(CHANNEL_CAPACITY);
            let (sender2, receiver2) = Receiver::new(CHANNEL_CAPACITY);

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
