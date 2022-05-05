//! Wrapper to in-memory channels.
//!
//! This module exists to ease the transition between channel libraries.
#![allow(dead_code)]


pub trait ChannelItem: Send + 'static {}
impl<T: Send + 'static> ChannelItem for T {}

pub type SendError<T> = flume::SendError<T>;
pub type RecvError = flume::TryRecvError;
pub type RecvTimeoutError = flume::TryRecvError;
pub type TryRecvError = flume::TryRecvError;
pub type TrySendError<T> = flume::TrySendError<T>;

pub type PollSender<T> = tokio_util::sync::PollSender<T>;
pub type PollSendError<T> = tokio_util::sync::PollSendError<T>;

pub fn channel<T>(size: usize) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = flume::bounded(size);
    (Sender(tx), Receiver(rx))
}

pub fn unbounded_channel<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let (tx, rx) = flume::unbounded();
    (UnboundedSender(tx), UnboundedReceiver(rx))
}

#[derive(Debug, Clone)]
pub struct Sender<T>(flume::Sender<T>);
#[derive(Debug, Clone)]
pub struct Receiver<T>(flume::Receiver<T>);
#[derive(Debug, Clone)]
pub struct UnboundedSender<T>(flume::Sender<T>);
#[derive(Debug, Clone)]
pub struct UnboundedReceiver<T>(flume::Receiver<T>);

/// An _either_ type with the result of a select on 2 channels.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SelectResult<In1, In2> {
    /// The result refers to the first selected channel.
    A(Option<In1>),
    /// The result refers to the second selected channel.
    B(Option<In2>),
}


impl<T> Sender<T> {
    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.0.send_async(value).await
    }
    pub fn blocking_send(&self, value: T) -> Result<(), SendError<T>> {
        self.0.send(value)
    }
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        self.0.try_send(value)
    }
}

impl<T> Receiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        self.0.recv_async().await.ok()
    }
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.0.try_recv()
    }
    pub fn blocking_recv(&mut self) -> Option<T> {
        self.0.recv().ok()
    }
}

impl<T> UnboundedSender<T> {
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.0.send(value)
    }
}

impl<T> UnboundedReceiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        self.0.recv_async().await.ok()
    }
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.0.try_recv()
    }
    pub fn blocking_recv(&mut self) -> Option<T> {
        self.0.recv().ok()
    }
}
