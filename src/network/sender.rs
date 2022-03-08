use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::{Sink, SinkExt};

use crate::channel::{PollSender, Sender};
use crate::network::multiplexer::MultiplexingSender;
use crate::network::{NetworkMessage, ReceiverEndpoint};
use crate::operator::ExchangeData;
use crate::profiler::{get_profiler, Profiler};

/// The sender part of a connection between two replicas.
///
/// This works for both a local in-memory connection and for a remote socket connection. When this
/// is bound to a local channel the receiver will be a `Receiver`. When it's bound to a remote
/// connection internally this points to the multiplexer that handles the remote channel.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub(crate) struct NetworkSender<Out: ExchangeData> {
    /// The ReceiverEndpoint of the recipient.
    pub receiver_endpoint: ReceiverEndpoint,
    /// The generic sender that will send the message either locally or remotely.
    #[derivative(Debug = "ignore")]
    sender: NetworkSenderImpl<Out>,
}

/// The internal sender that sends either to a local in-memory channel, or to a remote channel using
/// a multiplexer.
#[derive(Clone)]
pub(crate) enum NetworkSenderImpl<Out: ExchangeData> {
    /// The channel is local, use an in-memory channel.
    Local(PollSender<NetworkMessage<Out>>),
    /// The channel is remote, use the multiplexer.
    Remote(MultiplexingSender<Out>),
}

impl<Out: ExchangeData> NetworkSender<Out> {
    /// Create a new local sender that sends the data directly to the recipient.
    pub fn local(
        receiver_endpoint: ReceiverEndpoint,
        sender: Sender<NetworkMessage<Out>>,
    ) -> Self {
        Self {
            receiver_endpoint,
            sender: NetworkSenderImpl::Local(PollSender::new(sender)),
        }
    }

    /// Create a new remote sender that sends the data via a multiplexer.
    pub fn remote(receiver_endpoint: ReceiverEndpoint, sender: MultiplexingSender<Out>) -> Self {
        Self {
            receiver_endpoint,
            sender: NetworkSenderImpl::Remote(sender),
        }
    }

    /// Get the inner sender if the channel is local.
    pub fn inner(&self) -> Option<&PollSender<NetworkMessage<Out>>> {
        match &self.sender {
            NetworkSenderImpl::Local(inner) => Some(inner),
            NetworkSenderImpl::Remote(_) => None,
        }
    }
}

impl<T: ExchangeData> Sink<NetworkMessage<T>> for NetworkSender<T> {
    type Error = tokio_util::sync::PollSendError<NetworkMessage<T>>;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &self.sender {
            NetworkSenderImpl::Local(sender) => sender.poll_ready_unpin(cx),
            NetworkSenderImpl::Remote(sender) => todo!(), // sender.send(self.receiver_endpoint, message),
        }
    }

    fn start_send(self: Pin<&mut Self>, item: NetworkMessage<T>) -> Result<(), Self::Error> {
        match &self.sender {
            NetworkSenderImpl::Local(sender) => sender.start_send_unpin(item),
            NetworkSenderImpl::Remote(sender) => todo!(), // sender.send(self.receiver_endpoint, message),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &self.sender {
            NetworkSenderImpl::Local(sender) => sender.poll_flush_unpin(cx),
            NetworkSenderImpl::Remote(sender) => todo!(), // sender.send(self.receiver_endpoint, message),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &self.sender {
            NetworkSenderImpl::Local(sender) => sender.poll_close_unpin(cx),
            NetworkSenderImpl::Remote(sender) => todo!(), // sender.send(self.receiver_endpoint, message),
        }
    }
}

// #[derive(thiserror::Error)]
// pub enum PollSendError<T> {
//     #[error("The channel has disconnected")]
//     Disconnected(T)
// }
