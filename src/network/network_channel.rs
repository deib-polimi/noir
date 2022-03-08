use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::{Sink, SinkExt};

use crate::channel::{channel, Receiver, RecvTimeoutError, SelectResult, Sender, PollSender};
use crate::network::{NetworkMessage, ReceiverEndpoint};
use crate::network::multiplexer::MultiplexingSender;
use crate::operator::ExchangeData;
use crate::profiler::{get_profiler, Profiler};

pub fn local_channel<In: ExchangeData>(receiver_endpoint: ReceiverEndpoint, capacity: usize) -> (NetworkSender<In>, NetworkReceiver<In>) {
    let (sender, receiver) = channel(capacity);

    let sender = NetworkSender::local(receiver_endpoint, sender);

    let receiver = NetworkReceiver {
        receiver_endpoint,
        receiver,
    };

    (sender, receiver)
}

pub fn remote_channel<In: ExchangeData>(receiver_endpoint: ReceiverEndpoint, sender: MultiplexingSender<In>) -> NetworkSender<In> {
    let sender = NetworkSender::remote(receiver_endpoint, sender);

    sender
}


/// The receiving end of a connection between two replicas.
///
/// This works for both a local in-memory connection and for a remote socket connection. This will
/// always be able to listen to a socket. No socket will be bound until a message is sent to the
/// starter returned by the constructor.
///
/// Internally it contains a in-memory sender-receiver pair, to get the local sender call
/// `.sender()`. When the socket will be bound an task will be spawned, it will bind the
/// socket and send to the same in-memory channel the received messages.
#[pin_project::pin_project]
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct NetworkReceiver<In: ExchangeData> {
    /// The ReceiverEndpoint of the current receiver.
    pub receiver_endpoint: ReceiverEndpoint,
    /// The actual receiver where the users of this struct will wait upon.
    #[derivative(Debug = "ignore")]
    #[pin]
    receiver: Receiver<NetworkMessage<In>>,
}

impl<In: ExchangeData> NetworkReceiver<In> {
    #[inline]
    fn profile_message(&self, message: &NetworkMessage<In>) {
        get_profiler().items_in(
            message.sender,
            self.receiver_endpoint.coord,
            message.num_items(),
        );
    }

    pub fn poll_recv(
        self: Pin<&'_ mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<NetworkMessage<In>>> {
        match self.project().receiver.poll_recv(cx) {
            Poll::Ready(Some(msg)) => {
                self.profile_message(&msg);
                Poll::Ready(Some(msg))
            }
            o => o,
        }
    }

    /// Receive a message from any sender of this receiver of the other provided receiver.
    ///
    /// The first message of the two is returned. If both receivers are ready one of them is chosen
    /// randomly (with an unspecified probability). It's guaranteed this function has the eventual
    /// fairness property.
    pub fn select<In2: ExchangeData>(
        &self,
        other: &NetworkReceiver<In2>,
    ) -> SelectResult<NetworkMessage<In>, NetworkMessage<In2>> {
        todo!();
        // self.receiver.select(&other.receiver)
    }

    /// Same as `select`, with a timeout.
    pub fn select_timeout<In2: ExchangeData>(
        &self,
        other: &NetworkReceiver<In2>,
        timeout: Duration,
    ) -> Result<SelectResult<NetworkMessage<In>, NetworkMessage<In2>>, RecvTimeoutError> {
        todo!();
        // self.receiver.select_timeout(&other.receiver, timeout)
    }
}

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
    fn local(receiver_endpoint: ReceiverEndpoint, sender: Sender<NetworkMessage<Out>>) -> Self {
        Self {
            receiver_endpoint,
            sender: NetworkSenderImpl::Local(PollSender::new(sender)),
        }
    }

    /// Create a new remote sender that sends the data via a multiplexer.
    fn remote(receiver_endpoint: ReceiverEndpoint, sender: MultiplexingSender<Out>) -> Self {
        Self {
            receiver_endpoint,
            sender: NetworkSenderImpl::Remote(sender),
        }
    }

    /// Get the inner sender if the channel is local.
    pub fn clone_inner(&self) -> Option<Sender<NetworkMessage<Out>>> {
        match &self.sender {
            NetworkSenderImpl::Local(inner) => Some(inner.get_ref().expect("Trying to clone closed inner channel").clone()),
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
