use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{Sink, SinkExt};
use nanorand::Rng;
use tokio_util::sync::ReusableBoxFuture;

use crate::channel::{channel, Receiver, SelectResult, Sender, PollSender, SendError};
use crate::network::{NetworkMessage, ReceiverEndpoint};
use crate::network::multiplexer::MultiplexingSender;
use crate::operator::ExchangeData;
use crate::profiler::{get_profiler, Profiler};

pub(crate) fn local_channel<In: ExchangeData>(receiver_endpoint: ReceiverEndpoint, capacity: usize) -> (NetworkSender<In>, NetworkReceiver<In>) {
    let (sender, receiver) = channel(capacity);

    let sender = NetworkSender::local(receiver_endpoint, sender);

    let receiver = NetworkReceiver {
        receiver_endpoint,
        receiver,
    };

    (sender, receiver)
}

pub(crate) fn remote_channel<In: ExchangeData>(receiver_endpoint: ReceiverEndpoint, sender: MultiplexingSender<In>) -> NetworkSender<In> {
    let sender = NetworkSender::remote(receiver_endpoint, sender);

    sender
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
    Local(Sender<NetworkMessage<Out>>),
    /// The channel is remote, use the multiplexer.
    Remote(MultiplexingSender<Out>),
}

impl<Out: ExchangeData> NetworkSender<Out> {
    /// Create a new local sender that sends the data directly to the recipient.
    fn local(receiver_endpoint: ReceiverEndpoint, sender: Sender<NetworkMessage<Out>>) -> Self {
        Self {
            receiver_endpoint,
            sender: NetworkSenderImpl::Local(sender),
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
            NetworkSenderImpl::Local(inner) => Some(inner.clone()),
            NetworkSenderImpl::Remote(_) => None,
        }
    }

    pub async fn send(&mut self, msg: NetworkMessage<Out>) -> Result<(), NetworkSendError<Out>> {
        match &mut self.sender {
            NetworkSenderImpl::Local(sender) => {
                sender.send(msg).await?;
            }
            NetworkSenderImpl::Remote(_) => todo!(),
        }
        Ok(())
    }
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
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct NetworkReceiver<In: ExchangeData> {
    /// The ReceiverEndpoint of the current receiver.
    pub receiver_endpoint: ReceiverEndpoint,
    /// The actual receiver where the users of this struct will wait upon.
    #[derivative(Debug = "ignore")]
    receiver: Receiver<NetworkMessage<In>>,
}

impl<Out: ExchangeData> NetworkReceiver<Out> {
    #[tracing::instrument(name = "net_chan_rx_poll", skip_all, level="trace")]
    pub async fn recv(&mut self) -> Option<NetworkMessage<Out>> {
        match self.receiver.recv().await {
            Some(msg) => {
                // get_profiler().items_in(
                //     msg.sender,
                //     self.receiver_endpoint.coord,
                //     msg.num_items(),
                // );
                Some(msg)
            }
            None => 
            {
                tracing::error!("netchan {} received None!", self.receiver_endpoint);
                None
            }
        }
    }

    pub fn blocking_recv_one(&mut self) -> Option<NetworkMessage<Out>> {
        log::warn!("RECEIVING FROM BLOCKING START BLOCK, SWITCH TO POLLING IMPL PLS");
        self.receiver.blocking_recv()
    }

    /// Receive a message from any sender of this receiver of the other provided receiver.
    ///
    /// The first message of the two is returned. If both receivers are ready one of them is chosen
    /// randomly (with an unspecified probability). It's guaranteed this function has the eventual
    /// fairness property.
    pub fn blocking_select<In2: ExchangeData>(
        &mut self,
        other: &mut NetworkReceiver<In2>,
    ) -> SelectResult<NetworkMessage<Out>, NetworkMessage<In2>> {
        log::error!("SELECT RECV FROM BLOCKING START BLOCK, SWITCH TO POLLING IMPL PLS");

        // TODO: uniform rand usages
        if nanorand::tls_rng().generate_range(0..2u8) > 0 {
            SelectResult::A(self.blocking_recv_one())
        } else {
            SelectResult::B(other.blocking_recv_one())
        }
    }

        /// Receive a message from any sender of this receiver of the other provided receiver.
    ///
    /// The first message of the two is returned. If both receivers are ready one of them is chosen
    /// randomly (with an unspecified probability). It's guaranteed this function has the eventual
    /// fairness property.
    pub async fn poll_select<In2: ExchangeData>(
        self: Pin<&mut Self>,
        other: Pin<&mut NetworkReceiver<In2>>,
        cx: &mut Context<'_>
    ) -> Poll<SelectResult<NetworkMessage<Out>, NetworkMessage<In2>>> {

        todo!();
        // TODO: uniform rand usages
        // if nanorand::tls_rng().generate_range(0..2u8) > 0 {
        //     if let Poll::Ready(msg) = self.poll_recv(cx) {
        //         return Poll::Ready(SelectResult::A(msg));
        //     }
        //     if let Poll::Ready(msg) = other.poll_recv(cx) {
        //         return Poll::Ready(SelectResult::B(msg));
        //     }
        //     Poll::Pending
        // } else {
        //     if let Poll::Ready(msg) = other.poll_recv(cx) {
        //         return Poll::Ready(SelectResult::B(msg));
        //     }
        //     if let Poll::Ready(msg) = self.poll_recv(cx) {
        //         return Poll::Ready(SelectResult::A(msg));
        //     }
        //     Poll::Pending
        // }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NetworkSendError<T> {
    #[error("Local send error")]
    Local(#[from] SendError<NetworkMessage<T>>),
}