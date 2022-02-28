use anyhow::{anyhow, Result};

use crate::channel::{BoundedChannelSender, TrySendError};
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
    Local(BoundedChannelSender<NetworkMessage<Out>>),
    /// The channel is remote, use the multiplexer.
    Remote(MultiplexingSender<Out>),
}

impl<Out: ExchangeData> NetworkSender<Out> {
    /// Create a new local sender that sends the data directly to the recipient.
    pub fn local(
        receiver_endpoint: ReceiverEndpoint,
        sender: BoundedChannelSender<NetworkMessage<Out>>,
    ) -> Self {
        Self {
            receiver_endpoint,
            sender: NetworkSenderImpl::Local(sender),
        }
    }

    /// Create a new remote sender that sends the data via a multiplexer.
    pub fn remote(receiver_endpoint: ReceiverEndpoint, sender: MultiplexingSender<Out>) -> Self {
        Self {
            receiver_endpoint,
            sender: NetworkSenderImpl::Remote(sender),
        }
    }

    /// Send a message to a replica.
    pub fn send(&self, message: NetworkMessage<Out>) -> Result<()> {
        get_profiler().items_out(
            message.sender,
            self.receiver_endpoint.coord,
            message.num_items(),
        );
        match &self.sender {
            NetworkSenderImpl::Local(sender) => sender.send(message).map_err(|e| {
                anyhow!(
                    "Failed to send to channel to {:?}: {:?}",
                    self.receiver_endpoint,
                    e
                )
            }),
            NetworkSenderImpl::Remote(sender) => sender.send(self.receiver_endpoint, message),
        }
    }

    /// Send a message to a replica.
    pub fn try_send(
        &self,
        message: NetworkMessage<Out>,
    ) -> Result<(), TrySendError<NetworkMessage<Out>>> {
        get_profiler().items_out(
            message.sender,
            self.receiver_endpoint.coord,
            message.num_items(),
        );
        match &self.sender {
            NetworkSenderImpl::Local(sender) => sender.try_send(message),
            NetworkSenderImpl::Remote(sender) => todo!(), // sender.send(self.receiver_endpoint, message),
        }
    }

    /// Get the inner sender if the channel is local.
    pub fn inner(&self) -> Option<&BoundedChannelSender<NetworkMessage<Out>>> {
        match &self.sender {
            NetworkSenderImpl::Local(inner) => Some(inner),
            NetworkSenderImpl::Remote(_) => None,
        }
    }
}
