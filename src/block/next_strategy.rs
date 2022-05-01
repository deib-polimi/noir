use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use nanorand::{tls_rng, Rng};

use crate::network::{NetworkSender, ReceiverEndpoint};
use crate::operator::{ExchangeData, KeyerFn};
use crate::stream::BlockId;

/// The list with the interesting senders of a single block.
#[derive(Debug, Clone)]
pub(crate) struct SenderList(pub Vec<ReceiverEndpoint>);

/// The next strategy used at the end of a block.
///
/// A block in the job graph may have many next blocks. Each of them will receive the message, which
/// of their replica will receive it depends on the value of the next strategy.
#[derive(Clone, Debug)]
pub(crate) enum NextStrategy<Out: ExchangeData, IndexFn = fn(&Out) -> usize>
where
    IndexFn: KeyerFn<usize, Out>,
{
    /// Only one of the replicas will receive the message:
    ///
    /// - if the block is not replicated, the only replica will receive the message
    /// - if the next block is replicated as much as the current block the corresponding replica
    ///   will receive the message
    /// - otherwise the execution graph is malformed  
    OnlyOne,
    /// A random replica will receive the message.
    Random,
    /// Among the next replica, the one is selected based on the hash of the key of the message.
    GroupBy(IndexFn, PhantomData<Out>),
    /// Every following replica will receive every message.
    All,
}

impl<Out: ExchangeData> NextStrategy<Out> {
    /// Build a `NextStrategy` from a keyer function.
    pub(crate) fn group_by<Key: Hash, Keyer>(
        keyer: Keyer,
    ) -> NextStrategy<Out, impl KeyerFn<usize, Out>>
    where
        Keyer: KeyerFn<Key, Out>,
    {
        NextStrategy::GroupBy(
            move |item: &Out| {
                let mut s = ahash::AHasher::default();
                keyer(item).hash(&mut s);
                s.finish() as usize
            },
            Default::default(),
        )
    }

    /// Returns `NextStrategy::All` with default `IndexFn`.
    pub(crate) fn all() -> NextStrategy<Out> {
        NextStrategy::All
    }

    /// Returns `NextStrategy::OnlyOne` with default `IndexFn`.
    pub(crate) fn only_one() -> NextStrategy<Out> {
        NextStrategy::OnlyOne
    }

    /// Returns `NextStrategy::Random` with default `IndexFn`.
    pub(crate) fn random() -> NextStrategy<Out> {
        NextStrategy::Random
    }
}

impl<Out: ExchangeData, IndexFn> NextStrategy<Out, IndexFn>
where
    IndexFn: KeyerFn<usize, Out>,
{
    /// Group the senders from a block using the current next strategy.
    ///
    /// The returned value is a list of `SenderList`s, one for each next block in the execution
    /// graph. The messages will be sent to one replica of each group, according to the strategy.
    ///
    /// If `ignore_block` is specified, no senders to the specified block will be returned.
    pub fn group_senders(
        &self,
        senders: &HashMap<ReceiverEndpoint, NetworkSender<Out>>,
        ignore_block: Option<BlockId>,
    ) -> Vec<Vec<ReceiverEndpoint>> {
        // If NextStrategy is All, return every sender except the ignored ones.
        // Each sender has its own SenderList.
        if matches!(self, NextStrategy::All) {
            return senders
                .iter()
                .filter_map(|(coord, sender)| match ignore_block {
                    Some(ignore_block) if coord.coord.block_id == ignore_block => None,
                    _ => Some(vec![sender.receiver_endpoint]),
                })
                .collect();
        }

        let mut by_block_id: HashMap<_, Vec<_>, ahash::RandomState> = HashMap::default();
        for (coord, sender) in senders {
            by_block_id
                .entry(coord.coord.block_id)
                .or_default()
                .push(sender.receiver_endpoint);
        }
        if let Some(ignore_block) = ignore_block {
            by_block_id.remove(&ignore_block);
        }
        let mut senders = Vec::new();
        for (_block_id, mut block_senders) in by_block_id {
            block_senders.sort_unstable();
            if matches!(self, NextStrategy::OnlyOne) {
                assert_eq!(block_senders.len(), 1, "OnlyOne must have a single sender");
            }
            senders.push(block_senders);
        }
        senders
    }

    /// Compute the index of the replica which this message should be forwarded to.
    pub fn index(&self, message: &Out) -> usize {
        match self {
            NextStrategy::OnlyOne | NextStrategy::All => 0,
            NextStrategy::Random => tls_rng().generate(),
            NextStrategy::GroupBy(keyer, _) => keyer(message),
        }
    }
}
