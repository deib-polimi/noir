use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;
use nanorand::Rng;
use serde::{Deserialize, Serialize};

use crate::block::{BlockStructure, OperatorReceiver, OperatorStructure};
use crate::channel::SelectResult;
use crate::network::{Coord, NetworkMessage};
use crate::operator::start::{SingleStartBlockReceiver, StartBlockReceiver};
use crate::operator::{Data, ExchangeData, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::BlockId;

// TODO: check all of this

/// This enum is an _either_ type, it contain either an element from the left part or an element
/// from the right part.
///
/// Since those two parts are merged the information about which one ends is lost, therefore there
/// are two extra variants to keep track of that.
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) enum TwoSidesItem<OutL: Data, OutR: Data> {
    /// An element of the stream on the left.
    Left(OutL),
    /// An element of the stream on the right.
    Right(OutR),
    /// The left side has ended.
    LeftEnd,
    /// The right side has ended.
    RightEnd,
}

enum ReceiverSide {
    Left,
    Right,
}

/// The actual receiver from one of the two sides.
#[pin_project::pin_project]
#[derive(Clone, Debug)]
struct SideReceiver<Out: ExchangeData, Item: ExchangeData> {
    /// The internal receiver for this side.
    #[pin]
    receiver: SingleStartBlockReceiver<Out>,
    /// The number of replicas this side has.
    num_replicas: usize,
    /// How many replicas from this side has not yet sent `StreamElement::FlushAndRestart`.
    missing_flush_and_restart: usize,
    /// How many replicas from this side has not yet sent `StreamElement::Terminate`.
    missing_terminate: usize,
    /// Whether this side is cached (i.e. after it is fully read, it's just replayed indefinitely).
    cached: bool,
    /// The content of the cache, if any.
    cache: Vec<NetworkMessage<Item>>,
    /// Whether the cache has been fully populated.
    cache_full: bool,
    /// The index of the first element to return from the cache.
    cache_pointer: usize,
}

impl<Out: ExchangeData, Item: ExchangeData> SideReceiver<Out, Item> {
    fn new(previous_block_id: BlockId, cached: bool) -> Self {
        Self {
            receiver: SingleStartBlockReceiver::new(previous_block_id),
            num_replicas: 0,
            missing_flush_and_restart: 0,
            missing_terminate: 0,
            cached,
            cache: Default::default(),
            cache_full: false,
            cache_pointer: 0,
        }
    }

    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.receiver.setup(metadata);
        self.num_replicas = self.receiver.prev_replicas().len();
        self.missing_flush_and_restart = self.num_replicas;
        self.missing_terminate = self.num_replicas;
    }

    fn poll_recv(self: Pin<&mut Self>, side: ReceiverSide, cx: &mut Context<'_>) -> Poll<Option<NetworkMessage<Out>>> {
        todo!();
        // let res = ready!(self.project().receiver.poll_recv(cx));
        
        // let sender = message.sender();
        // let data = message
        //     .into_iter()
        //     .flat_map(|item| {
        //         let mut res = Vec::new();
        //         if matches!(item, StreamElement::FlushAndRestart) {
        //             side.missing_flush_and_restart -= 1;
        //             // make sure to add this message before `FlushAndRestart`
        //             if side.missing_flush_and_restart == 0 {
        //                 res.push(StreamElement::Item(end.clone()));
        //             }
        //         }
        //         if matches!(item, StreamElement::Terminate) {
        //             side.missing_terminate -= 1;
        //         }
        //         // StreamElement::Terminate should not be put in the cache
        //         if !side.cached || !matches!(item, StreamElement::Terminate) {
        //             res.push(item.map(wrap));
        //         }
        //         res
        //     })
        //     .collect::<Vec<_>>();
        // let message = NetworkMessage::new_batch(data, sender);
        // if side.cached {
        //     side.cache.push(message.clone());
        //     // the elements are already out, ignore the cache for this round
        //     side.cache_pointer = side.cache.len();
        // }
        // message
    }
    
    fn blocking_recv_one(&mut self) -> Option<NetworkMessage<Out>> {
        self.receiver.blocking_recv_one()
    }

    fn poll_select<Out2: ExchangeData, Item2: ExchangeData>(
        lhs: Pin<&mut Self>,
        rhs: Pin<&mut SideReceiver<Out2, Item2>>,
        cx: &mut Context<'_>
    ) -> Poll<SelectResult<NetworkMessage<Out>, NetworkMessage<Out2>>> {
        // TODO: uniform rand usages
        todo!();
        // if nanorand::tls_rng().generate_range(0..2u8) > 0 {
        //     if let Poll::Ready(msg) = lhs.poll_recv(cx) {
        //         return Poll::Ready(SelectResult::A(msg));
        //     }
        //     if let Poll::Ready(msg) = rhs.poll_recv(cx) {
        //         return Poll::Ready(SelectResult::B(msg));
        //     }
        //     Poll::Pending
        // } else {
        //     if let Poll::Ready(msg) = rhs.poll_recv(cx) {
        //         return Poll::Ready(SelectResult::B(msg));
        //     }
        //     if let Poll::Ready(msg) = lhs.poll_recv(cx) {
        //         return Poll::Ready(SelectResult::A(msg));
        //     }
        //     Poll::Pending
        // }
    }

    fn reset(&mut self) {
        self.missing_flush_and_restart = self.num_replicas;
        if self.cached {
            self.cache_full = true;
            self.cache_pointer = 0;
        }
    }

    /// There is nothing more to read from this side for this iteration.
    fn is_ended(&self) -> bool {
        if self.cached {
            self.is_terminated()
        } else {
            self.missing_flush_and_restart == 0
        }
    }

    /// No more data can come from this side ever again.
    fn is_terminated(&self) -> bool {
        self.missing_terminate == 0
    }

    /// All the cached items have already been read.
    fn cache_finished(&self) -> bool {
        self.cache_pointer >= self.cache.len()
    }

    /// Get the next batch from the cache. This will panic if the cache has been entirely consumed.
    fn next_cached_item(&mut self) -> NetworkMessage<Item> {
        self.cache_pointer += 1;
        if self.cache_finished() {
            // Items are simply returned, so flush and restarts are not counted properly. Just make
            // sure that when the cache ends the counter is zero.
            self.missing_flush_and_restart = 0;
        }
        self.cache[self.cache_pointer - 1].clone()
    }
}

enum Side<L, R> {
    Left(L),
    Right(R),
}


/// This receiver is able to receive data from two previous blocks.
///
/// To do so it will first select on the two channels, and wrap each element into an enumeration
/// that discriminates the two sides.
#[pin_project::pin_project]
#[derive(Clone, Debug)]
pub(crate) struct MultipleStartBlockReceiver<OutL: ExchangeData, OutR: ExchangeData> {
    #[pin]
    left: SideReceiver<OutL, TwoSidesItem<OutL, OutR>>,
    #[pin]
    right: SideReceiver<OutR, TwoSidesItem<OutL, OutR>>,
    first_message: bool,
}

impl<OutL: ExchangeData, OutR: ExchangeData> MultipleStartBlockReceiver<OutL, OutR> {
    pub(super) fn new(
        left_block_id: BlockId,
        right_block_id: BlockId,
        left_cache: bool,
        right_cache: bool,
    ) -> Self {
        assert!(
            !(left_cache && right_cache),
            "At most one of the two sides can be cached"
        );
        Self {
            left: SideReceiver::new(left_block_id, left_cache),
            right: SideReceiver::new(right_block_id, right_cache),
            first_message: false,
        }
    }

    /// Process the incoming batch from one of the two sides.
    ///
    /// This will map all the elements of the batch into a new batch whose elements are wrapped in
    /// the variant of the correct side. Additionally this will search for
    /// `StreamElement::FlushAndRestart` messages and eventually emit the `LeftEnd`/`RightEnd`
    /// accordingly.
    fn process_side<Out: ExchangeData>(
        side: &mut SideReceiver<Out, TwoSidesItem<OutL, OutR>>,
        message: NetworkMessage<Out>,
        wrap: fn(Out) -> TwoSidesItem<OutL, OutR>,
        end: TwoSidesItem<OutL, OutR>,
    ) -> NetworkMessage<TwoSidesItem<OutL, OutR>> {
        let sender = message.sender();
        let data = message
            .into_iter()
            .flat_map(|item| {
                let mut res = Vec::new();
                if matches!(item, StreamElement::FlushAndRestart) {
                    side.missing_flush_and_restart -= 1;
                    // make sure to add this message before `FlushAndRestart`
                    if side.missing_flush_and_restart == 0 {
                        res.push(StreamElement::Item(end.clone()));
                    }
                }
                if matches!(item, StreamElement::Terminate) {
                    side.missing_terminate -= 1;
                }
                // StreamElement::Terminate should not be put in the cache
                if !side.cached || !matches!(item, StreamElement::Terminate) {
                    res.push(item.map(wrap));
                }
                res
            })
            .collect::<Vec<_>>();
        let message = NetworkMessage::new_batch(data, sender);
        if side.cached {
            side.cache.push(message.clone());
            // the elements are already out, ignore the cache for this round
            side.cache_pointer = side.cache.len();
        }
        message
    }

    /// Receive from the previous sides the next batch, or fail with a timeout if provided.
    ///
    /// This will access only the needed side (i.e. if one of the sides ended, only the other is
    /// probed). This will try to use the cache if it's available.
    fn blocking_select(
        &mut self,
    ) -> Option<NetworkMessage<TwoSidesItem<OutL, OutR>>> {
        // both sides received all the `StreamElement::Terminate`, but the cached ones have never
        // been emitted
        if self.left.is_terminated() && self.right.is_terminated() {
            let num_terminates = if self.left.cached {
                self.left.num_replicas
            } else if self.right.cached {
                self.right.num_replicas
            } else {
                0
            };
            if num_terminates > 0 {
                return Some(NetworkMessage::new_batch(
                    (0..num_terminates)
                        .map(|_| StreamElement::Terminate)
                        .collect(),
                    Default::default(),
                ));
            }
        }

        // both left and right received all the FlushAndRestart, prepare for the next iteration
        if self.left.is_ended()
            && self.right.is_ended()
            && self.left.cache_finished()
            && self.right.cache_finished()
        {
            self.left.reset();
            self.right.reset();
            self.first_message = true;
        }

        enum Side<L, R> {
            Left(L),
            Right(R),
        }

        // First message of this iteration, and there is a side with the cache:
        // we need to ask to the other side FIRST to know if this is the end of the stream or a new
        // iteration is about to start.
        let data = if self.first_message && (self.left.cached || self.right.cached) {
            debug_assert!(!self.left.cached || self.left.cache_full);
            debug_assert!(!self.right.cached || self.right.cache_full);
            self.first_message = false;
            if self.left.cached {
                Side::Right(self.right.blocking_recv_one())
            } else {
                Side::Left(self.left.blocking_recv_one())
            }
        } else if self.left.cached && self.left.cache_full && !self.left.cache_finished() {
            // The left side is cached, therefore we can access it immediately
            return Some(self.left.next_cached_item());
        } else if self.right.cached && self.right.cache_full && !self.right.cache_finished() {
            // The right side is cached, therefore we can access it immediately
            return Some(self.right.next_cached_item());
        } else if self.left.is_ended() {
            // There is nothing more to read from the left side (if cached, all the cache has
            // already been read).
            Side::Right(self.right.blocking_recv_one())
        } else if self.right.is_ended() {
            // There is nothing more to read from the right side (if cached, all the cache has
            // already been read).
            Side::Left(self.left.blocking_recv_one())
        } else {
            let left = self.left.receiver.receiver.as_mut().unwrap();
            let right = self.right.receiver.receiver.as_mut().unwrap();
            let data = left.blocking_select(right);
            match data {
                SelectResult::A(left) => {
                    Side::Left(left)
                }
                SelectResult::B(right) => {
                    Side::Right(right)
                }
            }
        };

        match data {
            Side::Left(Some(left)) => Some(Self::process_side(
                &mut self.left,
                left,
                TwoSidesItem::Left,
                TwoSidesItem::LeftEnd,
            )),
            Side::Right(Some(right)) => Some(Self::process_side(
                &mut self.right,
                right,
                TwoSidesItem::Right,
                TwoSidesItem::RightEnd,
            )),
            Side::Left(None) | Side::Right(None) => None,
        }
    }


    /// Receive from the previous sides the next batch, or fail with a timeout if provided.
    ///
    /// This will access only the needed side (i.e. if one of the sides ended, only the other is
    /// probed). This will try to use the cache if it's available.
    fn poll_select( // TODO: CHECK
        mut self: Pin<&mut Self>, cx: &mut Context<'_>
    ) -> Poll<Option<NetworkMessage<TwoSidesItem<OutL, OutR>>>> {
        todo!();
        // both sides received all the `StreamElement::Terminate`, but the cached ones have never
        // been emitted
        // let mut this = self.as_mut();

        // if this.left.is_terminated() && this.right.is_terminated() {
        //     let num_terminates = if this.left.cached {
        //         this.left.num_replicas
        //     } else if this.right.cached {
        //         this.right.num_replicas
        //     } else {
        //         0
        //     };
        //     if num_terminates > 0 {
        //         return Poll::Ready(Some(NetworkMessage::new_batch(
        //             (0..num_terminates)
        //                 .map(|_| StreamElement::Terminate)
        //                 .collect(),
        //             Default::default(),
        //         )));
        //     }
        // }

        // // both left and right received all the FlushAndRestart, prepare for the next iteration
        // if this.left.is_ended()
        //     && this.right.is_ended()
        //     && this.left.cache_finished()
        //     && this.right.cache_finished()
        // {
        //     this.left.reset();
        //     this.right.reset();
        //     this.first_message = true;
        // }

        // let mut proj = this.project();

        // // First message of this iteration, and there is a side with the cache:
        // // we need to ask to the other side FIRST to know if this is the end of the stream or a new
        // // iteration is about to start.
        // let data = if *proj.first_message && (proj.left.cached || proj.right.cached) {
        //     debug_assert!(!proj.left.cached || proj.left.cache_full);
        //     debug_assert!(!proj.right.cached || proj.right.cache_full);
        //     *proj.first_message = false;
        //     if proj.left.cached {
        //         Side::Right(ready!(proj.right.poll_recv(cx)))
        //     } else {
        //         Side::Left(ready!(proj.left.poll_recv(cx)))
        //     }
        // } else if proj.left.cached && proj.left.cache_full && !proj.left.cache_finished() {
        //     // The left side is cached, therefore we can access it immediately
        //     return Poll::Ready(Some(proj.left.next_cached_item()));
        // } else if proj.right.cached && proj.right.cache_full && !proj.right.cache_finished() {
        //     // The right side is cached, therefore we can access it immediately
        //     return Poll::Ready(Some(proj.right.next_cached_item()));
        // } else if proj.left.is_ended() {
        //     // There is nothing more to read from the left side (if cached, all the cache has
        //     // already been read).
        //         Side::Right(ready!(proj.right.poll_recv(cx)))
        // } else if proj.right.is_ended() {
        //     // There is nothing more to read from the right side (if cached, all the cache has
        //     // already been read).
        //         Side::Left(ready!(proj.left.poll_recv(cx)))
        // } else {
        //     let data = SideReceiver::poll_select(proj.left, proj.right, cx);
        //     match ready!(data) {
        //         SelectResult::A(left) => {
        //             Side::Left(left)
        //         }
        //         SelectResult::B(right) => {
        //             Side::Right(right)
        //         }
        //     }
        // };

        // Poll::Ready(match data {
        //     Side::Left(Some(left)) => Some(Self::process_side(
        //         proj.left.get_mut(),
        //         left,
        //         TwoSidesItem::Left,
        //         TwoSidesItem::LeftEnd,
        //     )),
        //     Side::Right(Some(right)) => Some(Self::process_side(
        //         proj.right.get_mut(),
        //         right,
        //         TwoSidesItem::Right,
        //         TwoSidesItem::RightEnd,
        //     )),
        //     Side::Left(None) | Side::Right(None) => None,
        // })
    }
}

impl<OutL: ExchangeData, OutR: ExchangeData> StartBlockReceiver<TwoSidesItem<OutL, OutR>>
    for MultipleStartBlockReceiver<OutL, OutR>
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.left.setup(metadata.clone());
        self.right.setup(metadata);
    }

    fn prev_replicas(&self) -> Vec<Coord> {
        let mut previous = self.left.receiver.prev_replicas();
        previous.append(&mut self.right.receiver.prev_replicas());
        previous
    }

    fn cached_replicas(&self) -> usize {
        let mut cached = 0;
        if self.left.cached {
            cached += self.left.num_replicas
        }
        if self.right.cached {
            cached += self.right.num_replicas
        }
        cached
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<TwoSidesItem<OutL, OutR>, _>("StartBlock");
        operator.receivers.push(OperatorReceiver::new::<OutL>(
            self.left.receiver.previous_block_id,
        ));
        operator.receivers.push(OperatorReceiver::new::<OutR>(
            self.right.receiver.previous_block_id,
        ));

        BlockStructure::default().add_operator(operator)
    }

    fn poll_recv(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<NetworkMessage<TwoSidesItem<OutL, OutR>>>> {
        self.poll_select(cx)
    }

    fn blocking_recv_one(&mut self) -> Option<NetworkMessage<TwoSidesItem<OutL, OutR>>> {
        self.blocking_select()
    }
}
