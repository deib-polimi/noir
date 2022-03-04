use std::cell::RefCell;
use std::pin::Pin;

use crate::block::{BlockStructure, InnerBlock};
use crate::channel::{BoundedChannelReceiver, BoundedChannelSender, UnboundedChannelSender};
use crate::network::Coord;
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::{ExecutionMetadata, StartHandle};

thread_local! {
    /// Coordinates of the replica the current worker thread is working on.
    ///
    /// Access to this by calling `replica_coord()`.
    static COORD: RefCell<Option<Coord>> = RefCell::new(None);
}

/// Get the coord of the replica the current thread is working on.
///
/// This will return `Some(coord)` only when called from a worker thread of a replica, otherwise
/// `None` is returned.
pub fn replica_coord() -> Option<Coord> {
    COORD.with(|x| *x.borrow())
}

/// Call a function if this struct goes out of scope without calling `defuse`, including during a
/// panic stack-unwinding.
struct CatchPanic<F: FnOnce()> {
    /// True if the function should be called.
    primed: bool,
    /// Function to call.
    ///
    /// The `Drop` implementation will move out the function.
    handler: Option<F>,
}

impl<F: FnOnce()> CatchPanic<F> {
    fn new(handler: F) -> Self {
        Self {
            primed: true,
            handler: Some(handler),
        }
    }

    /// Avoid calling the function on drop.
    fn defuse(&mut self) {
        self.primed = false;
    }
}

impl<F: FnOnce()> Drop for CatchPanic<F> {
    fn drop(&mut self) {
        if self.primed {
            (self.handler.take().unwrap())();
        }
    }
}

struct BlockThunkInner<Out, Op>
where
    Out: Data,
    Op: Operator<Out>,
{
    block: InnerBlock<Out, Op>,
    metadata: ExecutionMetadata,
    tx_end: BoundedChannelSender<()>,
}

struct BlockThunk<Out, Op>(Box<BlockThunkInner<Out, Op>>)
where
    Out: Data,
    Op: Operator<Out>;

impl<Out, Op> BlockThunk<Out, Op>
where
    Out: Data,
    Op: Operator<Out>,
{
    pub fn new(
        block: InnerBlock<Out, Op>,
        metadata: ExecutionMetadata,
        tx_end: BoundedChannelSender<()>,
    ) -> Self {
        let inner = BlockThunkInner {
            block,
            metadata,
            tx_end,
        };

        Self(Box::new(inner))
    }

    pub fn next(&mut self) -> StreamElement<Out> {
        self.0.block.operators.next()
    }

    pub fn metadata(&self) -> &ExecutionMetadata {
        &self.0.metadata
    }

    pub fn end(self) {
        self.0.tx_end.send(()).unwrap()
    }
}

// pub(crate) fn spawn_worker<Out: Data, OperatorChain>(
//     s: &rayon::ScopeFifo,
//     block: InnerBlock<Out, OperatorChain>,
//     structure_sender: UnboundedChannelSender<(Coord, BlockStructure)>,
// ) -> StartHandle
// where
//     OperatorChain: Operator<Out> + 'static,
// {
//     let (tx_start, rx_start) = BoundedChannelReceiver::new(0);
//     let (tx_end, rx_end) = BoundedChannelReceiver::new(1);
//     debug!("Creating start handle for block {}", block.id);

//     s.spawn_fifo(move |s| wait_then_run(s, block, rx_start, tx_end, structure_sender));

//     StartHandle::new(tx_start, rx_end)
// }

pub(crate) fn spawn_scoped_worker<Out: Data, OperatorChain>(
    s: &rayon::ScopeFifo,
    mut block: InnerBlock<Out, OperatorChain>,
    metadata: ExecutionMetadata,
) -> (StartHandle, BlockStructure)
where
    OperatorChain: Operator<Out> + 'static,
{
    let (tx_start, rx_start) = BoundedChannelReceiver::new(0);
    let (tx_end, rx_end) = BoundedChannelReceiver::new(1);
    debug!("Creating start handle for block {}", block.id);

    COORD.with(|x| *x.borrow_mut() = Some(metadata.coord));
    block.operators.setup(metadata.clone());

    let structure = block.operators.structure();


    info!(
        "Starting worker for {}: {}",
        metadata.coord,
        block.to_string(),
    );

    let thunk = BlockThunk::new(block, metadata, tx_end);

    s.spawn_fifo(move |s| run(s, thunk));

    (StartHandle::new(tx_start, rx_end), structure)
}

// fn wait_then_run<Out: Data, OperatorChain>(
//     s: &rayon::ScopeFifo,
//     mut block: InnerBlock<Out, OperatorChain>,
//     rx_start: BoundedChannelReceiver<ExecutionMetadata>,
//     tx_end: BoundedChannelSender<()>,
//     structure_sender: UnboundedChannelSender<(Coord, BlockStructure)>,
// ) where
//     OperatorChain: Operator<Out> + 'static,
// {
//     debug!("Waiting for metadata, block {}", block.id);
//     let metadata = rx_start.recv().unwrap();
//     drop(rx_start);

//     debug!("Received metadata for {}", metadata.coord);

//     // remember in the thread-local the coordinate of this block
//     COORD.with(|x| *x.borrow_mut() = Some(metadata.coord));
//     // notify the operators that we are about to start
//     block.operators.setup(metadata.clone());

//     debug!("Setup done for {}", metadata.coord);

//     let structure = block.operators.structure();
//     structure_sender.send((metadata.coord, structure)).unwrap();
//     drop(structure_sender);

//     info!(
//         "Starting worker for {}: {}",
//         metadata.coord,
//         block.to_string(),
//     );
//     s.spawn_fifo(move |s| run(s, block, metadata, tx_end));
// }

fn run<Out: Data, OperatorChain>(
    s: &rayon::ScopeFifo,
    mut thunk: BlockThunk<Out, OperatorChain>
) where
    OperatorChain: Operator<Out> + 'static,
{
    // log::trace!("Running {}", metadata.coord);
    // let mut catch_panic = CatchPanic::new(move || {
    //     error!("Worker {} has crashed!", thunk.metadata().coord);
    // });
    loop {
        match thunk.next() {
            StreamElement::Terminate => {
                thunk.end();
                break;
            }
            StreamElement::Yield => {
                s.spawn_fifo(move |s| run(s, thunk));
                break;
            }
            _ => {} // Nothing to do
        }
    }
    // catch_panic.defuse();
}

// fn worker<Out: Data, OperatorChain>(
//     mut block: InnerBlock<Out, OperatorChain>,
//     metadata_receiver: BoundedChannelReceiver<ExecutionMetadata>,
//     structure_sender: UnboundedChannelSender<(Coord, BlockStructure)>,
// ) where
//     OperatorChain: Operator<Out> + 'static,
// {
//     let metadata = metadata_receiver.recv().unwrap();
//     drop(metadata_receiver);
//     info!(
//         "Starting worker for {}: {}",
//         metadata.coord,
//         block.to_string(),
//     );
//     // remember in the thread-local the coordinate of this block
//     COORD.with(|x| *x.borrow_mut() = Some(metadata.coord));
//     // notify the operators that we are about to start
//     block.operators.setup(metadata.clone());

//     let structure = block.operators.structure();
//     structure_sender.send((metadata.coord, structure)).unwrap();
//     drop(structure_sender);

//     let mut catch_panic = CatchPanic::new(|| {
//         error!("Worker {} has crashed!", metadata.coord);
//     });
//     while !matches!(block.operators.next(), StreamElement::Terminate) {
//         // nothing to do
//     }
//     catch_panic.defuse();
//     info!("Worker {} completed, exiting", metadata.coord);
// }
