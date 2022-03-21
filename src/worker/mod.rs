use std::cell::RefCell;
use std::pin::Pin;

use futures::{Stream, StreamExt};
use tokio::runtime::Handle;

use crate::block::{BlockStructure, InnerBlock};
use crate::channel::{Sender, channel};
use crate::network::Coord;
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::{ExecutionMetadata, CompletionHandle};

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

#[pin_project::pin_project]
struct BlockThunkInner<Out, Op>
where
    Out: Data,
    Op: Operator<Out>,
{
    #[pin]
    block: InnerBlock<Out, Op>,
    metadata: ExecutionMetadata,
    tx_end: Sender<()>,
}

impl<Out, Op> BlockThunkInner<Out, Op>
where
    Out: Data,
    Op: Operator<Out>,
{
    pub fn new(
        block: InnerBlock<Out, Op>,
        metadata: ExecutionMetadata,
        tx_end: Sender<()>,
    ) -> Self {
        BlockThunkInner {
            block,
            metadata,
            tx_end,
        }
    }

    pub fn next(&mut self) -> StreamElement<Out> {
        log::warn!("USING BLOCKING WORKER");
        self.block.operators.next()
    }

    pub fn metadata(&self) -> &ExecutionMetadata {
        &self.metadata
    }

    pub fn tx_end(&self) -> Sender<()> {
        self.tx_end.clone()
    }
}


impl<Out, Op> Stream for BlockThunkInner<Out, Op>
where
    Out: Data,
    Op: Operator<Out> + Stream<Item=StreamElement<Out>>
{
    type Item = StreamElement<Out>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        self.project().block.project().operators.poll_next(cx)
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
) -> (CompletionHandle, BlockStructure)
where
    OperatorChain: Operator<Out> + 'static,
{
    let (tx_end, rx_end) = channel(1);
    debug!("Creating start handle for block {}", block.id);

    COORD.with(|x| *x.borrow_mut() = Some(metadata.coord));
    block.operators.setup(metadata.clone());

    let structure = block.operators.structure();


    info!(
        "Starting worker for {}: {}",
        metadata.coord,
        block.to_string(),
    );

    let thunk = BlockThunkInner::new(block, metadata, tx_end);

    s.spawn_fifo(move |s| run(s, thunk));

    (CompletionHandle::new(rx_end), structure)
}

pub(crate) fn spawn_async_worker<Out: Data, OperatorChain>(
    rt: &Handle,
    mut block: InnerBlock<Out, OperatorChain>,
    metadata: ExecutionMetadata,
) -> (CompletionHandle, BlockStructure)
where
    OperatorChain: Operator<Out> + Stream<Item=StreamElement<Out>> + 'static,
{
    let (tx_end, rx_end) = channel(1);
    debug!("Creating start handle for block {}", block.id);

    COORD.with(|x| *x.borrow_mut() = Some(metadata.coord));
    block.operators.setup(metadata.clone());

    let structure = block.operators.structure();


    info!(
        "Starting async worker for {}: {}",
        metadata.coord,
        block.to_string(),
    );

    let thunk = Box::pin(BlockThunkInner::new(block, metadata, tx_end));

    rt.spawn(run_async(thunk));

    // s.spawn_fifo(move |s| run(s, thunk));

    (CompletionHandle::new(rx_end), structure)
}

fn run<Out: Data, OperatorChain>(
    s: &rayon::ScopeFifo,
    mut thunk: BlockThunkInner<Out, OperatorChain>
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
                thunk.tx_end().blocking_send(()).unwrap();
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

async fn run_async<Out: Data, OperatorChain>(
    thunk: Pin<Box<BlockThunkInner<Out, OperatorChain>>>
) where
    OperatorChain: Operator<Out> + Stream<Item=StreamElement<Out>> + 'static,
{
    log::trace!("Async worker started {}", thunk.metadata().coord);
    // let mut catch_panic = CatchPanic::new(move || {
    //     error!("Worker {} has crashed!", thunk.metadata().coord);
    // });

    let coord = thunk.metadata().coord;
    let tx_end = thunk.tx_end();
    let cnt = thunk.fuse().count().await;
    log::info!("Stopping {} after {} events", coord, cnt);
    tx_end.send(()).await.unwrap();
}
