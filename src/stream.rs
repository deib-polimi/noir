use parking_lot::Mutex;
use std::any::TypeId;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::block::{BatchMode, InnerBlock, NextStrategy, SchedulerRequirements};
use crate::environment::StreamEnvironmentInner;
use crate::operator::end::EndBlock;
use crate::operator::iteration::IterationStateLock;
use crate::operator::window::WindowDescription;
use crate::operator::{DataKey, AsyncOperator};
use crate::operator::StartBlock;
use crate::operator::{Data, ExchangeData, KeyerFn, Operator};

/// Identifier of a block in the job graph.
pub type BlockId = usize;

/// On keyed streams, this is the type of the items of the stream.
///
/// For now it's just a type alias, maybe downstream it can become a richer struct.
pub type KeyValue<Key, Value> = (Key, Value);

/// A Stream represents a chain of operators that work on a flow of data. The type of the elements
/// that is leaving the stream is `Out`.
///
/// Internally a stream is composed by a chain of blocks, each of which can be seen as a simpler
/// stream with input and output types.
///
/// A block is internally composed of a chain of operators, nested like the `Iterator` from `std`.
/// The type of the chain inside the block is `OperatorChain` and it's required as type argument of
/// the stream. This type only represents the chain inside the last block of the stream, not all the
/// blocks inside of it.
pub struct Stream<Out: Data, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    /// The last block inside the stream.
    pub(crate) block: InnerBlock<Out, OperatorChain>,
    /// A reference to the environment this stream lives in.
    pub(crate) env: Arc<Mutex<StreamEnvironmentInner>>,
}

/// A [`KeyedStream`] is like a set of [`Stream`]s, each of which partitioned by some `Key`. Internally
/// it's just a stream whose elements are `KeyValue` pairs and the operators behave following the
/// [`KeyedStream`] semantics.
///
/// The type of the `Key` must be a valid key inside an hashmap.
pub struct KeyedStream<Key: Data, Out: Data, OperatorChain>(
    pub Stream<KeyValue<Key, Out>, OperatorChain>,
)
where
    OperatorChain: Operator<KeyValue<Key, Out>>;

/// A [`WindowedStream`] is a data stream where elements are divided in multiple groups called
/// windows. Internally, a [`WindowedStream`] is just a [`KeyedWindowedStream`] where each element is
/// assigned to the same key `()`.
///
/// These are the windows supported out-of-the-box:
///  - [`EventTimeWindow`][crate::operator::window::EventTimeWindow]
///     - [`EventTimeWindow::sliding`][crate::operator::window::EventTimeWindow::sliding]
///     - [`EventTimeWindow::tumbling`][crate::operator::window::EventTimeWindow::tumbling]
///     - [`EventTimeWindow::session`][crate::operator::window::EventTimeWindow::session]
///  - [`ProcessingTimeWindow`][crate::operator::window::ProcessingTimeWindow]
///     - [`ProcessingTimeWindow::sliding`][crate::operator::window::ProcessingTimeWindow::sliding]
///     - [`ProcessingTimeWindow::tumbling`][crate::operator::window::ProcessingTimeWindow::tumbling]
///     - [`ProcessingTimeWindow::session`][crate::operator::window::ProcessingTimeWindow::session]
///  - [`CountWindow`][crate::operator::window::CountWindow]
///     - [`CountWindow::sliding`][crate::operator::window::CountWindow::sliding]
///     - [`CountWindow::tumbling`][crate::operator::window::CountWindow::tumbling]
///
/// To apply a window to a [`Stream`], see [`Stream::window_all`].
pub struct WindowedStream<Out: Data, OperatorChain, WinOut: Data, WinDescr>
where
    OperatorChain: Operator<KeyValue<(), Out>>,
    WinDescr: WindowDescription<(), WinOut>,
{
    pub(crate) inner: KeyedWindowedStream<(), Out, OperatorChain, WinOut, WinDescr>,
}

/// A [`KeyedWindowedStream`] is a data stream partitioned by `Key`, where elements of each partition
/// are divided in groups called windows.
/// Each element can be assigned to one or multiple windows.
///
/// Windows are handled independently for each partition of the stream.
/// Each partition may be processed in parallel.
///
/// The trait [`WindowDescription`] is used to specify how windows behave, that is how elements are
/// grouped into windows.
///
/// These are the windows supported out-of-the-box:
///  - [`EventTimeWindow`][crate::operator::window::EventTimeWindow]
///     - [`EventTimeWindow::sliding`][crate::operator::window::EventTimeWindow::sliding]
///     - [`EventTimeWindow::tumbling`][crate::operator::window::EventTimeWindow::tumbling]
///     - [`EventTimeWindow::session`][crate::operator::window::EventTimeWindow::session]
///  - [`ProcessingTimeWindow`][crate::operator::window::ProcessingTimeWindow]
///     - [`ProcessingTimeWindow::sliding`][crate::operator::window::ProcessingTimeWindow::sliding]
///     - [`ProcessingTimeWindow::tumbling`][crate::operator::window::ProcessingTimeWindow::tumbling]
///     - [`ProcessingTimeWindow::session`][crate::operator::window::ProcessingTimeWindow::session]
///  - [`CountWindow`][crate::operator::window::CountWindow]
///     - [`CountWindow::sliding`][crate::operator::window::CountWindow::sliding]
///     - [`CountWindow::tumbling`][crate::operator::window::CountWindow::tumbling]
///
/// To apply a window to a [`KeyedStream`], see [`KeyedStream::window`].
pub struct KeyedWindowedStream<Key: DataKey, Out: Data, OperatorChain, WinOut: Data, WinDescr>
where
    OperatorChain: Operator<KeyValue<Key, Out>>,
    WinDescr: WindowDescription<Key, WinOut>,
{
    pub(crate) inner: KeyedStream<Key, Out, OperatorChain>,
    pub(crate) descr: WinDescr,
    pub(crate) _win_out: PhantomData<WinOut>,
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Add a new operator to the current chain inside the stream. This consumes the stream and
    /// returns a new one with the operator added.
    ///
    /// `get_operator` is a function that is given the previous chain of operators and should return
    /// the new chain of operators. The new chain cannot be simply passed as argument since it is
    /// required to do a partial move of the `InnerBlock` structure.
    ///
    /// **Note**: this is an advanced function that manipulates the block structure. Probably it is
    /// not what you are looking for.
    pub fn add_operator<NewOut: Data, Op, GetOp>(self, get_operator: GetOp) -> Stream<NewOut, Op>
    where
        Op: Operator<NewOut> + 'static,
        GetOp: FnOnce(OperatorChain) -> Op,
    {
        Stream {
            block: InnerBlock {
                id: self.block.id,
                operators: get_operator(self.block.operators),
                batch_mode: self.block.batch_mode,
                iteration_state_lock_stack: self.block.iteration_state_lock_stack,
                is_only_one_strategy: false,
                scheduler_requirements: self.block.scheduler_requirements,
                _out_type: Default::default(),
            },
            env: self.env,
        }
    }

    /// Add a new block to the stream, closing and registering the previous one. The new block is
    /// connected to the previous one.
    ///
    /// `get_end_operator` is used to extend the operator chain of the old block with the last
    /// operator (e.g. `operator::EndBlock`, `operator::GroupByEndOperator`). The end operator must
    /// be an `Operator<()>`.
    ///
    /// The new block is initialized with a `StartBlock`.
    pub(crate) fn add_block<GetEndOp, Op, IndexFn>(
        self,
        get_end_operator: GetEndOp,
        next_strategy: NextStrategy<Out, IndexFn>,
    ) -> Stream<Out, impl Operator<Out>>
    where
        IndexFn: KeyerFn<usize, Out>,
        Out: ExchangeData,
        Op: Operator<()> + 'static,
        GetEndOp: FnOnce(OperatorChain, NextStrategy<Out, IndexFn>, BatchMode) -> Op,
    {
        let batch_mode = self.block.batch_mode;
        let state_lock = self.block.iteration_state_lock_stack.clone();
        let mut old_stream =
            self.add_operator(|prev| get_end_operator(prev, next_strategy.clone(), batch_mode));
        old_stream.block.is_only_one_strategy = matches!(next_strategy, NextStrategy::OnlyOne);
        let mut env = old_stream.env.lock();
        let old_id = old_stream.block.id;
        let new_id = env.new_block_id();
        let scheduler = env.scheduler_mut();
        scheduler.add_block(old_stream.block);
        scheduler.connect_blocks(old_id, new_id, TypeId::of::<Out>());
        drop(env);

        let start_block = StartBlock::single(old_id, state_lock.last().cloned());
        Stream {
            block: InnerBlock::new(new_id, start_block, batch_mode, state_lock),
            env: old_stream.env,
        }
    }

    /// Similar to `.add_block`, but with 2 incoming blocks.
    ///
    /// This will add a new Y connection between two blocks. The two incoming blocks will be closed
    /// and a new one will be created with the 2 previous ones coming into it.
    ///
    /// This won't add any network shuffle, hence the next strategy will be `OnlyOne`. For this
    /// reason the 2 input streams must have the same parallelism, otherwise this function panics.
    ///
    /// The start operator of the new block must support multiple inputs: the provided function
    /// will be called with the ids of the 2 input blocks and should return the new start operator
    /// of the new block.
    pub(crate) fn add_y_connection<
        Out2,
        OperatorChain2,
        NewOut,
        StartOperator,
        GetStartOp,
        IndexFn1,
        IndexFn2,
    >(
        self,
        oth: Stream<Out2, OperatorChain2>,
        get_start_operator: GetStartOp,
        next_strategy1: NextStrategy<Out, IndexFn1>,
        next_strategy2: NextStrategy<Out2, IndexFn2>,
    ) -> Stream<NewOut, StartOperator>
    where
        Out: ExchangeData,
        Out2: ExchangeData,
        IndexFn1: KeyerFn<usize, Out>,
        IndexFn2: KeyerFn<usize, Out2>,
        OperatorChain2: Operator<Out2> + 'static,
        NewOut: Data,
        StartOperator: Operator<NewOut>,
        GetStartOp:
            FnOnce(BlockId, BlockId, bool, bool, Option<Arc<IterationStateLock>>) -> StartOperator,
    {
        let batch_mode = self.block.batch_mode;
        let is_only_one1 = matches!(next_strategy1, NextStrategy::OnlyOne);
        let is_only_one2 = matches!(next_strategy2, NextStrategy::OnlyOne);
        let scheduler_requirements1 = self.block.scheduler_requirements.clone();
        let scheduler_requirements2 = oth.block.scheduler_requirements.clone();
        if is_only_one1
            && is_only_one2
            && scheduler_requirements1.max_parallelism != scheduler_requirements2.max_parallelism
        {
            panic!(
                "The parallelism of the 2 blocks coming inside a Y connection must be equal. \
                On the left ({}) is {:?}, on the right ({}) is {:?}",
                self.block,
                scheduler_requirements1.max_parallelism,
                oth.block,
                scheduler_requirements2.max_parallelism
            );
        }

        let iteration_stack1 = self.block.iteration_stack();
        let iteration_stack2 = oth.block.iteration_stack();
        let (state_lock, left_cache, right_cache) = if iteration_stack1 == iteration_stack2 {
            (self.block.iteration_state_lock_stack.clone(), false, false)
        } else {
            if !iteration_stack1.is_empty() && !iteration_stack2.is_empty() {
                panic!("Side inputs are supported only if one of the streams is coming from outside any iteration");
            }
            if iteration_stack1.is_empty() {
                // self is the side input, cache it
                (oth.block.iteration_state_lock_stack.clone(), true, false)
            } else {
                // oth is the side input, cache it
                (self.block.iteration_state_lock_stack.clone(), false, true)
            }
        };

        // close previous blocks
        let mut old_stream1 =
            self.add_operator(|prev| EndBlock::new(prev, next_strategy1, batch_mode));
        let mut old_stream2 =
            oth.add_operator(|prev| EndBlock::new(prev, next_strategy2, batch_mode));
        old_stream1.block.is_only_one_strategy = is_only_one1;
        old_stream2.block.is_only_one_strategy = is_only_one2;

        let mut env = old_stream1.env.lock();
        let old_id1 = old_stream1.block.id;
        let old_id2 = old_stream2.block.id;
        let new_id = env.new_block_id();

        // add and connect the old blocks with the new one
        let scheduler = env.scheduler_mut();
        scheduler.add_block(old_stream1.block);
        scheduler.add_block(old_stream2.block);
        scheduler.connect_blocks(old_id1, new_id, TypeId::of::<Out>());
        scheduler.connect_blocks(old_id2, new_id, TypeId::of::<Out2>());
        drop(env);

        let mut new_stream = Stream {
            block: InnerBlock::new(
                new_id,
                get_start_operator(
                    old_id1,
                    old_id2,
                    left_cache,
                    right_cache,
                    state_lock.last().cloned(),
                ),
                batch_mode,
                state_lock,
            ),
            env: old_stream1.env,
        };
        // make sure the new block has the same parallelism of the previous one with OnlyOne
        // strategy
        new_stream.block.scheduler_requirements = match (is_only_one1, is_only_one2) {
            (true, _) => scheduler_requirements1,
            (_, true) => scheduler_requirements2,
            _ => SchedulerRequirements::default(),
        };
        new_stream
    }

    /// Clone the given block, taking care of connecting the new block to the same previous blocks
    /// of the original one.
    pub(crate) fn clone(&mut self) -> Self {
        let mut env = self.env.lock();
        let prev_nodes = env.scheduler_mut().prev_blocks(self.block.id).unwrap();
        let new_id = env.new_block_id();

        for (prev_node, typ) in prev_nodes.into_iter() {
            env.scheduler_mut().connect_blocks(prev_node, new_id, typ);
        }
        drop(env);

        let mut new_block = self.block.clone();
        new_block.id = new_id;
        Stream {
            block: new_block,
            env: self.env.clone(),
        }
    }

    /// Like `add_block` but without creating a new block. Therefore this closes the current stream
    /// and just add the last block to the scheduler.
    pub(crate) fn finalize_block(self) {
        let mut env = self.env.lock();
        info!("Finalizing block id={}", self.block.id);
        env.scheduler_mut().add_block(self.block);
    }
}


impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: AsyncOperator<Out> + 'static,
{

    pub fn add_async_operator<NewOut: Data, Op, GetOp>(self, get_operator: GetOp) -> Stream<NewOut, Op>
    where
        Op: AsyncOperator<NewOut> + 'static,
        GetOp: FnOnce(OperatorChain) -> Op,
    {
        Stream {
            block: InnerBlock {
                id: self.block.id,
                operators: get_operator(self.block.operators),
                batch_mode: self.block.batch_mode,
                iteration_state_lock_stack: self.block.iteration_state_lock_stack,
                is_only_one_strategy: false,
                scheduler_requirements: self.block.scheduler_requirements,
                _out_type: Default::default(),
            },
            env: self.env,
        }
    }

    pub(crate) fn add_async_block<GetEndOp, Op, IndexFn>(
        self,
        get_end_operator: GetEndOp,
        next_strategy: NextStrategy<Out, IndexFn>,
    ) -> Stream<Out, impl AsyncOperator<Out>>
    where
        IndexFn: KeyerFn<usize, Out>,
        Out: ExchangeData,
        Op: AsyncOperator<()> + 'static,
        GetEndOp: FnOnce(OperatorChain, NextStrategy<Out, IndexFn>, BatchMode) -> Op,
    {
        let batch_mode = self.block.batch_mode;
        let state_lock = self.block.iteration_state_lock_stack.clone();
        let mut old_stream =
            self.add_async_operator(|prev| get_end_operator(prev, next_strategy.clone(), batch_mode));
        old_stream.block.is_only_one_strategy = matches!(next_strategy, NextStrategy::OnlyOne);
        let mut env = old_stream.env.lock();
        let old_id = old_stream.block.id;
        let new_id = env.new_block_id();
        let scheduler = env.scheduler_mut();
        scheduler.add_async_block(old_stream.block);
        scheduler.connect_blocks(old_id, new_id, TypeId::of::<Out>());
        drop(env);

        let start_block = StartBlock::single(old_id, state_lock.last().cloned());
        Stream {
            block: InnerBlock::new(new_id, start_block, batch_mode, state_lock),
            env: old_stream.env,
        }
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    pub(crate) fn add_operator<NewOut: Data, Op, GetOp>(
        self,
        get_operator: GetOp,
    ) -> KeyedStream<Key, NewOut, Op>
    where
        Op: Operator<KeyValue<Key, NewOut>> + 'static,
        GetOp: FnOnce(OperatorChain) -> Op,
    {
        KeyedStream(self.0.add_operator(get_operator))
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: AsyncOperator<KeyValue<Key, Out>> + 'static,
{
    pub(crate) fn add_async_operator<NewOut: Data, Op, GetOp>(
        self,
        get_operator: GetOp,
    ) -> KeyedStream<Key, NewOut, Op>
    where
        Op: AsyncOperator<KeyValue<Key, NewOut>> + 'static,
        GetOp: FnOnce(OperatorChain) -> Op,
    {
        KeyedStream(self.0.add_async_operator(get_operator))
    }
}
