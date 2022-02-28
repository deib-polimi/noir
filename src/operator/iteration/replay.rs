use std::any::TypeId;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::block::{BlockStructure, NextStrategy, OperatorReceiver, OperatorStructure};
use crate::environment::StreamEnvironmentInner;
use crate::network::Coord;
use crate::operator::end::EndBlock;
use crate::operator::iteration::iteration_end::IterationEndBlock;
use crate::operator::iteration::leader::IterationLeader;
use crate::operator::iteration::state_handler::IterationStateHandler;
use crate::operator::iteration::{IterationStateHandle, IterationStateLock, NewIterationState};
use crate::operator::{Data, ExchangeData, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{BlockId, Stream};

/// This is the first operator of the chain of blocks inside an iteration.
///
/// If a new iteration should start, the initial dataset is replayed.
#[derive(Debug, Clone)]
pub struct Replay<Out: Data, State: ExchangeData, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    /// The coordinate of this replica.
    coord: Coord,

    /// Helper structure that manages the iteration's state.
    state: IterationStateHandler<State>,

    /// The chain of previous operators where the dataset to replay is read from.
    prev: OperatorChain,

    /// The content of the stream to replay.
    content: Vec<StreamElement<Out>>,
    /// The index inside `content` of the first message to be sent.
    content_index: usize,

    /// Whether the input stream has ended or not.
    has_input_ended: bool,
}

impl<Out: Data, State: ExchangeData, OperatorChain> Replay<Out, State, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    fn new(
        prev: OperatorChain,
        state_ref: IterationStateHandle<State>,
        leader_block_id: BlockId,
        state_lock: Arc<IterationStateLock>,
    ) -> Self {
        Self {
            // these fields will be set inside the `setup` method
            coord: Coord::new(0, 0, 0),

            prev,
            content: Default::default(),
            content_index: 0,
            has_input_ended: false,
            state: IterationStateHandler::new(leader_block_id, state_ref, state_lock),
        }
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Construct an iterative dataflow where the input stream is repeatedly fed inside a cycle,
    /// i.e. what comes into the cycle is _replayed_ at every iteration.
    ///
    /// This iteration is stateful, this means that all the replicas have a read-only access to the
    /// _iteration state_. The initial value of the state is given as parameter. When an iteration
    /// ends all the elements are reduced locally at each replica producing a `DeltaUpdate`. Those
    /// delta updates are later reduced on a single node that, using the `global_fold` function will
    /// compute the state for the next iteration. This state is also used in `loop_condition` to
    /// check whether the next iteration should start or not. `loop_condition` is also allowed to
    /// mutate the state.
    ///
    /// The initial value of `DeltaUpdate` is initialized with [`Default::default()`].
    ///
    /// The content of the loop has a new scope: it's defined by the `body` function that takes as
    /// parameter the stream of data coming inside the iteration and a reference to the state. This
    /// function should return the stream of the data that exits from the loop (that will be fed
    /// back).
    ///
    /// This construct produces a single stream with a single element: the final state of the
    /// iteration.
    ///
    /// **Note**: due to an internal limitation, it's not currently possible to add an iteration
    /// operator when the stream has limited parallelism. This means, for example, that after a
    /// non-parallel source you have to add a shuffle.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new(0..3)).shuffle();
    /// let state = s.replay(
    ///     3, // at most 3 iterations
    ///     0, // the initial state is zero
    ///     |s, state| s.map(|n| n + 10),
    ///     |delta: &mut i32, n| *delta += n,
    ///     |state, delta| *state += delta,
    ///     |_state| true,
    /// );
    /// let state = state.collect_vec();
    /// env.execute();
    ///
    /// assert_eq!(state.get().unwrap(), vec![3 * (10 + 11 + 12)]);
    /// ```
    pub fn replay<
        Body,
        DeltaUpdate: ExchangeData + Default,
        State: ExchangeData + Sync,
        OperatorChain2,
    >(
        self,
        num_iterations: usize,
        initial_state: State,
        body: Body,
        local_fold: impl Fn(&mut DeltaUpdate, Out) + Send + Clone + 'static,
        global_fold: impl Fn(&mut State, DeltaUpdate) + Send + Clone + 'static,
        loop_condition: impl Fn(&mut State) -> bool + Send + Clone + 'static,
    ) -> Stream<State, impl Operator<State>>
    where
        Body: FnOnce(
            Stream<Out, Replay<Out, State, OperatorChain>>,
            IterationStateHandle<State>,
        ) -> Stream<Out, OperatorChain2>,
        OperatorChain2: Operator<Out> + 'static,
    {
        // this is required because if the iteration block is not present on all the hosts, the ones
        // without it won't receive the state updates.
        assert!(
            self.block.scheduler_requirements.max_parallelism.is_none(),
            "Cannot have an iteration block with limited parallelism"
        );

        let state = IterationStateHandle::new(initial_state.clone());
        let state_clone = state.clone();
        let env = self.env.clone();

        // the id of the block where IterationEndBlock is. At this moment we cannot know it, so we
        // store a fake value inside this and as soon as we know it we set it to the right value.
        let feedback_block_id = Arc::new(AtomicUsize::new(0));

        let mut output_stream = StreamEnvironmentInner::stream(
            env,
            IterationLeader::new(
                initial_state,
                num_iterations,
                global_fold,
                loop_condition,
                feedback_block_id.clone(),
            ),
        );
        let leader_block_id = output_stream.block.id;
        // the output stream is outside this loop, so it doesn't have the lock for this state
        output_stream.block.iteration_state_lock_stack =
            self.block.iteration_state_lock_stack.clone();

        // the lock for synchronizing the access to the state of this iteration
        let state_lock = Arc::new(IterationStateLock::default());

        let mut iter_start =
            self.add_operator(|prev| Replay::new(prev, state, leader_block_id, state_lock.clone()));
        let replay_block_id = iter_start.block.id;

        // save the stack of the iteration for checking the stream returned by the body
        iter_start.block.iteration_state_lock_stack.push(state_lock);
        let pre_iter_stack = iter_start.block.iteration_stack();

        let mut iter_end = body(iter_start, state_clone)
            .key_by(|_| ())
            .fold(DeltaUpdate::default(), local_fold)
            .drop_key();

        let post_iter_stack = iter_end.block.iteration_stack();
        if pre_iter_stack != post_iter_stack {
            panic!("The body of the iteration should return the stream given as parameter");
        }
        iter_end.block.iteration_state_lock_stack.pop().unwrap();

        let iter_end = iter_end.add_operator(|prev| IterationEndBlock::new(prev, leader_block_id));
        let iteration_end_block_id = iter_end.block.id;

        let mut env = iter_end.env.lock();
        let scheduler = env.scheduler_mut();
        scheduler.add_block(iter_end.block);
        // connect the IterationEndBlock to the IterationLeader
        scheduler.connect_blocks(
            iteration_end_block_id,
            leader_block_id,
            TypeId::of::<DeltaUpdate>(),
        );
        scheduler.connect_blocks(
            leader_block_id,
            replay_block_id,
            TypeId::of::<NewIterationState<State>>(),
        );
        drop(env);

        // store the id of the block containing the IterationEndBlock
        feedback_block_id.store(iteration_end_block_id, Ordering::Release);

        // TODO: check parallelism and make sure the blocks are spawned on the same replicas

        // FIXME: this add_block is here just to make sure that the NextStrategy of output_stream
        //        is not changed by the following operators. This because the next strategy affects
        //        the connections made by the scheduler and if accidentally set to OnlyOne will
        //        break the connections.
        output_stream.add_block(EndBlock::new, NextStrategy::random())
    }
}

impl<Out: Data, State: ExchangeData + Sync, OperatorChain> Operator<Out>
    for Replay<Out, State, OperatorChain>
where
    OperatorChain: Operator<Out>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.coord = metadata.coord;
        self.prev.setup(metadata.clone());
        self.state.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<Out> {
        if !self.has_input_ended {
            let item = self.prev.next();

            return match &item {
                StreamElement::FlushAndRestart => {
                    debug!(
                        "Replay at {} received all the input: {} elements total",
                        self.coord,
                        self.content.len()
                    );
                    self.has_input_ended = true;
                    self.content.push(StreamElement::FlushAndRestart);
                    // the first iteration has already happened
                    self.content_index = self.content.len();
                    // since this moment accessing the state for the next iteration must wait
                    self.state.lock();
                    StreamElement::FlushAndRestart
                }
                // messages to save for the replay
                StreamElement::Item(_)
                | StreamElement::Timestamped(_, _)
                | StreamElement::Watermark(_) => {
                    self.content.push(item.clone());
                    item
                }
                // messages to forward without replaying
                StreamElement::FlushBatch => item,
                StreamElement::Yield => return StreamElement::Yield, //TODO: Check
                StreamElement::Terminate => {
                    debug!("Replay at {} is terminating", self.coord);
                    item
                }
            };
        }

        // from here the input has for sure ended, so we need to replay it...

        // this iteration has not ended yet
        if self.content_index < self.content.len() {
            let item = self.content.get(self.content_index).unwrap().clone();
            self.content_index += 1;
            if matches!(item, StreamElement::FlushAndRestart) {
                // since this moment accessing the state for the next iteration must wait
                self.state.lock();
            }
            return item;
        }

        debug!("Replay at {} has ended the iteration", self.coord);

        self.content_index = 0;

        // this iteration has ended, wait here for the leader
        let should_continue = self.state.wait_leader();

        // the loop has ended
        if !should_continue {
            debug!("Replay block at {} ended the iteration", self.coord);
            // cleanup so that if this is a nested iteration next time we'll be good to start again
            self.content.clear();
            self.has_input_ended = false;
        }

        // This iteration has ended but FlushAndRestart has already been sent. To avoid sending
        // twice the FlushAndRestart recurse.
        self.next()
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> Replay<{}>",
            self.prev.to_string(),
            std::any::type_name::<Out>()
        )
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("Replay");
        operator
            .receivers
            .push(OperatorReceiver::new::<NewIterationState<State>>(
                self.state.leader_block_id,
            ));
        self.prev.structure().add_operator(operator)
    }
}
