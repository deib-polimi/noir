use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::any::TypeId;
use std::sync::Arc;
use std::thread::available_parallelism;

use crate::block::Block;
use crate::config::{EnvironmentConfig, ExecutionRuntime, RemoteRuntimeConfig};
use crate::operator::iteration::IterationStateLock;
use crate::operator::source::Source;
use crate::operator::{Data, Operator};
#[cfg(feature = "ssh")]
use crate::runner::spawn_remote_workers;
use crate::scheduler::{BlockId, Scheduler};
use crate::stream::Stream;
use crate::{BatchMode, CoordUInt};

static LAST_REMOTE_CONFIG: Lazy<Mutex<Option<RemoteRuntimeConfig>>> =
    Lazy::new(|| Mutex::new(None));

/// Actual content of the StreamEnvironment. This is stored inside a `Rc` and it's shared among all
/// the blocks.
pub(crate) struct StreamEnvironmentInner {
    /// The configuration of the environment.
    pub(crate) config: EnvironmentConfig,
    /// The number of blocks in the job graph, it's used to assign new ids to the blocks.
    block_count: CoordUInt,
    /// The scheduler that will start the computation. It's an option because it will be moved out
    /// of this struct when the computation starts.
    scheduler: Option<Scheduler>,
}

/// Streaming environment from which it's possible to register new streams and start the
/// computation.
///
/// This is the entrypoint for the library: construct an environment providing an
/// [`EnvironmentConfig`], then you can ask new streams providing the source from where to read from.
///
/// If you want to use a distributed environment (i.e. using remote workers) you have to spawn them
/// using [`spawn_remote_workers`](StreamEnvironment::spawn_remote_workers) before asking for some stream.
///
/// When all the stream have been registered you have to call [`execute`](StreamEnvironment::execute_blocking) that will consume the
/// environment and start the computation. This function will return when the computation ends.
///
/// TODO: example usage
pub struct StreamEnvironment {
    /// Reference to the actual content of the environment.
    inner: Arc<Mutex<StreamEnvironmentInner>>,
}

impl Default for StreamEnvironment {
    fn default() -> Self {
        Self::new(EnvironmentConfig::local(
            available_parallelism().map(|q| q.get()).unwrap_or(1) as u64,
        ))
    }
}

impl StreamEnvironment {
    /// Construct a new environment from the config.
    pub fn new(config: EnvironmentConfig) -> Self {
        debug!("new environment");
        if !config.skip_single_remote_check {
            Self::single_remote_environment_check(&config);
        }
        StreamEnvironment {
            inner: Arc::new(Mutex::new(StreamEnvironmentInner::new(config))),
        }
    }

    /// Construct a new stream bound to this environment starting with the specified source.
    pub fn stream<S>(&mut self, source: S) -> Stream<S>
    where
        S: Source + Send + 'static,
    {
        let inner = self.inner.lock();
        let config = &inner.config;
        if config.host_id.is_none() {
            match config.runtime {
                ExecutionRuntime::Remote(_) => {
                    panic!(
                        "Call StreamEnvironment::spawn_remote_workers() before calling ::stream()"
                    );
                }
                ExecutionRuntime::Local(_) => {
                    unreachable!("Local environments do not need an host_id");
                }
            }
        }
        drop(inner);
        StreamEnvironmentInner::stream(self.inner.clone(), source)
    }

    /// Spawn the remote workers via SSH and exit if this is the process that should spawn. If this
    /// is already a spawned process nothing is done.
    pub fn spawn_remote_workers(&self) {
        match &self.inner.lock().config.runtime {
            ExecutionRuntime::Local(_) => {}
            #[cfg(feature = "ssh")]
            ExecutionRuntime::Remote(remote) => {
                spawn_remote_workers(remote.clone());
            }
            #[cfg(not(feature = "ssh"))]
            ExecutionRuntime::Remote(_) => {
                panic!("spawn_remote_workers() requires the `ssh` feature for remote configs.");
            }
        }
    }

    /// Start the computation. Await on the returned future to actually start the computation.
    #[cfg(feature = "async-tokio")]
    pub async fn execute(self) {
        let mut env = self.inner.lock();
        info!("starting execution ({} blocks)", env.block_count);
        let scheduler = env.scheduler.take().unwrap();
        let block_count = env.block_count;
        drop(env);
        scheduler.start(block_count).await;
        info!("finished execution");
    }

    /// Start the computation. Blocks until the computation is complete.
    ///
    /// Execute on a thread or use the async version [`execute`]
    /// for non-blocking alternatives
    pub fn execute_blocking(self) {
        let mut env = self.inner.lock();
        info!("starting execution ({} blocks)", env.block_count);
        let scheduler = env.scheduler.take().unwrap();
        scheduler.start_blocking(env.block_count);
        info!("finished execution");
    }

    /// Start the computation. Blocks until the computation is complete.
    ///
    /// This version of execute is interactive, meaning that it will not consume
    /// the environment. This allows for multiple executions of the same environment.
    pub fn execute_blocking_interactive(&self) {
        let mut env = self.inner.lock();
        info!("starting execution ({} blocks)", env.block_count);
        let scheduler = env.scheduler.take().unwrap();
        scheduler.start_blocking(env.block_count);
        env.reset();
        info!("finished execution");
    }

    /// Get the total number of processing cores in the cluster.
    pub fn parallelism(&self) -> CoordUInt {
        match &self.inner.lock().config.runtime {
            ExecutionRuntime::Local(local) => local.num_cores,
            ExecutionRuntime::Remote(remote) => remote.hosts.iter().map(|h| h.num_cores).sum(),
        }
    }

    /// Make sure that, if an environment with a remote configuration is built, all the following
    /// remote environments use the same config.
    fn single_remote_environment_check(config: &EnvironmentConfig) {
        if let ExecutionRuntime::Remote(config) = &config.runtime {
            let mut prev = LAST_REMOTE_CONFIG.lock();
            match &*prev {
                Some(prev) => {
                    if prev != config {
                        panic!("Spawning remote runtimes with different configurations is not supported");
                    }
                }
                None => {
                    *prev = Some(config.clone());
                }
            }
        }
    }
}

impl StreamEnvironmentInner {
    fn new(config: EnvironmentConfig) -> Self {
        Self {
            config: config.clone(),
            block_count: 0,
            scheduler: Some(Scheduler::new(config)),
        }
    }

    /// Reset the environment to its initial state by dropping the scheduler and creating a new
    /// one and resetting the block count
    /// This is used to allow multiple executions of the same environment.
    fn reset(&mut self) {
        self.block_count = 0;
        self.scheduler = Some(Scheduler::new(self.config.clone()));
    }

    pub fn stream<S>(env_rc: Arc<Mutex<StreamEnvironmentInner>>, source: S) -> Stream<S>
    where
        S: Source + Send + 'static,
    {
        let mut env = env_rc.lock();
        if matches!(env.config.runtime, ExecutionRuntime::Remote(_)) {
            // calling .spawn_remote_workers() will exit so it wont reach this point
            if env.config.host_id.is_none() {
                panic!("Call `StreamEnvironment::spawn_remote_workers` before calling stream!");
            }
        }

        let source_replication = source.replication();
        let mut block = env.new_block(source, Default::default(), Default::default());

        block.scheduler_requirements.replication(source_replication);
        drop(env);
        Stream { block, env: env_rc }
    }

    pub(crate) fn new_block<S: Source>(
        &mut self,
        source: S,
        batch_mode: BatchMode,
        iteration_ctx: Vec<Arc<IterationStateLock>>,
    ) -> Block<S> {
        let new_id = self.new_block_id();
        let parallelism = source.replication();
        info!("new block (b{new_id:02}), replication {parallelism:?}",);
        Block::new(new_id, source, batch_mode, iteration_ctx)
    }

    pub(crate) fn close_block<Out: Data, Op: Operator<Out = Out> + 'static>(
        &mut self,
        block: Block<Op>,
    ) -> BlockId {
        let id = block.id;
        let scheduler = self.scheduler_mut();
        scheduler.schedule_block(block);
        id
    }

    pub(crate) fn connect_blocks<Out: 'static>(&mut self, from: BlockId, to: BlockId) {
        let scheduler = self.scheduler_mut();
        scheduler.connect_blocks(from, to, TypeId::of::<Out>());
    }

    /// Allocate a new BlockId inside the environment.
    pub(crate) fn new_block_id(&mut self) -> BlockId {
        let new_id = self.block_count;
        self.block_count += 1;
        debug!("new block_id (b{new_id:02})");
        new_id
    }

    /// Return a mutable reference to the scheduler. This method will panic if the computation has
    /// already been started.
    pub(crate) fn scheduler_mut(&mut self) -> &mut Scheduler {
        self.scheduler
            .as_mut()
            .expect("The environment has already been started, cannot access the scheduler")
    }
}
