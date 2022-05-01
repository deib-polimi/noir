use parking_lot::Mutex;
use std::sync::Arc;

use crate::block::InnerBlock;
use crate::config::{EnvironmentConfig, ExecutionRuntime, RemoteRuntimeConfig};
use crate::operator::source::Source;
use crate::operator::Data;
use crate::profiler::Stopwatch;
use crate::runner::spawn_remote_workers;
use crate::scheduler::{Scheduler, NoirJoinHandle};
use crate::stream::{BlockId, Stream};

lazy_static! {
    static ref LAST_REMOTE_CONFIG: Mutex<Option<RemoteRuntimeConfig>> = Mutex::new(None);
}

/// Actual content of the StreamEnvironment. This is stored inside a `Rc` and it's shared among all
/// the blocks.
pub(crate) struct StreamEnvironmentInner {
    /// The configuration of the environment.
    pub(crate) config: EnvironmentConfig,
    /// The number of blocks in the job graph, it's used to assign new ids to the blocks.
    block_count: BlockId,
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
/// When all the stream have been registered you have to call [`execute`](StreamEnvironment::execute) that will consume the
/// environment and start the computation. This function will return when the computation ends.
///
/// TODO: example usage
pub struct StreamEnvironment {
    /// Reference to the actual content of the environment.
    inner: Arc<Mutex<StreamEnvironmentInner>>,
    /// Measure the time for building the graph.
    build_time: Stopwatch,
}

impl StreamEnvironment {
    /// Construct a new environment from the config.
    pub fn new(config: EnvironmentConfig) -> Self {
        info!("Constructing environment");
        if !config.skip_single_remote_check {
            Self::single_remote_environment_check(&config);
        }
        StreamEnvironment {
            inner: Arc::new(Mutex::new(StreamEnvironmentInner::new(config))),
            build_time: Stopwatch::new("build"),
        }
    }

    /// Construct a new stream bound to this environment starting with the specified source.
    pub fn stream<Out: Data, S>(&mut self, source: S) -> Stream<Out, S>
    where
        S: Source<Out> + Send + 'static,
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
            ExecutionRuntime::Remote(remote) => {
                spawn_remote_workers(remote.clone());
            }
        }
    }

    /// Start the computation. Await on the returned future to actually start the computation.
    pub fn execute(self) {
        drop(self.build_time);
        let _stopwatch = Stopwatch::new("execution");
        let mut env = self.inner.lock();
        info!("Starting execution of {} blocks", env.block_count);
        let scheduler = env.scheduler.take().unwrap();
        scheduler.start(env.block_count);
    }

    /// Start the computation. Await on the returned future to actually start the computation.
    pub fn execute_async_blocking(self) {
        drop(self.build_time);
        let _stopwatch = Stopwatch::new("execution");
        let mut env = self.inner.lock();
        info!("Starting execution of {} blocks", env.block_count);
        let scheduler = env.scheduler.take().unwrap();
        scheduler.start_async_blocking(env.block_count);
    }

    /// Start the computation. Await on the returned future to actually start the computation.
    pub fn execute_async(self, threads: usize) -> NoirJoinHandle {
        drop(self.build_time);
        let mut env = self.inner.lock();
        info!("Starting execution of {} blocks", env.block_count);
        let scheduler = env.scheduler.take().unwrap();
        scheduler.start_async(env.block_count, threads)
    }

    /// Get the total number of processing cores in the cluster.
    pub fn parallelism(&self) -> usize {
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

    pub fn stream<Out: Data, S>(
        env_rc: Arc<Mutex<StreamEnvironmentInner>>,
        source: S,
    ) -> Stream<Out, S>
    where
        S: Source<Out> + Send + 'static,
    {
        let mut env = env_rc.lock();
        if matches!(env.config.runtime, ExecutionRuntime::Remote(_)) {
            // calling .spawn_remote_workers() will exit so it wont reach this point
            if env.config.host_id.is_none() {
                panic!("Call `StreamEnvironment::spawn_remote_workers` before calling stream!");
            }
        }
        let block_id = env.new_block_id();
        let source_max_parallelism = source.get_max_parallelism();
        info!(
            "Creating a new stream, block_id={} with max_parallelism {:?}",
            block_id, source_max_parallelism
        );
        let mut block = InnerBlock::new(block_id, source, Default::default(), Default::default());
        if let Some(p) = source_max_parallelism {
            block.scheduler_requirements.max_parallelism(p);
        }
        drop(env);
        Stream { block, env: env_rc }
    }

    /// Allocate a new BlockId inside the environment.
    pub(crate) fn new_block_id(&mut self) -> BlockId {
        let new_id = self.block_count;
        self.block_count += 1;
        info!("Creating a new block, id={}", new_id);
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

pub fn max_cpu_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}