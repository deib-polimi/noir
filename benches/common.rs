use criterion::black_box;
use std::marker::PhantomData;
use std::time::{Instant, Duration};

use noir::*;

pub struct NoirBenchBuilder<F, G, R>
where
    F: Fn() -> StreamEnvironment,
    G: Fn(u64, &mut StreamEnvironment) -> R,
{
    make_env: F,
    make_network: G,
    _result: PhantomData<R>,
}

impl<F, G, R> NoirBenchBuilder<F, G, R>
where
    F: Fn() -> StreamEnvironment,
    G: Fn(u64, &mut StreamEnvironment) -> R,
{
    pub fn new(make_env: F, make_network: G,) -> Self {
        Self {
            make_env,
            make_network,
            _result: Default::default()
        }
    }

    pub async fn bench(&self, n: u64) -> Duration {
        let mut env = (self.make_env)();
        let _result = (self.make_network)(n, &mut env);
        let start = Instant::now();
        let handle = env.execute_async(max_cpu_parallelism());
        black_box(_result);
        handle.join().await;
        let duration = start.elapsed();
        duration
    }
}

pub fn noir_max_parallism_env() -> StreamEnvironment {
    noir_local_env(max_cpu_parallelism())
}

pub fn noir_local_env(parallelism: usize) -> StreamEnvironment {
    let config = EnvironmentConfig::local(parallelism);
    StreamEnvironment::new(config)
}

pub fn max_cpu_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}