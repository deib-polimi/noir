use std::time::Instant;

use itertools::Itertools;
use noir::environment::max_cpu_parallelism;
use regex::Regex;

use noir::operator::source::FileSourceAsync;
use noir::BatchMode;
use noir::EnvironmentConfig;
use noir::StreamEnvironment;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tracing_subscriber::fmt::init();


    let (config, args) = EnvironmentConfig::from_args();
    if args.len() != 1 {
        panic!("Pass the dataset path as an argument");
    }
    let path = &args[0];

    let mut env = StreamEnvironment::new(config);

    env.spawn_remote_workers();

    let source = FileSourceAsync::new(path);
    let tokenizer = Tokenizer::new();
    let result = env
        .stream(source)
        .batch_mode(BatchMode::fixed(1024))
        .flat_map_async(move |line| tokenizer.tokenize(line))
        .group_by_fold_async(
            |w| w.clone(),
            0u32,
            |count, _word| *count += 1u32,
            |count1, count2| *count1 += count2,
        )
        .collect_vec_async();
    let start = Instant::now();
    env.execute_async(max_cpu_parallelism()).join().await; // TODO: CHANGE PARALLELISM
    let elapsed = start.elapsed();
    if let Some(res) = result.get() {
        eprintln!("Output: {:?}", res.len());
        eprintln!("{:?}", res.iter().sorted_by_key(|t| t.1).rev().take(10).collect::<Vec<_>>())
    }
    eprintln!("Elapsed: {:?}", elapsed);
}

#[derive(Clone)]
struct Tokenizer {
    re: Regex,
}

impl Tokenizer {
    fn new() -> Self {
        Self {
            re: Regex::new(r"[A-Za-z]+").unwrap(),
        }
    }
    fn tokenize(&self, value: String) -> Vec<String> {
        self.re
            .find_iter(&value)
            .map(|t| t.as_str().to_lowercase())
            .collect()
    }
}
