use std::time::{Duration, Instant};

use regex::Regex;

use noir::operator::source::FileSource;
use noir::operator::window::CountWindow;
use noir::BatchMode;
use noir::EnvironmentConfig;
use noir::StreamEnvironment;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    env_logger::init();
    
    let (config, args) = EnvironmentConfig::from_args();
    if args.len() != 1 {
        panic!("Pass the dataset path as an argument");
    }
    let path = &args[0];

    let mut env = StreamEnvironment::new(config);

    env.spawn_remote_workers();

    let source = FileSource::new(path);
    let tokenizer = Tokenizer::new();
    env.stream(source)
        .batch_mode(BatchMode::adaptive(1000, Duration::from_millis(100)))
        .flat_map(move |line| tokenizer.tokenize(line))
        .group_by(|word| word.clone())
        .window(CountWindow::sliding(10, 5))
        .fold(0, |count, _word| *count += 1)
        .for_each(|_, _| {});
    let start = Instant::now();
    env.execute();
    let elapsed = start.elapsed();
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
