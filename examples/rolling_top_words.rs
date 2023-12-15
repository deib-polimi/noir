use std::time::{Instant, SystemTime};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use noir_compute::prelude::*;
use rand::prelude::*;

const TOPICS: [&str; 50] = [
    "#love",
    "#instagood",
    "#fashion",
    "#photooftheday",
    "#beautiful",
    "#art",
    "#photography",
    "#happy",
    "#picoftheday",
    "#cute",
    "#follow",
    "#tbt",
    "#followme",
    "#nature",
    "#like",
    "#travel",
    "#instagram",
    "#style",
    "#repost",
    "#summer",
    "#instadaily",
    "#selfie",
    "#me",
    "#friends",
    "#fitness",
    "#girl",
    "#food",
    "#fun",
    "#beauty",
    "#instalike",
    "#smile",
    "#family",
    "#photo",
    "#life",
    "#likeforlike",
    "#music",
    "#ootd",
    "#follow",
    "#makeup",
    "#amazing",
    "#igers",
    "#nofilter",
    "#dog",
    "#model",
    "#sunset",
    "#beach",
    "#instamood",
    "#foodporn",
    "#motivation",
    "#followforfollow",
];
const PROB: f64 = 0.1;

fn random_topic() -> String {
    let mut rng = rand::thread_rng();

    for topic in TOPICS {
        if rng.gen::<f64>() < PROB {
            return topic.to_string();
        }
    }
    TOPICS[0].to_string()
}

#[derive(Clone)]
struct ThroughputTester {
    name: String,
    count: usize,
    limit: usize,
    last: Instant,
    start: Instant,
    total: usize,
}

impl ThroughputTester {
    fn new(name: String, limit: usize) -> Self {
        Self {
            name,
            count: 0,
            limit,
            last: Instant::now(),
            start: Instant::now(),
            total: 0,
        }
    }

    fn add(&mut self) {
        self.count += 1;
        self.total += 1;
        if self.count > self.limit {
            let elapsed = self.last.elapsed();
            eprintln!(
                "{}: {:10.2}/s @ {}",
                self.name,
                self.count as f64 / elapsed.as_secs_f64(),
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            );
            self.count = 0;
            self.last = Instant::now();
        }
    }
}

impl Drop for ThroughputTester {
    fn drop(&mut self) {
        eprintln!(
            "(done) {}: {:10.2}/s (total {})",
            self.name,
            self.total as f64 / self.start.elapsed().as_secs_f64(),
            self.total,
        );
    }
}

struct TopicSource {
    tester: ThroughputTester,
    start: Instant,
    id: u64,
    instances: u64,
    num_gen: u64,
}

impl TopicSource {
    fn new(id: u64, instances: u64) -> Self {
        Self {
            tester: ThroughputTester::new(format!("source{id}"), 50_000),
            start: Instant::now(),
            id,
            instances,
            num_gen: 0,
        }
    }
}

impl Iterator for TopicSource {
    type Item = (i64, String);

    fn next(&mut self) -> Option<Self::Item> {
        if self.start.elapsed().as_secs() > 10 {
            return None;
        }
        let nth = self.num_gen * self.instances + self.id;
        let topic = random_topic();
        let ts_millis = nth as i64;
        self.num_gen += 1;
        self.tester.add();

        Some((ts_millis, topic))
    }
}

fn main() {
    let win_size_millis = 1000;
    let win_step_millis = 500;
    let k = 4;

    let (config, _args) = EnvironmentConfig::from_args();
    let mut env = StreamEnvironment::new(config);
    env.spawn_remote_workers();

    let source = ParallelIteratorSource::new(TopicSource::new);
    env.stream(source)
        // add a timestamp for each item (using the one generated by the source) and add a watermark
        // every 10 items
        .add_timestamps(|(ts, _)| *ts, {
            let mut count = 0;
            move |_, &ts| {
                count += 1;
                if count % 10 == 0 {
                    Some(ts)
                } else {
                    None
                }
            }
        })
        // forget about the timestamp, it's already attached to the messages
        .map(|(_ts, w)| w)
        // count each word separately
        .group_by(|w| w.clone())
        .window(EventTimeWindow::sliding(win_size_millis, win_step_millis))
        // count how many times each word appears in the window
        .map(|w| w.len())
        .unkey()
        // this window has the same alignment of the previous one, so it will contain the same items
        .window_all(EventTimeWindow::tumbling(win_step_millis))
        .map(move |mut words| {
            // find the k most frequent words for each window
            words.sort_by_key(|(_w, c)| -(*c as i64));
            words.resize_with(k.min(words.len()), Default::default);
            words
        })
        .for_each({
            let mut tester = ThroughputTester::new("sink".into(), 100);
            move |_win| {
                tester.add();
            }
        });
    env.execute_blocking();
}
