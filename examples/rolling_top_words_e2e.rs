use std::time::Instant;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use nanorand::Rng;
use noir_compute::prelude::*;

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
    let mut rng = nanorand::tls_rng();

    for topic in TOPICS {
        if rng.generate::<f64>() < PROB {
            return topic.to_string();
        }
    }
    TOPICS[0].to_string()
}

struct TopicSource {
    id: u64,
    instances: u64,
    num_gen: u64,
    limit: u64,
}

impl TopicSource {
    fn new(id: u64, instances: u64, limit: u64) -> Self {
        Self {
            id,
            instances,
            num_gen: 0,
            limit,
        }
    }
}

impl Iterator for TopicSource {
    type Item = (i64, String);

    fn next(&mut self) -> Option<Self::Item> {
        let nth = self.num_gen * self.instances + self.id;
        if nth > self.limit {
            return None;
        }
        let topic = random_topic();
        let ts_millis = nth as i64;
        self.num_gen += 1;

        Some((ts_millis, topic))
    }
}

fn main() {
    let win_size_millis = 1000;
    let win_step_millis = 500;
    let k = 4;

    let (config, args) = EnvironmentConfig::from_args();
    let limit: u64 = args[0].parse().expect("Invalid number of events");

    config.spawn_remote_workers();
    let mut env = StreamEnvironment::new(config);

    let source =
        ParallelIteratorSource::new(move |id, instances| TopicSource::new(id, instances, limit));
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
            words.sort_unstable_by_key(|(_w, c)| -(*c as i64));
            words.truncate(k);
            words
        })
        .drop_key()
        .flatten()
        .fold(0, |acc, _| *acc += 1)
        .for_each(|c| println!("{c}"));
    let start = Instant::now();
    env.execute_blocking();
    println!("Elapsed: {:?}", start.elapsed());
}
