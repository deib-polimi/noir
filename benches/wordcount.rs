use std::io::Write;
use std::path::Path;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use fake::Fake;
use itertools::Itertools;
use noir::operator::source::FileSourceAsync;
use rand::prelude::StdRng;
use rand::SeedableRng;

use noir::operator::source::FileSource;
use noir::BatchMode;
use noir::EnvironmentConfig;
use noir::StreamEnvironment;

fn wordcount_fold(path: &Path) {
    let config = EnvironmentConfig::local(4);
    let mut env = StreamEnvironment::new(config);

    let source = FileSourceAsync::new(path);
    let _result = env
        .stream(source)
        .batch_mode(BatchMode::fixed(1024))
        .flat_map_async(move |line| line.split(' ').map(|s| s.to_owned()).collect_vec())
        .group_by_async(|word: &String| word.clone())
        .fold_async(0, |count, _word| *count += 1)
        .collect_vec_async();
    env.execute();
}

// fn wordcount_fold_assoc(path: &Path) {
//     let config = EnvironmentConfig::local(4);
//     let mut env = StreamEnvironment::new(config);

//     let source = FileSource::new(path);
//     let stream = env
//         .stream(source)
//         .batch_mode(BatchMode::fixed(1024))
//         .flat_map_async(move |line| line.split(' ').map(|s| s.to_owned()).collect_vec())
//         .group_by_fold(
//             |w| w.clone(),
//             0,
//             |count, _word| *count += 1,
//             |count1, count2| *count1 += count2,
//         )
//         .unkey();
//     let _result = stream.collect_vec();
//     env.execute();
// }

// fn wordcount_fold_assoc_kstring(path: &Path) {
//     let config = EnvironmentConfig::local(4);
//     let mut env = StreamEnvironment::new(config);

//     let source = FileSource::new(path);
//     let stream = env
//         .stream(source)
//         .batch_mode(BatchMode::fixed(1024))
//         .flat_map_async(move |line| {
//             line.split(' ')
//                 .map(kstring::KString::from_ref)
//                 .collect_vec()
//         })
//         .group_by_fold(
//             |w| w.clone(),
//             0,
//             |count, _word| *count += 1,
//             |count1, count2| *count1 += count2,
//         )
//         .unkey();
//     let _result = stream.collect_vec();
//     env.execute();
// }

// fn wordcount_reduce(path: &Path) {
//     let config = EnvironmentConfig::local(4);
//     let mut env = StreamEnvironment::new(config);

//     let source = FileSource::new(path);
//     let _result = env
//         .stream(source)
//         .batch_mode(BatchMode::fixed(1024))
//         .flat_map_async(move |line| line.split(' ').map(|s| s.to_owned()).collect_vec())
//         .group_by_async(|word| word.clone())
//         .map(|(_, word)| (word, 1))
//         .reduce(|(_w1, c1), (_w2, c2)| *c1 += c2)
//         .collect_vec();
//     env.execute();
// }

// fn wordcount_reduce_assoc(path: &Path) {
//     let config = EnvironmentConfig::local(4);
//     let mut env = StreamEnvironment::new(config);

//     let source = FileSource::new(path);
//     let stream = env
//         .stream(source)
//         .batch_mode(BatchMode::fixed(1024))
//         .flat_map_async(move |line| line.split(' ').map(|s| s.to_owned()).collect_vec())
//         .map(|word| (word, 1))
//         .group_by_reduce(|w| w.clone(), |(_w1, c1), (_w, c2)| *c1 += c2)
//         .unkey();
//     let _result = stream.collect_vec();
//     env.execute();
// }

fn wordcount_benchmark(c: &mut Criterion) {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    let seed = b"rstream2 by edomora97 and mark03".to_owned();
    let r = &mut StdRng::from_seed(seed);

    const N_LINES: usize = 100_000;
    for _ in 0..N_LINES {
        use fake::faker::lorem::en::*;
        let line = Sentence(10..100).fake_with_rng::<String, _>(r);
        file.write_all(line.as_bytes()).unwrap();
        file.write_all(b"\n").unwrap();
    }

    let path = file.path();
    let metadata = path.metadata().unwrap();
    let size = metadata.len();

    let mut group = c.benchmark_group("wordcount");
    group.sample_size(10);
    group.throughput(Throughput::Bytes(size));
    group.warm_up_time(Duration::from_secs(4));
    group.bench_function("wordcount-fold", |b| {
        b.iter(|| wordcount_fold(black_box(path)))
    });
    group.bench_function("wordcount-fold-assoc", |b| {
        b.iter(|| wordcount_fold_assoc(black_box(path)))
    });
    group.bench_function("wordcount-fold-kstring", |b| {
        b.iter(|| wordcount_fold_assoc_kstring(black_box(path)))
    });
    group.bench_function("wordcount-reduce", |b| {
        b.iter(|| wordcount_reduce(black_box(path)))
    });
    group.bench_function("wordcount-reduce-assoc", |b| {
        b.iter(|| wordcount_reduce_assoc(black_box(path)))
    });
    group.finish();
}

criterion_group!(benches, wordcount_benchmark);
criterion_main!(benches);
