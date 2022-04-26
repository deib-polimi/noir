use std::io::Write;
use std::time::Duration;

use criterion::Throughput;
use criterion::{criterion_group, criterion_main, Criterion};
use fake::Fake;
use itertools::Itertools;
use rand::prelude::StdRng;
use rand::SeedableRng;

use noir::operator::source::{FileSource, FileSourceAsync};
use noir::BatchMode;
use noir::StreamEnvironment;

mod common;
use common::*;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn make_file(lines: usize) -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    let seed = b"By imDema, edomora97 and mark03.".to_owned();
    let r = &mut StdRng::from_seed(seed);

    for _ in 0..lines {
        use fake::faker::lorem::en::*;
        let line = Sentence(10..100).fake_with_rng::<String, _>(r);
        file.write_all(line.as_bytes()).unwrap();
        file.write_all(b"\n").unwrap();
    }

    file
}

fn wordcount_by_line_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("wordcount-line");
    group.sample_size(30);
    group.warm_up_time(Duration::from_secs(3));
    group.measurement_time(Duration::from_secs(12));
    group.throughput(Throughput::Elements(1));
    let rt = tokio::runtime::Builder::new_current_thread().enable_io().build().unwrap();
    
    group.bench_function("wordcount-fold", |b| {
        let builder = NoirBenchBuilder::new(
            noir_max_parallism_env,
            |n: u64, env: &mut StreamEnvironment| {
                let file = make_file(n as usize);
                let path = file.path();
                let source = FileSourceAsync::new(path);
                let result = env
                    .stream(source)
                    .batch_mode(BatchMode::fixed(1024))
                    .flat_map_async(move |line| line.split(' ').map(|s| s.to_owned()).collect::<Vec<_>>())
                    .group_by_async(|word: &String| word.clone())
                    .fold_async(0u64, |count, _word| *count += 1)
                    .unkey_async()
                    .collect_vec_async();
                (result, file)
            }
        );
        b.to_async(&rt).iter_custom(|n| builder.bench(n))
    });


    group.bench_function("wordcount-fold-assoc", |b| {
        let builder = NoirBenchBuilder::new(
            noir_max_parallism_env,
            |n: u64, env: &mut StreamEnvironment| {
                let file = make_file(n as usize);
                let path = file.path();
                let source = FileSourceAsync::new(path);
                let result = env
                    .stream(source)
                    .batch_mode(BatchMode::fixed(1024))
                    .flat_map_async(move |line| line.split(' ').map(|s| s.to_owned()).collect_vec())
                    .group_by_fold_async(
                        |w| w.clone(),
                        0,
                        |count, _word| *count += 1u64,
                        |count1, count2| *count1 += count2,
                    )
                    .unkey_async()
                    .collect_vec_async();
                (result, file)
            }
        );
        b.to_async(&rt).iter_custom(|n| builder.bench(n))
    });


    // group.bench_function("wordcount-fold-kstring", |b| {
    //     let builder = NoirBenchBuilder::new(
    //         noir_max_parallism_env,
    //         |n: u64, env: &mut StreamEnvironment| {
    //             let file = make_file(n as usize);
    //             let path = file.path();
    //             let source = FileSource::new(path);
    //             let result = env
    //                 .stream(source)
    //                 .batch_mode(BatchMode::fixed(1024))
    //                 .flat_map(move |line| {
    //                     line.split(' ')
    //                         .map(kstring::KString::from_ref)
    //                         .collect_vec()
    //                 })
    //                 .group_by_fold(
    //                     |w| w.clone(),
    //                     0,
    //                     |count, _word| *count += 1,
    //                     |count1, count2| *count1 += count2,
    //                 )
    //                 .unkey()
    //                 .collect_vec();
    //             (result, file)
    //         }
    //     );
    //     b.iter_custom(|n| builder.bench(n))
    // });

    // group.bench_function("wordcount-reduce", |b| {
    //     let builder = NoirBenchBuilder::new(
    //         noir_max_parallism_env,
    //         |n: u64, env: &mut StreamEnvironment| {
    //             let file = make_file(n as usize);
    //             let path = file.path();
    //             let source = FileSource::new(path);
    //             let result = env
    //                 .stream(source)
    //                 .batch_mode(BatchMode::fixed(1024))
    //                 .flat_map(move |line| line.split(' ').map(|s| s.to_owned()).collect_vec())
    //                 .group_by(|word| word.clone())
    //                 .map(|(_, word)| (word, 1))
    //                 .reduce(|(_w1, c1), (_w2, c2)| *c1 += c2)
    //                 .collect_vec();
    //             (result, file)
    //         }
    //     );
    //     b.iter_custom(|n| builder.bench(n))
    // });

    // group.bench_function("wordcount-reduce-assoc", |b| {
    //     let builder = NoirBenchBuilder::new(
    //         noir_max_parallism_env,
    //         |n: u64, env: &mut StreamEnvironment| {
    //             let file = make_file(n as usize);
    //             let path = file.path();
    //             let source = FileSource::new(path);
    //             let result = env
    //                 .stream(source)
    //                 .batch_mode(BatchMode::fixed(1024))
    //                 .flat_map(move |line| line.split(' ').map(|s| s.to_owned()).collect_vec())
    //                 .map(|word| (word, 1))
    //                 .group_by_reduce(|w| w.clone(), |(_w1, c1), (_w, c2)| *c1 += c2)
    //                 .unkey()
    //                 .collect_vec();
    //             (result, file)
    //         }
    //     );
    //     b.iter_custom(|n| builder.bench(n))
    // });
    group.finish();
}
use pprof::criterion::{PProfProfiler, Output};
criterion_group!(
    name = benches;
    config = Criterion::default(); //.with_profiler(PPRofProfiler::new(100, Output::Flamegraph(None)))
    targets = wordcount_by_line_benchmark);
criterion_main!(benches);
