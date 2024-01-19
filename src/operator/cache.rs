use std::collections::{HashMap, VecDeque};
use std::{fmt::Display, sync::Arc};

use itertools::Itertools;
use parking_lot::Mutex;

use crate::{
    block::{BlockStructure, OperatorKind, OperatorStructure},
    environment::StreamEnvironmentInner,
    Replication, Stream,
};

use super::{source::Source, Data, Operator, StreamElement};

pub(crate) type StreamCacheRef<Out> = Arc<Mutex<Option<HashMap<u64, VecDeque<Out>>>>>;

pub struct StreamCache<Out> {
    cache_reference: StreamCacheRef<Out>,
    env: Arc<Mutex<StreamEnvironmentInner>>,
}

#[derive(Clone)]
pub struct CacheSource<Out> {
    data: StreamCacheRef<Out>,
    items: VecDeque<Out>,
}

#[derive(Clone)]
pub struct CacheSink<Out: Data, PreviousOperator>
where
    PreviousOperator: Operator<Out = Out> + 'static,
{
    cache_reference: StreamCacheRef<Out>,
    result: Option<VecDeque<Out>>,
    id: Option<u64>,
    prev: PreviousOperator,
}

impl<Out: Data + Send> StreamCache<Out> {
    pub(crate) fn new(
        output: StreamCacheRef<Out>,
        environment: Arc<Mutex<StreamEnvironmentInner>>,
    ) -> Self {
        Self {
            cache_reference: output,
            env: environment,
        }
    }

    // for debuggin purposes
    pub fn read(&self) -> Vec<Out> {
        let cache_lock = self.cache_reference.lock();
        assert!(
            cache_lock.is_some(),
            "Can't read the cache before the execution"
        );
        cache_lock
            .clone()
            .unwrap()
            .into_iter()
            .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
            .flat_map(|(_, values)| values.into_iter())
            .fold(Vec::new(), |mut acc, values| {
                acc.push(values);
                acc
            })
    }

    pub fn stream(&self) -> Stream<CacheSource<Out>> {
        let source = CacheSource::from(self.cache_reference.clone());

        StreamEnvironmentInner::stream(self.env.clone(), source)
    }

    pub fn clear(self) {
        self.cache_reference.lock().take();
    }
}

impl<Out: Data + Send> Clone for StreamCache<Out> {
    fn clone(&self) -> Self {
        let lock = self.cache_reference.lock();
        let cache_clone = lock
            .as_ref()
            .expect("Can't read the cache before the execution")
            .clone();
        Self {
            cache_reference: Arc::new(Mutex::new(Some(cache_clone))),
            env: self.env.clone(),
        }
    }
}

impl<Out: Data, PreviousOperator> CacheSink<Out, PreviousOperator>
where
    PreviousOperator: Operator<Out = Out> + 'static,
{
    pub(crate) fn new(prev: PreviousOperator, output: StreamCacheRef<Out>) -> Self {
        Self {
            cache_reference: output,
            result: Some(VecDeque::new()),
            id: None,
            prev,
        }
    }
}

impl<Out: Data, PreviousOperator> Display for CacheSink<Out, PreviousOperator>
where
    PreviousOperator: Operator<Out = Out> + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> Cache<{}>",
            self.prev,
            std::any::type_name::<Out>()
        )
    }
}

impl<Out: Data, PreviousOperator> Operator for CacheSink<Out, PreviousOperator>
where
    PreviousOperator: Operator<Out = Out> + 'static,
{
    type Out = ();

    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        self.id = Some(metadata.global_id);
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> super::StreamElement<()> {
        match self.prev.next() {
            StreamElement::Item(t) | StreamElement::Timestamped(t, _) => {
                self.result.as_mut().unwrap().push_back(t);
                StreamElement::Item(())
            }
            StreamElement::Terminate => {
                let mut cache_opt = self.cache_reference.lock();
                if cache_opt.is_none() {
                    cache_opt.replace(HashMap::new());
                }
                let cache = cache_opt.as_mut().unwrap();
                cache.insert(self.id.unwrap(), self.result.take().unwrap());
                StreamElement::Terminate
            }
            StreamElement::Watermark(w) => StreamElement::Watermark(w),
            StreamElement::FlushBatch => StreamElement::FlushBatch,
            StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
        }
    }

    fn structure(&self) -> crate::block::BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<Out, _>("Cache"))
    }
}

impl<Out: Data> Display for CacheSource<Out> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CacheSource<{}>", std::any::type_name::<Out>())
    }
}

impl<Out> From<StreamCacheRef<Out>> for CacheSource<Out> {
    fn from(output: StreamCacheRef<Out>) -> Self {
        assert!(
            output.lock().is_some(),
            "Can't read the cache before the execution"
        );
        Self {
            data: output,
            items: VecDeque::new(),
        }
    }
}

impl<Out: Data> Source for CacheSource<Out> {
    fn replication(&self) -> Replication {
        Replication::Unlimited
    }
}

impl<Out: Data> Operator for CacheSource<Out> {
    type Out = Out;

    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        let id = metadata.global_id;
        let mut cache_lock = self.data.lock();
        match cache_lock
            .as_mut()
            .expect("Can't read the cache before the execution")
            .get_mut(&id)
        {
            Some(items) => {
                std::mem::swap(&mut self.items, items);
            }
            None => {
                self.items.clear();
            }
        }
    }

    fn next(&mut self) -> StreamElement<Out> {
        match self.items.pop_front() {
            Some(item) => StreamElement::Item(item),
            None => StreamElement::Terminate,
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("ChannelSource");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        operator::source::{IteratorSource, ParallelIteratorSource},
        StreamEnvironment,
    };

    #[test]
    #[should_panic(expected = "Can't read the cache before the execution")]
    fn read_before_execution() {
        let mut env = StreamEnvironment::default();

        let n = 20;
        let source = ParallelIteratorSource::new(move |id, instances| {
            let chunk_size = (n + instances - 1) / instances;
            let remaining = n - n.min(chunk_size * id);
            let range = remaining.min(chunk_size);
            let start = id * chunk_size;
            let stop = id * chunk_size + range;
            start..stop
        });

        let (cache, _stream) = env.stream(source).map(|x| x + 1).checkpoint();

        cache.read();
    }

    #[test]
    fn read_after_execution() {
        let mut env = StreamEnvironment::default();

        let n = 20;
        let source = IteratorSource::new(0..n);

        let (cache, stream) = env.stream(source).map(|x| x + 1).checkpoint();

        let result = stream.map(|x| x * 2).collect_vec();

        env.execute_blocking_interactive();
        assert_eq!(cache.read(), (0..n).map(|x| x + 1).collect::<Vec<_>>());
        assert_eq!(
            result.get().unwrap(),
            (0..n).map(|x| (x + 1) * 2).collect::<Vec<_>>()
        );

        let result = cache.stream().map(|x| x * 3).collect_vec();

        env.execute_blocking();
        assert_eq!(cache.read(), Vec::<u64>::new());
        assert_eq!(
            result.get().unwrap(),
            (0..n).map(|x| (x + 1) * 3).collect::<Vec<_>>()
        );
    }

    #[test]
    fn read_after_execution_parallel() {
        let mut env = StreamEnvironment::default();

        let n = 20;
        let source = ParallelIteratorSource::new(move |id, instances| {
            let chunk_size = (n + instances - 1) / instances;
            let remaining = n - n.min(chunk_size * id);
            let range = remaining.min(chunk_size);
            let start = id * chunk_size;
            let stop = id * chunk_size + range;
            start..stop
        });

        let (cache, stream) = env.stream(source).map(|x| x + 1).checkpoint();

        let result = stream.map(|x| x * 2).collect_vec();

        env.execute_blocking_interactive();
        assert_eq!(cache.read(), (0..n).map(|x| x + 1).collect::<Vec<_>>());
        assert_eq!(
            result.get().unwrap(),
            (0..n).map(|x| (x + 1) * 2).collect::<Vec<_>>()
        );

        let result = cache.stream().map(|x| x * 3).collect_vec();

        env.execute_blocking();
        assert_eq!(cache.read(), Vec::<u64>::new());
        assert_eq!(
            result.get().unwrap(),
            (0..n).map(|x| (x + 1) * 3).collect::<Vec<_>>()
        );
    }
}
