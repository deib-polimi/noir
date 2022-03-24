use std::collections::HashMap;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Data, DataKey, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, Stream};

use super::AsyncOperator;

#[pin_project::pin_project]
#[derive(Clone, Debug)]
struct RichMap<Key: DataKey, Out: Data, NewOut: Data, F, OperatorChain>
where
    F: FnMut(KeyValue<&Key, Out>) -> NewOut + Clone + Send,
    OperatorChain: Operator<KeyValue<Key, Out>>,
{
    #[pin]
    prev: OperatorChain,
    maps_fn: HashMap<Key, F, ahash::RandomState>,
    init_map: F,
    _out: PhantomData<Out>,
    _new_out: PhantomData<NewOut>,
}

impl<Key: DataKey, Out: Data, NewOut: Data, F, OperatorChain>
    RichMap<Key, Out, NewOut, F, OperatorChain>
where
    F: FnMut(KeyValue<&Key, Out>) -> NewOut + Clone + Send,
    OperatorChain: Operator<KeyValue<Key, Out>>,
{
    fn new(prev: OperatorChain, f: F) -> Self {
        Self {
            prev,
            maps_fn: Default::default(),
            init_map: f,
            _out: Default::default(),
            _new_out: Default::default(),
        }
    }
}

impl<Key: DataKey, Out: Data, NewOut: Data, F, OperatorChain> Operator<KeyValue<Key, NewOut>>
    for RichMap<Key, Out, NewOut, F, OperatorChain>
where
    F: FnMut(KeyValue<&Key, Out>) -> NewOut + Clone + Send,
    OperatorChain: Operator<KeyValue<Key, Out>>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<(Key, NewOut)> {
        let element = self.prev.next();
        if matches!(element, StreamElement::FlushAndRestart) {
            self.maps_fn.clear();
        }
        element.map(|(key, value)| {
            let map_fn = if let Some(map_fn) = self.maps_fn.get_mut(&key) {
                map_fn
            } else {
                // the key is not present in the hashmap, so this always inserts a new map function
                let map_fn = self.init_map.clone();
                self.maps_fn.entry(key.clone()).or_insert(map_fn)
            };

            let new_value = (map_fn)((&key, value));
            (key, new_value)
        })
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> RichMap<{} -> {}>",
            self.prev.to_string(),
            std::any::type_name::<Out>(),
            std::any::type_name::<NewOut>()
        )
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<NewOut, _>("RichMap"))
    }
}


impl<Key: DataKey, Out: Data, NewOut: Data, F, OperatorChain> futures::Stream
    for RichMap<Key, Out, NewOut, F, OperatorChain>
where
    F: FnMut(KeyValue<&Key, Out>) -> NewOut + Clone + Send,
    OperatorChain: AsyncOperator<KeyValue<Key, Out>>,
{
    type Item = StreamElement<KeyValue<Key, NewOut>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let proj = self.project();
        let element = ready!(proj.prev.poll_next(cx)).unwrap_or_else(|| StreamElement::Terminate);
        if matches!(element, StreamElement::FlushAndRestart) {
            proj.maps_fn.clear();
        }
        let r = element.map(|(key, value)| {
            let map_fn = proj.maps_fn.entry(key.clone())
                .or_insert_with(|| proj.init_map.clone());
            // let map_fn = if let Some(map_fn) = proj.maps_fn.get_mut(&key) {
            //     map_fn
            // } else {
            //     // the key is not present in the hashmap, so this always inserts a new map function
            //     let map_fn = proj.init_map.clone();
            //     proj.maps_fn.entry(key.clone()).or_insert(map_fn)
            // };

            let new_value = (map_fn)((&key, value));
            (key, new_value)
        });

        Poll::Ready(Some(r))
    }
}

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Map the elements of the stream into new elements. The mapping function can be stateful.
    ///
    /// This is equivalent to [`Stream::map`] but with a stateful function.
    ///
    /// Since the mapping function can be stateful, it is a `FnMut`. This allows expressing simple
    /// algorithms with very few lines of code (see examples).
    ///
    /// The mapping function is _cloned_ inside each replica, and they will not share state between
    /// each other. If you want that only a single replica handles all the items you may want to
    /// change the parallelism of this operator with [`Stream::max_parallelism`].
    ///
    /// ## Examples
    ///
    /// This is a simple implementation of the prefix-sum using a single replica (i.e. each element
    /// is mapped to the sum of all the elements up to that point). Note that this won't work if
    /// there are more replicas.
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((1..=5)));
    /// let res = s.rich_map({
    ///     let mut sum = 0;
    ///     move |x| {
    ///         sum += x;
    ///         sum
    ///     }
    /// }).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![1, 1 + 2, 1 + 2 + 3, 1 + 2 + 3 + 4, 1 + 2 + 3 + 4 + 5]);
    /// ```    
    ///
    /// This will enumerate all the elements that reach a replica. This is basically equivalent to
    /// the `enumerate` function in Python.
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((1..=5)));
    /// let res = s.rich_map({
    ///     let mut id = 0;
    ///     move |x| {
    ///         id += 1;
    ///         (id - 1, x)
    ///     }
    /// }).collect_vec();
    ///
    /// env.execute();
    ///
    /// assert_eq!(res.get().unwrap(), vec![(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)]);
    /// ```
    pub fn rich_map<NewOut: Data, F>(self, mut f: F) -> Stream<NewOut, impl Operator<NewOut>>
    where
        F: FnMut(Out) -> NewOut + Send + Clone + 'static,
    {
        self.key_by(|_| ())
            .add_operator(|prev| RichMap::new(prev, move |(_, value)| f(value)))
            .drop_key()
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Map the elements of the stream into new elements. The mapping function can be stateful.
    ///
    /// This is exactly like [`Stream::rich_map`], but the function is cloned for each key. This
    /// means that each key will have a unique mapping function (and therefore a unique state).
    pub fn rich_map<NewOut: Data, F>(
        self,
        f: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        F: FnMut(KeyValue<&Key, Out>) -> NewOut + Clone + Send + 'static,
    {
        self.add_operator(|prev| RichMap::new(prev, f))
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: AsyncOperator<KeyValue<Key, Out>> + 'static,
{
    /// Map the elements of the stream into new elements. The mapping function can be stateful.
    ///
    /// This is exactly like [`Stream::rich_map`], but the function is cloned for each key. This
    /// means that each key will have a unique mapping function (and therefore a unique state).
    pub fn rich_map_async<NewOut: Data, F>(
        self,
        f: F,
    ) -> KeyedStream<Key, NewOut, impl AsyncOperator<KeyValue<Key, NewOut>>>
    where
        F: FnMut(KeyValue<&Key, Out>) -> NewOut + Clone + Send + 'static,
    {
        self.add_async_operator(|prev| RichMap::new(prev, f))
    }
}
