use std::collections::VecDeque;

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::reorder::Reorder;
use crate::operator::window::window_manager::KeyedWindowManager;
use crate::operator::window::{Window, WindowDescription, WindowGenerator};
use crate::operator::{Data, DataKey, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::stream::{KeyValue, KeyedStream, KeyedWindowedStream};

/// This operator abstracts the window logic as an operator and delegates to the
/// `KeyedWindowManager` and a `ProcessFunc` the job of building and processing the windows,
/// respectively.
#[derive(Clone)]
pub(crate) struct GenericWindowOperator<
    Key: DataKey,
    Out: Data,
    NewOut: Data,
    ProcessFunc,
    WindowDescr,
    OperatorChain,
> where
    ProcessFunc: Fn(&Window<Key, Out>) -> NewOut + Clone,
    WindowDescr: WindowDescription<Key, Out> + Clone,
    OperatorChain: Operator<KeyValue<Key, Out>>,
{
    /// The previous operators in the chain.
    prev: OperatorChain,
    /// The name of the actual operator that this one abstracts.
    ///
    /// It is used only for tracing purposes.
    name: String,
    /// The manager that will build the windows.
    manager: KeyedWindowManager<Key, Out, WindowDescr>,
    /// The processing function that, given a window, produces an output element.
    process_func: ProcessFunc,
    /// A buffer for storing ready items.
    buffer: VecDeque<StreamElement<KeyValue<Key, NewOut>>>,
}

impl<Key: DataKey, Out: Data, NewOut: Data, F, WindowDescr, OperatorChain>
    GenericWindowOperator<Key, Out, NewOut, F, WindowDescr, OperatorChain>
where
    F: Fn(&Window<Key, Out>) -> NewOut + Clone,
    WindowDescr: WindowDescription<Key, Out> + Clone,
    OperatorChain: Operator<KeyValue<Key, Out>>,
{
    pub(crate) fn new<S: Into<String>>(
        name: S,
        prev: OperatorChain,
        descr: WindowDescr,
        process_func: F,
    ) -> Self {
        Self {
            name: name.into(),
            manager: KeyedWindowManager::new(descr),
            process_func,
            prev,
            buffer: Default::default(),
        }
    }
}

impl<Key: DataKey, Out: Data, NewOut: Data, F, WindowDescr, OperatorChain>
    Operator<KeyValue<Key, NewOut>>
    for GenericWindowOperator<Key, Out, NewOut, F, WindowDescr, OperatorChain>
where
    F: Fn(&Window<Key, Out>) -> NewOut + Clone + Send,
    WindowDescr: WindowDescription<Key, Out> + Clone,
    OperatorChain: Operator<KeyValue<Key, Out>>,
{
    fn setup(&mut self, metadata: ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<KeyValue<Key, NewOut>> {
        loop {
            if let Some(item) = self.buffer.pop_front() {
                return item;
            }

            let item = self.prev.next();
            // the manager is not interested in FlushBatch and Terminate
            if matches!(
                item,
                StreamElement::FlushBatch | StreamElement::Terminate | StreamElement::Yield
            ) {
                return item.map(|_| unreachable!());
            }

            for (key, window_gen) in self.manager.add(item) {
                while let Some(window) = window_gen.next_window() {
                    let value = (self.process_func)(&window);
                    self.buffer.push_back(if let Some(ts) = window.timestamp {
                        StreamElement::Timestamped((key.clone(), value), ts)
                    } else {
                        StreamElement::Item((key.clone(), value))
                    });
                }
            }

            while let Some(el) = self.manager.next_extra_elements() {
                self.buffer.push_back(match el {
                    StreamElement::Item(_)
                    | StreamElement::Timestamped(_, _)
                    | StreamElement::FlushBatch
                    | StreamElement::Terminate => {
                        unreachable!(
                            "StreamElement::{} cannot be returned as extra elements",
                            el.variant()
                        );
                    }
                    StreamElement::Watermark(ts) => StreamElement::Watermark(ts),
                    StreamElement::FlushAndRestart => StreamElement::FlushAndRestart,
                    StreamElement::Yield => return StreamElement::Yield, //TODO: Check
                });
            }
        }
    }

    fn to_string(&self) -> String {
        format!(
            "{} -> {} -> GenericOperator[{}]<{}>",
            self.prev.to_string(),
            self.manager,
            self.name,
            std::any::type_name::<NewOut>(),
        )
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<KeyValue<Key, NewOut>, _>(
                &self.name,
            ))
    }
}

impl<Key: DataKey, Out: Data, WindowDescr, OperatorChain>
    KeyedWindowedStream<Key, Out, OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<Key, Out> + Clone + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    /// Add a new generic window operator to a `KeyedWindowedStream`,
    /// after adding a Reorder operator.
    /// This should be used by every custom window aggregator.
    pub(crate) fn add_generic_window_operator<NewOut, S, F>(
        self,
        name: S,
        process_func: F,
    ) -> KeyedStream<Key, NewOut, impl Operator<KeyValue<Key, NewOut>>>
    where
        NewOut: Data,
        S: Into<String>,
        F: Fn(&Window<Key, Out>) -> NewOut + Clone + Send + 'static,
    {
        let stream = self.inner;
        let descr = self.descr;

        stream
            .add_operator(Reorder::new)
            .add_operator(|prev| GenericWindowOperator::new(name.into(), prev, descr, process_func))
    }
}
