use std::collections::VecDeque;
use std::fmt::{Display, Formatter};

use hashbrown::hash_map::IterMut;
use hashbrown::HashMap;

use crate::operator::window::{WindowDescription, WindowGenerator};
use crate::operator::{Data, DataKey, StreamElement};
use crate::stream::KeyValue;

/// Wrapper used to return either a single or multiple `WindowGenerator`.
///
/// This is the type returned by `KeyedWindowManager::add`. Adding an item into the window may
/// produce different outcomes based on the item's type.
enum ManagerAddIterator<'a, Key: DataKey, Out: Data, WindowDescr: WindowDescription<Key, Out>> {
    /// The item triggered just one key, so only the generator of that key is returned.
    ///
    /// This is the typical case for `Item` and `Timestamped`.
    Single(Option<(&'a Key, &'a mut WindowDescr::Generator)>),
    /// The item involed all the keys, so all the generators are returned.
    ///
    /// This happens for watermarks or stream ending, when all the keys may advance the window.
    All(IterMut<'a, Key, WindowDescr::Generator>),
}

impl<'a, Key: DataKey, Out: Data, WindowDescr: WindowDescription<Key, Out>> Iterator
    for ManagerAddIterator<'a, Key, Out, WindowDescr>
{
    type Item = (&'a Key, &'a mut WindowDescr::Generator);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ManagerAddIterator::Single(single) => single.take(),
            ManagerAddIterator::All(iter) => iter.next(),
        }
    }
}

/// Manager of multiple `WindowGenerator`, one for each key of the stream.
#[derive(Clone)]
pub(crate) struct KeyedWindowManager<
    Key: DataKey,
    Out: Data,
    WindowDescr: WindowDescription<Key, Out>,
> {
    /// The description that is used for generating the windows.
    descr: WindowDescr,
    /// The set of all the generators, indexed by key.
    generators: HashMap<Key, WindowDescr::Generator, ahash::RandomState>,
    /// The list of additional items to return downstream.
    ///
    /// This list will include the `Watermark`s, `Terminate`, and other non-data items.
    extra_items: VecDeque<StreamElement<KeyValue<Key, Out>>>,
    /// Whether the content of `generators` should be dropped.
    should_reset: bool,
}

impl<Key: DataKey, Out: Data, WindowDescr: WindowDescription<Key, Out>>
    KeyedWindowManager<Key, Out, WindowDescr>
{
    pub(crate) fn new(descr: WindowDescr) -> Self {
        Self {
            descr,
            generators: Default::default(),
            extra_items: Default::default(),
            should_reset: false,
        }
    }

    /// Add a `StreamElement` to this window manager.
    ///
    /// The returned value contains the set of all the involved window generators. The caller should
    /// process all the windows returned from each generator.
    pub(crate) fn add(
        &mut self,
        el: StreamElement<KeyValue<Key, Out>>,
    ) -> impl Iterator<Item = (&Key, &mut WindowDescr::Generator)> {
        if self.should_reset {
            assert!(
                self.extra_items.is_empty(),
                "resetting would lose extra items"
            );
            self.generators.clear();
        }

        match &el {
            StreamElement::Item(_) | StreamElement::Timestamped(_, _) => {
                let (key, el) = el.remove_key();
                let key = key.unwrap();
                if let Some(gen) = self.generators.get_mut(&key) {
                    gen.add(el);
                } else {
                    let mut gen = self.descr.new_generator();
                    gen.add(el);
                    self.generators.insert(key.clone(), gen);
                }

                // Only return this window generator, since the others are not affected by this element
                return ManagerAddIterator::<Key, Out, WindowDescr>::Single(
                    self.generators.get_key_value_mut(&key),
                );
            }

            StreamElement::FlushAndRestart | StreamElement::Watermark(_) => {
                // This is a flush, so all the generators should be dropped.
                self.should_reset = matches!(el, StreamElement::FlushAndRestart);
                // Pass the element to every window generator
                for (_key, gen) in self.generators.iter_mut() {
                    gen.add(el.clone().remove_key().1);
                }
                // Save this element so that it can be forwarded downstream
                self.extra_items.push_back(el);
                // Return all the generators
                return ManagerAddIterator::<Key, Out, WindowDescr>::All(
                    self.generators.iter_mut(),
                );
            }
            StreamElement::FlushBatch => {
                unreachable!("KeyedWindowManager does not handle FlushBatch")
            }
            StreamElement::Terminate => {
                unreachable!("KeyedWindowManager does not handle Terminate")
            }
            StreamElement::Yield => unreachable!("Windows do not handle Yield"),
        }
    }

    /// Return extra elements that should be forwarded downstream.
    pub(crate) fn next_extra_elements(&mut self) -> Option<StreamElement<KeyValue<Key, Out>>> {
        self.extra_items.pop_front()
    }
}

impl<Key: DataKey, Out: Data, WindowDescr: WindowDescription<Key, Out>> Display
    for KeyedWindowManager<Key, Out, WindowDescr>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "KeyedWindowManager<({}, {}), {}>",
            std::any::type_name::<Key>(),
            std::any::type_name::<Out>(),
            self.descr.to_string()
        )
    }
}
