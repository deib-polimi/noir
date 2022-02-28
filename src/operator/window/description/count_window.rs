use std::collections::VecDeque;
use std::marker::PhantomData;
use std::num::NonZeroUsize;

use crate::operator::window::{Window, WindowDescription, WindowGenerator};
use crate::operator::{Data, DataKey, StreamElement, Timestamp};

/// Count windows divide elements into windows by counting them.
/// They can be used with both timestamped and non-timestamped streams.
/// When used with timestamped streams, elements are ordered according to their timestamps.
///
/// There are two different kinds of count windows:
///  - [`CountWindow::sliding`]
///  - [`CountWindow::tumbling`]
#[derive(Clone, Debug)]
pub struct CountWindow {
    size: NonZeroUsize,
    step: NonZeroUsize,
}

impl CountWindow {
    /// With sliding count windows, the first window contains the first `size` elements.
    /// From then on, each window contains the same elements of the previous one, except that the
    /// oldest `step` elements are replaced with new ones.
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # use noir::operator::window::CountWindow;
    /// # use std::time::Duration;
    /// # use itertools::Itertools;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s
    ///     .window_all(CountWindow::sliding(3, 2))
    ///     .map(|window| window.cloned().collect_vec())
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// assert_eq!(res, vec![vec![0, 1, 2], vec![2, 3, 4], vec![4]]);
    /// ```
    pub fn sliding(size: usize, step: usize) -> Self {
        Self {
            size: NonZeroUsize::new(size).expect("CountWindow size must be positive"),
            step: NonZeroUsize::new(step).expect("CountWindow size must be positive"),
        }
    }

    /// Tumbling count windows are equivalent to sliding count windows with `step` equal to `size`.
    ///
    /// ## Example
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::IteratorSource;
    /// # use noir::operator::window::CountWindow;
    /// # use std::time::Duration;
    /// # use itertools::Itertools;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s
    ///     .window_all(CountWindow::tumbling(3))
    ///     .map(|window| window.cloned().collect_vec())
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// assert_eq!(res, vec![vec![0, 1, 2], vec![3, 4]]);
    /// ```
    pub fn tumbling(size: usize) -> Self {
        CountWindow::sliding(size, size)
    }
}

impl<Key: DataKey, Out: Data> WindowDescription<Key, Out> for CountWindow {
    type Generator = CountWindowGenerator<Key, Out>;

    fn new_generator(&self) -> Self::Generator {
        CountWindowGenerator::new(self.clone())
    }

    fn to_string(&self) -> String {
        format!(
            "CountWindow[size={}, step={}]",
            self.size.get(),
            self.step.get()
        )
    }
}

#[derive(Clone, Debug)]
pub struct CountWindowGenerator<Key: DataKey, Out: Data> {
    descr: CountWindow,
    buffer: VecDeque<Out>,
    timestamp_buffer: VecDeque<Timestamp>,
    received_end: bool,
    last_watermark: Option<Timestamp>,
    _key: PhantomData<Key>,
}

impl<Key: DataKey, Out: Data> CountWindowGenerator<Key, Out> {
    fn new(descr: CountWindow) -> Self {
        Self {
            descr,
            buffer: Default::default(),
            timestamp_buffer: Default::default(),
            received_end: false,
            last_watermark: None,
            _key: Default::default(),
        }
    }
}

impl<Key: DataKey, Out: Data> WindowGenerator<Key, Out> for CountWindowGenerator<Key, Out> {
    fn add(&mut self, item: StreamElement<Out>) {
        match item {
            StreamElement::Item(item) => self.buffer.push_back(item),
            StreamElement::Timestamped(item, ts) => {
                self.buffer.push_back(item);
                self.timestamp_buffer.push_back(ts);
            }
            StreamElement::Watermark(ts) => self.last_watermark = Some(ts),
            StreamElement::FlushAndRestart => {
                self.received_end = true;
            }
            StreamElement::FlushBatch => unreachable!("Windows do not handle FlushBatch"),
            StreamElement::Terminate => unreachable!("Windows do not handle Terminate"),
            StreamElement::Yield => unreachable!("Windows do not handle Yield"),
        }
    }

    fn next_window(&mut self) -> Option<Window<Key, Out>> {
        if self.buffer.len() >= self.descr.size.get()
            || (self.received_end && !self.buffer.is_empty())
        {
            let size = self.descr.size.get().min(self.buffer.len());
            let timestamp_items = self.timestamp_buffer.iter().take(size).max().cloned();
            let timestamp = match &(timestamp_items, self.last_watermark) {
                (Some(ts), Some(w)) => {
                    // Make sure timestamp is correct with respect to watermarks
                    Some((*ts).max(*w + Timestamp::from_nanos(1)))
                }
                (Some(ts), _) => Some(*ts),
                _ => None,
            };

            Some(Window {
                size,
                gen: self,
                timestamp,
            })
        } else {
            None
        }
    }

    fn advance(&mut self) {
        for _ in 0..self.descr.step.get() {
            self.buffer.pop_front();
            self.timestamp_buffer.pop_front();
        }
    }

    fn buffer(&self) -> &VecDeque<Out> {
        &self.buffer
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::operator::window::description::count_window::{CountWindow, CountWindowGenerator};
    use crate::operator::window::{WindowDescription, WindowGenerator};
    use crate::operator::StreamElement;

    #[test]
    fn count_window_watermark() {
        let descr = CountWindow::sliding(3, 2);
        let mut generator: CountWindowGenerator<u32, _> = descr.new_generator();

        generator.add(StreamElement::Timestamped(1, Duration::from_secs(1)));
        assert!(generator.next_window().is_none());
        generator.add(StreamElement::Timestamped(2, Duration::from_secs(2)));
        assert!(generator.next_window().is_none());
        generator.add(StreamElement::Watermark(Duration::from_secs(4)));
        assert!(generator.next_window().is_none());
        generator.add(StreamElement::FlushAndRestart);
        let window = generator.next_window().unwrap();
        assert_eq!(
            window.timestamp,
            Some(Duration::from_nanos(1) + Duration::from_secs(4))
        );
        assert_eq!(window.size, 2);
    }

    #[test]
    fn count_window_timestamp() {
        let descr = CountWindow::sliding(3, 2);
        let mut generator: CountWindowGenerator<u32, _> = descr.new_generator();

        generator.add(StreamElement::Timestamped(1, Duration::from_secs(1)));
        assert!(generator.next_window().is_none());
        generator.add(StreamElement::Timestamped(2, Duration::from_secs(2)));
        assert!(generator.next_window().is_none());
        generator.add(StreamElement::Timestamped(3, Duration::from_secs(3)));
        let window = generator.next_window().unwrap();
        assert_eq!(window.timestamp, Some(Duration::from_secs(3)));
        assert_eq!(window.size, 3);
    }
}
