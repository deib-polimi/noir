//! TODO:
//!
//!  - describe what noir is for
//!  - explain briefly the internals (job graph, execution graph, scheduler, blocks, operators,
//!    timestamps, watermarks, ...)
//!  - simple examples

#[macro_use]
extern crate derivative;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;

use std::ops::{Add, AddAssign};

use serde::{Deserialize, Serialize};

pub use block::structure;
pub use block::BatchMode;
pub use config::EnvironmentConfig;
pub use environment::StreamEnvironment;
pub use operator::iteration::IterationStateHandle;
pub use scheduler::ExecutionMetadata;
pub use stream::{KeyValue, KeyedStream, KeyedWindowedStream, Stream, WindowedStream};

use crate::block::BlockStructure;
use crate::network::Coord;
use crate::profiler::ProfilerResult;

pub(crate) mod block;
pub(crate) mod channel;
pub mod config;
pub(crate) mod environment;
pub(crate) mod network;
pub mod operator;
mod profiler;
pub(crate) mod runner;
pub(crate) mod scheduler;
pub(crate) mod stream;
#[cfg(test)]
pub(crate) mod test;
pub(crate) mod worker;

pub use scheduler::NoirJoinHandle;

/// Tracing information of the current execution.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct TracingData {
    structures: Vec<(Coord, BlockStructure)>,
    profilers: Vec<ProfilerResult>,
}

impl Add for TracingData {
    type Output = TracingData;

    fn add(mut self, rhs: Self) -> Self::Output {
        self += rhs;
        self
    }
}

impl AddAssign for TracingData {
    fn add_assign(&mut self, mut rhs: Self) {
        self.structures.append(&mut rhs.structures);
        self.profilers.append(&mut rhs.profilers);
    }
}
