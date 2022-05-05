#[cfg(not(feature = "flume"))]
mod tokio_channel;
#[cfg(not(feature = "flume"))]
pub use self::tokio_channel::*;

#[cfg(feature = "flume")]
mod flume_channel;
#[cfg(feature = "flume")]
pub use self::flume_channel::*;