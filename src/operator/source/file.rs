use std::fs::File;
use std::io::BufRead;
use std::io::Seek;
use std::io::{BufReader, SeekFrom};
use std::path::PathBuf;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use futures::StreamExt;
use futures::executor::block_on;
use futures::ready;
use tokio::io::AsyncBufReadExt;
use tokio::io::{BufReader as BufReaderAsync, SeekFrom as SeekFromAsync};
use tokio::io::AsyncSeekExt;
use tokio::fs::File as FileAsync;
use tokio_stream::wrappers::LinesStream;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::operator::source::Source;
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;

/// Source that reads a text file line-by-line.
///
/// The file is divided in chunks and is read concurrently by multiple replicas.
#[derive(Debug)]
pub struct FileSource {
    path: PathBuf,
    // reader is initialized in `setup`, before it is None
    reader: Option<BufReader<File>>,
    current: usize,
    end: usize,
    terminated: bool,
}

impl FileSource {
    /// Create a new source that reads the lines from a text file.
    ///
    /// The file is partitioned into as many chunks as replicas, each replica has to have the
    /// **same** file in the same path. It is guaranteed that each line of the file is emitted by
    /// exactly one replica.
    ///
    /// **Note**: the file must be readable and its size must be available. This means that only
    /// regular files can be read.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::FileSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let source = FileSource::new("/datasets/huge.txt");
    /// let s = env.stream(source);
    /// ```
    pub fn new<P>(path: P) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            path: path.into(),
            reader: Default::default(),
            current: 0,
            end: 0,
            terminated: false,
        }
    }
}

impl Source<String> for FileSource {
    fn get_max_parallelism(&self) -> Option<usize> {
        None
    }
}

impl Operator<String> for FileSource {
    fn setup(&mut self, metadata: ExecutionMetadata) {
        let global_id = metadata.global_id;
        let num_replicas = metadata.replicas.len();

        let file = File::open(&self.path).unwrap_or_else(|err| {
            panic!(
                "FileSource: error while opening file {:?}: {:?}",
                self.path, err
            )
        });
        let file_size = file.metadata().unwrap().len() as usize;

        let range_size = file_size / num_replicas;
        let start = range_size * global_id;
        self.current = start;
        self.end = if global_id == num_replicas - 1 {
            file_size
        } else {
            start + range_size
        };

        let mut reader = BufReader::new(file);
        // Seek reader to the first byte to be read
        reader
            .seek(SeekFrom::Current(start as i64))
            .expect("seek file");
        if global_id != 0 {
            // discard first line
            let mut s = String::new();
            self.current += reader
                .read_line(&mut s)
                .expect("Cannot read line from file");
        }
        self.reader = Some(reader);
    }

    fn next(&mut self) -> StreamElement<String> {
        if self.terminated {
            return StreamElement::Terminate;
        }
        let element = if self.current <= self.end {
            let mut line = String::new();
            match self
                .reader
                .as_mut()
                .expect("BufReader was not initialized")
                .read_line(&mut line)
            {
                Ok(len) if len > 0 => {
                    self.current += len;
                    StreamElement::Item(line)
                }
                Ok(_) => {
                    self.terminated = true;
                    StreamElement::FlushAndRestart
                }
                Err(e) => panic!("Error while reading file: {:?}", e),
            }
        } else {
            self.terminated = true;
            StreamElement::FlushAndRestart
        };

        element
    }

    fn to_string(&self) -> String {
        format!("FileSource<{}>", std::any::type_name::<String>())
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<String, _>("FileSource");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }
}

impl Clone for FileSource {
    fn clone(&self) -> Self {
        assert!(
            self.reader.is_none(),
            "FileSource must be cloned before calling setup"
        );
        FileSource {
            path: self.path.clone(),
            reader: None,
            current: 0,
            end: 0,
            terminated: false,
        }
    }
}



/// Source that reads a text file line-by-line.
///
/// The file is divided in chunks and is read concurrently by multiple replicas.
#[derive(Debug)]
pub struct FileSourceAsync {
    path: PathBuf,
    // reader is initialized in `setup`, before it is None
    lines: Option<LinesStream<BufReaderAsync<FileAsync>>>,
    current: usize,
    end: usize,
    terminated: bool,
}

impl FileSourceAsync {

    /// Create a new source that reads the lines from a text file.
    ///
    /// The file is partitioned into as many chunks as replicas, each replica has to have the
    /// **same** file in the same path. It is guaranteed that each line of the file is emitted by
    /// exactly one replica.
    ///
    /// **Note**: the file must be readable and its size must be available. This means that only
    /// regular files can be read.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::FileSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let source = FileSource::new("/datasets/huge.txt");
    /// let s = env.stream(source);
    /// ```
    pub fn new<P>(path: P) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            path: path.into(),
            lines: Default::default(),
            current: 0,
            end: 0,
            terminated: false,
        }
    }
}


impl Source<String> for FileSourceAsync {
    fn get_max_parallelism(&self) -> Option<usize> {
        None
    }
}

impl Operator<String> for FileSourceAsync {
    fn setup(&mut self, metadata: ExecutionMetadata) {
        let global_id = metadata.global_id;
        let num_replicas = metadata.replicas.len();

        block_on(async move { // TODO: Change
            let file = FileAsync::open(&self.path).await.unwrap_or_else(|err| {
                panic!(
                    "FileSource: error while opening file {:?}: {:?}",
                    self.path, err
                )
            });
            let file_size = file.metadata().await.unwrap().len() as usize;
    
            let range_size = file_size / num_replicas;
            let start = range_size * global_id;
            self.current = start;
            self.end = if global_id == num_replicas - 1 {
                file_size
            } else {
                start + range_size
            };
    
            let mut reader = BufReaderAsync::new(file);
            // Seek reader to the first byte to be read
            reader
                .seek(SeekFrom::Current(start as i64))
                .await
                .expect("seek file");
            if global_id != 0 {
                // discard first line
                let mut s = String::new();
                self.current += reader
                    .read_line(&mut s)
                    .await
                    .expect("Cannot read line from file");
            }
            self.lines = Some(LinesStream::new(reader.lines()));
        })
    }

    fn next(&mut self) -> StreamElement<String> {
        todo!()
    }
    
    fn to_string(&self) -> String {
        format!("FileSourceAsync<{}>", std::any::type_name::<String>())
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<String, _>("FileSourceAsync");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }
}

impl futures::Stream for FileSourceAsync {
    type Item = StreamElement<String>;

    #[tracing::instrument(name = "file_source_async", skip_all)]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.terminated {
            tracing::trace!("emitting terminate, finished {}/{}", self.current, self.end);
            return Poll::Ready(Some(StreamElement::Terminate));
        }
        let element = if self.current <= self.end {
            match ready!(self.lines.as_mut().expect("FileSourceAsync not initialized!").poll_next_unpin(cx))
            {
                Some(Ok(line)) if line.len() > 0 => {
                    self.current += line.len();
                    StreamElement::Item(line)
                }
                Some(Err(e)) => panic!("Error while reading file: {:?}", e),
                _ => {
                    self.terminated = true;
                    StreamElement::FlushAndRestart
                }
            }
        } else {
            self.terminated = true;
            StreamElement::FlushAndRestart
        };

        Poll::Ready(Some(element))
    }
}

impl Clone for FileSourceAsync {
    fn clone(&self) -> Self {
        assert!(
            self.lines.is_none(),
            "FileSource must be cloned before calling setup"
        );
        FileSourceAsync {
            path: self.path.clone(),
            lines: None,
            current: 0,
            end: 0,
            terminated: false,
        }
    }
}