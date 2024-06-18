use serde_derive::{Deserialize, Serialize};

/// toyDB errors.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Error {
    /// The operation was aborted and must be retried. This typically happens
    /// with e.g. Raft leader changes.
    Abort,
    /// Invalid data, typically decoding errors or unexpected internal values.
    InvalidData(String),
    /// Invalid user input, typically parser or query errors.
    InvalidInput(String),
    /// An IO error.
    IO(String),
    /// A write was attempted in a read-only transaction.
    ReadOnly,
    /// A write transaction conflicted with a different writer and lost. The
    /// transaction must be retried.
    Serialization,
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::Abort => write!(f, "operation aborted"),
            Error::InvalidData(msg) => write!(f, "invalid data: {msg}"),
            Error::InvalidInput(msg) => write!(f, "invalid input: {msg}"),
            Error::IO(msg) => write!(f, "io error: {msg}"),
            Error::ReadOnly => write!(f, "read-only transaction"),
            Error::Serialization => write!(f, "serialization failure, retry transaction"),
        }
    }
}

impl Error {
    /// Returns whether the error is considered deterministic. State machine
    /// application needs to know whether a command failure is deterministic on
    /// the input command -- if it is, the command can be considered applied and
    /// the error returned to the client, but otherwise the state machine must
    /// panic to prevent replica divergence.
    pub fn is_deterministic(&self) -> bool {
        match self {
            // Aborts don't happen during application, only leader changes. But
            // we consider them non-deterministic in case a abort should happen
            // unexpectedly below Raft.
            Error::Abort => false,
            // Possible data corruption local to this node.
            Error::InvalidData(_) => false,
            // Input errors are (likely) deterministic. We could employ command
            // checksums to be sure.
            Error::InvalidInput(_) => true,
            // IO errors are typically node-local.
            Error::IO(_) => false,
            // Write commands in read-only transactions are deterministic.
            Error::ReadOnly => true,
            // Write conflicts are determinstic.
            Error::Serialization => true,
        }
    }
}

/// Constructs an Error::InvalidData via format!() and into().
#[macro_export]
macro_rules! errdata {
    ($($args:tt)*) => { $crate::error::Error::InvalidData(format!($($args)*)).into() };
}

/// Constructs an Error::InvalidInput via format!() and into().
#[macro_export]
macro_rules! errinput {
    ($($args:tt)*) => { $crate::error::Error::InvalidInput(format!($($args)*)).into() };
}

/// Result returning Error.
pub type Result<T> = std::result::Result<T, Error>;

impl<T> From<Error> for Result<T> {
    fn from(error: Error) -> Self {
        Err(error)
    }
}

impl serde::de::Error for Error {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Error::InvalidData(msg.to_string())
    }
}

impl serde::ser::Error for Error {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Error::InvalidData(msg.to_string())
    }
}

impl From<Box<bincode::ErrorKind>> for Error {
    fn from(err: Box<bincode::ErrorKind>) -> Self {
        Error::InvalidData(err.to_string())
    }
}

impl From<config::ConfigError> for Error {
    fn from(err: config::ConfigError) -> Self {
        Error::InvalidInput(err.to_string())
    }
}

impl From<crossbeam::channel::RecvError> for Error {
    fn from(err: crossbeam::channel::RecvError) -> Self {
        Error::IO(err.to_string())
    }
}

impl<T> From<crossbeam::channel::SendError<T>> for Error {
    fn from(err: crossbeam::channel::SendError<T>) -> Self {
        Error::IO(err.to_string())
    }
}

impl From<crossbeam::channel::TryRecvError> for Error {
    fn from(err: crossbeam::channel::TryRecvError) -> Self {
        Error::IO(err.to_string())
    }
}

impl<T> From<crossbeam::channel::TrySendError<T>> for Error {
    fn from(err: crossbeam::channel::TrySendError<T>) -> Self {
        Error::IO(err.to_string())
    }
}

impl From<hdrhistogram::CreationError> for Error {
    fn from(err: hdrhistogram::CreationError) -> Self {
        panic!("{err}")
    }
}

impl From<hdrhistogram::RecordError> for Error {
    fn from(err: hdrhistogram::RecordError) -> Self {
        Error::InvalidInput(err.to_string())
    }
}

impl From<log::ParseLevelError> for Error {
    fn from(err: log::ParseLevelError) -> Self {
        Error::InvalidInput(err.to_string())
    }
}

impl From<log::SetLoggerError> for Error {
    fn from(err: log::SetLoggerError) -> Self {
        panic!("{err}")
    }
}

impl From<regex::Error> for Error {
    fn from(err: regex::Error) -> Self {
        panic!("{err}")
    }
}

impl From<rustyline::error::ReadlineError> for Error {
    fn from(err: rustyline::error::ReadlineError) -> Self {
        Error::IO(err.to_string())
    }
}

impl From<std::array::TryFromSliceError> for Error {
    fn from(err: std::array::TryFromSliceError) -> Self {
        Error::InvalidData(err.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IO(err.to_string())
    }
}

impl From<std::num::ParseFloatError> for Error {
    fn from(err: std::num::ParseFloatError) -> Self {
        Error::InvalidInput(err.to_string())
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Self {
        Error::InvalidInput(err.to_string())
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(err: std::num::TryFromIntError) -> Self {
        Error::InvalidData(err.to_string())
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Error::InvalidData(err.to_string())
    }
}

impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        // This only happens when a different thread panics while holding a
        // mutex. This should be fatal, so we panic here too.
        panic!("{err}")
    }
}
