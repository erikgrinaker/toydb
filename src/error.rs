#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    RaftBaseNotFound { index: u64, term: u64 },
    Serialization,
    ReadOnly,
    Config(String),
    IO(String),
    Internal(String),
    Network(String),
    Parse(String),
    Value(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::Config(s)
            | Error::IO(s)
            | Error::Internal(s)
            | Error::Network(s)
            | Error::Parse(s)
            | Error::Value(s) => write!(f, "{}", s),
            Error::RaftBaseNotFound { index, term } => {
                write!(f, "Base entry at index {} with term {} not found", index, term)
            }
            Error::Serialization => write!(f, "Serialization failure, retry transaction"),
            Error::ReadOnly => write!(f, "Read-only transaction"),
        }
    }
}

impl From<config::ConfigError> for Error {
    fn from(err: config::ConfigError) -> Self {
        Error::Config(err.to_string())
    }
}

impl From<crossbeam_channel::RecvError> for Error {
    fn from(err: crossbeam_channel::RecvError) -> Self {
        Error::Network(err.to_string())
    }
}

impl<T> From<crossbeam_channel::SendError<T>> for Error {
    fn from(err: crossbeam_channel::SendError<T>) -> Self {
        Error::Network(err.to_string())
    }
}

impl From<grpc::Error> for Error {
    fn from(err: grpc::Error) -> Self {
        Error::Network(err.to_string())
    }
}

impl From<httpbis::Error> for Error {
    fn from(err: httpbis::Error) -> Self {
        Error::Network(err.to_string())
    }
}

impl From<log::ParseLevelError> for Error {
    fn from(err: log::ParseLevelError) -> Self {
        Error::Config(err.to_string())
    }
}

impl From<log::SetLoggerError> for Error {
    fn from(err: log::SetLoggerError) -> Self {
        Error::Config(err.to_string())
    }
}

impl From<regex::Error> for Error {
    fn from(err: regex::Error) -> Self {
        Error::Value(err.to_string())
    }
}

impl From<rmps::decode::Error> for Error {
    fn from(err: rmps::decode::Error) -> Self {
        Error::IO(err.to_string())
    }
}

impl From<rmps::encode::Error> for Error {
    fn from(err: rmps::encode::Error) -> Self {
        Error::IO(err.to_string())
    }
}

impl From<rustyline::error::ReadlineError> for Error {
    fn from(err: rustyline::error::ReadlineError) -> Self {
        Error::Internal(err.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IO(err.to_string())
    }
}

impl From<std::net::AddrParseError> for Error {
    fn from(err: std::net::AddrParseError) -> Self {
        Error::Network(err.to_string())
    }
}

impl From<std::num::ParseFloatError> for Error {
    fn from(err: std::num::ParseFloatError) -> Self {
        Error::Parse(err.to_string())
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Self {
        Error::Parse(err.to_string())
    }
}

impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        Error::Internal(err.to_string())
    }
}
