pub enum Error {
    Config(String),
    IO(String),
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::Config(s) | Error::IO(s) => write!(f, "{}", s),
        }
    }
}

impl From<grpc::Error> for Error {
    fn from(err: grpc::Error) -> Self {
        Error::IO(err.to_string())
    }
}

impl From<httpbis::Error> for Error {
    fn from(err: httpbis::Error) -> Self {
        Error::IO(err.to_string())
    }
}

impl From<log::SetLoggerError> for Error {
    fn from(err: log::SetLoggerError) -> Self {
        Error::Config(err.to_string())
    }
}
