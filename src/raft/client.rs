use super::Status;
use crate::Error;

use tokio::runtime;
use tokio::sync::{mpsc, oneshot};

/// A client request.
#[derive(Debug)]
pub enum Request {
    Query(Vec<u8>),
    Mutate(Vec<u8>),
    Status,
}

/// A client response.
#[derive(Debug)]
pub enum Response {
    State(Vec<u8>),
    Status(Status),
    Error(Error),
}

/// A client for a local Raft server.
#[derive(Clone)]
pub struct Client {
    request_tx: mpsc::UnboundedSender<(Request, oneshot::Sender<Response>)>,
    runtime: runtime::Handle,
}

impl Client {
    /// Creates a new Raft client.
    pub async fn new(
        request_tx: mpsc::UnboundedSender<(Request, oneshot::Sender<Response>)>,
    ) -> Self {
        Self { request_tx, runtime: runtime::Handle::current() }
    }

    /// Executes a request against the Raft cluster.
    async fn request(&self, request: Request) -> Result<Response, Error> {
        let (response_tx, response_rx) = oneshot::channel();
        self.request_tx.send((request, response_tx))?;
        match response_rx.await? {
            Response::Error(error) => Err(error),
            response => Ok(response),
        }
    }

    /// Mutates the Raft state machine.
    pub async fn mutate(&self, command: Vec<u8>) -> Result<Vec<u8>, Error> {
        match self.request(Request::Mutate(command)).await? {
            Response::State(response) => Ok(response),
            resp => Err(Error::Internal(format!("Unexpected Raft mutate response {:?}", resp))),
        }
    }

    /// Mutates the Raft machine synchronously.
    pub fn mutate_sync(&self, command: Vec<u8>) -> Result<Vec<u8>, Error> {
        self.runtime.enter(|| futures::executor::block_on(self.mutate(command)))
    }

    /// Queries the Raft state machine.
    pub async fn query(&self, command: Vec<u8>) -> Result<Vec<u8>, Error> {
        match self.request(Request::Query(command)).await? {
            Response::State(response) => Ok(response),
            resp => Err(Error::Internal(format!("Unexpected Raft query response {:?}", resp))),
        }
    }

    /// Queries the Raft state machine synchronously.
    pub fn query_sync(&self, command: Vec<u8>) -> Result<Vec<u8>, Error> {
        self.runtime.enter(|| futures::executor::block_on(self.query(command)))
    }

    /// Fetches Raft node status.
    pub async fn status(&self) -> Result<Status, Error> {
        match self.request(Request::Status).await? {
            Response::Status(status) => Ok(status),
            resp => Err(Error::Internal(format!("Unexpected Raft status response {:?}", resp))),
        }
    }

    /// Fetches Raft node status synchronously.
    pub fn status_sync(&self) -> Result<Status, Error> {
        self.runtime.enter(|| futures::executor::block_on(self.status()))
    }
}
