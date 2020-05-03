use super::{Request, Response, Status};
use crate::Error;

use tokio::sync::{mpsc, oneshot};

/// A client for a local Raft server.
#[derive(Clone)]
pub struct Client {
    request_tx: mpsc::UnboundedSender<(Request, oneshot::Sender<Result<Response, Error>>)>,
}

impl Client {
    /// Creates a new Raft client.
    pub fn new(
        request_tx: mpsc::UnboundedSender<(Request, oneshot::Sender<Result<Response, Error>>)>,
    ) -> Self {
        Self { request_tx }
    }

    /// Executes a request against the Raft cluster.
    async fn request(&self, request: Request) -> Result<Response, Error> {
        let (response_tx, response_rx) = oneshot::channel();
        self.request_tx.send((request, response_tx))?;
        response_rx.await?
    }

    /// Mutates the Raft state machine.
    pub async fn mutate(&self, command: Vec<u8>) -> Result<Vec<u8>, Error> {
        match self.request(Request::Mutate(command)).await? {
            Response::State(response) => Ok(response),
            resp => Err(Error::Internal(format!("Unexpected Raft mutate response {:?}", resp))),
        }
    }

    /// Queries the Raft state machine.
    pub async fn query(&self, command: Vec<u8>) -> Result<Vec<u8>, Error> {
        match self.request(Request::Query(command)).await? {
            Response::State(response) => Ok(response),
            resp => Err(Error::Internal(format!("Unexpected Raft query response {:?}", resp))),
        }
    }

    /// Fetches Raft node status.
    pub async fn status(&self) -> Result<Status, Error> {
        match self.request(Request::Status).await? {
            Response::Status(status) => Ok(status),
            resp => Err(Error::Internal(format!("Unexpected Raft status response {:?}", resp))),
        }
    }
}
