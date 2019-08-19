use crate::service;
use crate::Error;
use grpc::ClientStubExt;
use service::ToyDB;

/// A ToyDB client
pub struct Client {
    client: service::ToyDBClient,
}

impl Client {
    /// Creates a new client
    pub fn new(addr: std::net::SocketAddr) -> Result<Self, Error> {
        Ok(Self {
            client: service::ToyDBClient::new_plain(
                &addr.ip().to_string(),
                addr.port(),
                grpc::ClientConf::new(),
            )?,
        })
    }

    /// Runs an echo request
    pub fn echo(&self, value: &str) -> Result<String, Error> {
        let (_, resp, _) = self.client.echo(
            grpc::RequestOptions::new(),
            service::EchoRequest { value: value.to_owned(), ..Default::default() },
        ).wait()?;
        Ok(resp.value)
    }
}
