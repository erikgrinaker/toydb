#![allow(clippy::implicit_hasher)]
#![allow(clippy::type_complexity)]

use toydb::client::Client;
use toydb::server::Server;
use toydb::Error;

use futures_util::future::FutureExt as _;
use pretty_assertions::assert_eq;
use std::collections::HashMap;

/// Sets up a test server, returning a teardown closure
pub async fn server(
    id: &str,
    addr_sql: &str,
    addr_raft: &str,
    peers: HashMap<String, String>,
) -> Result<Box<dyn FnOnce()>, Error> {
    let data_dir = tempdir::TempDir::new("toydb").unwrap();
    let srv = Server::new(
        id,
        peers.into_iter().map(|(k, v)| (k, v.parse().unwrap())).collect(),
        &data_dir.path().to_string_lossy(),
    )?;

    let (f, abort) = srv.listen(addr_sql.into(), addr_raft.into()).remote_handle();

    tokio::spawn(f);

    Ok(Box::new(move || {
        std::mem::drop(abort);
    }))
}

/// Sets up a server with a client
pub async fn server_with_client() -> Result<(Client, Box<dyn FnOnce()>), Error> {
    let teardown = server("test", "127.0.0.1:9605", "127.0.0.1:9705", HashMap::new()).await?;
    tokio::time::delay_for(std::time::Duration::from_millis(10)).await;
    let client = Client::new("127.0.0.1:9605").await?;
    Ok((client, teardown))
}

/// Sets up a server with a client and a s
pub async fn server_with_queries(queries: Vec<&str>) -> Result<(Client, Box<dyn FnOnce()>), Error> {
    let (client, teardown) = server_with_client().await?;
    client.execute("BEGIN").await?;
    for query in queries {
        client.execute(query).await?;
    }
    client.execute("COMMIT").await?;
    Ok((client, teardown))
}

/// Sets up a server cluster, returning a teardown closure
pub async fn cluster(nodes: HashMap<String, (String, String)>) -> Result<Box<dyn FnOnce()>, Error> {
    let mut teardowns = Vec::new();
    for (id, (addr_sql, addr_raft)) in nodes.iter() {
        let peers = nodes
            .iter()
            .filter(|(i, _)| i != &id)
            .map(|(id, (_, raft))| (id.clone(), raft.clone()))
            .collect();
        let teardown = server(id, addr_sql, addr_raft, peers).await?;
        teardowns.push(teardown);
    }
    Ok(Box::new(move || {
        for teardown in teardowns {
            teardown()
        }
    }))
}

/// Sets up a server cluster with clients, returning the clients and a teardown closure
pub async fn cluster_with_clients(size: u64) -> Result<(Vec<Client>, Box<dyn FnOnce()>), Error> {
    let mut nodes = HashMap::new();
    for i in 0..size {
        nodes.insert(
            format!("toydb{}", i),
            (format!("127.0.0.1:{}", 9605 + i), format!("127.0.0.1:{}", 9705 + i)),
        );
    }
    let teardown = cluster(nodes.clone()).await?;

    // FIXME Wait for cluster to start listening (should wait for listen future)
    tokio::time::delay_for(std::time::Duration::from_millis(20)).await;

    let mut clients = Vec::<Client>::new();
    for (id, (addr_sql, _)) in nodes {
        let client = Client::new(addr_sql).await?;
        assert_eq!(id, client.status().await?.id);
        clients.push(client);
    }
    Ok((clients, teardown))
}

/// Sets up a simple cluster with 3 clients and a test table
pub async fn cluster_simple() -> Result<(Client, Client, Client, Box<dyn FnOnce()>), Error> {
    let (mut clients, teardown) = cluster_with_clients(3).await?;
    let a = clients.remove(0);
    let b = clients.remove(0);
    let c = clients.remove(0);
    assert!(clients.is_empty());

    // FIXME Wait for cluster to stabilize, see: https://github.com/erikgrinaker/toydb/issues/19
    tokio::time::delay_for(std::time::Duration::from_millis(2000)).await;

    a.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value STRING)").await?;

    Ok((a, b, c, teardown))
}
