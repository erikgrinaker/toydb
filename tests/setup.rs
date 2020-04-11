use toydb::client::Client;
use toydb::server::Server;
use toydb::Error;

use pretty_assertions::assert_eq;
use std::collections::HashMap;

/// Sets up a test server, returning a teardown closure
pub fn server(
    id: &str,
    addr: &str,
    peers: HashMap<String, String>,
) -> Result<Box<dyn FnOnce()>, Error> {
    let data_dir = tempdir::TempDir::new("toydb")?;
    let mut srv = Server::new(
        id,
        peers.into_iter().map(|(k, v)| (k, v.parse().unwrap())).collect(),
        &data_dir.path().to_string_lossy(),
    )?;
    srv.listen(addr, 4)?;

    Ok(Box::new(move || {
        srv.shutdown().unwrap();
        std::mem::drop(data_dir)
    }))
}

/// Sets up a server cluster, returning a teardown closure
pub fn cluster(nodes: HashMap<String, String>) -> Result<Box<dyn FnOnce()>, Error> {
    let mut teardowns = Vec::new();
    for (id, addr) in nodes.iter() {
        let mut peers = nodes.clone();
        peers.remove(id);
        let teardown = server(id, addr, peers)?;
        teardowns.push(teardown);
    }
    Ok(Box::new(move || {
        for teardown in teardowns {
            teardown()
        }
    }))
}

/// Sets up a server cluster with clients, returning the clients and a teardown closure
pub fn cluster_with_clients(size: u64) -> Result<(Vec<Client>, Box<dyn FnOnce()>), Error> {
    let mut nodes = HashMap::new();
    for i in 0..size {
        nodes.insert(format!("toydb{}", i), format!("127.0.0.1:{}", 9605 + i));
    }
    let teardown = cluster(nodes.clone())?;
    let mut clients = Vec::<Client>::new();
    for (id, addr) in nodes {
        let mut parts = addr.split(":");
        let client = Client::new(parts.next().unwrap(), parts.next().unwrap().parse()?)?;
        assert_eq!(id, client.status()?.id);
        clients.push(client);
    }
    Ok((clients, teardown))
}

/// Sets up a simple cluster with 3 clients and a test table
pub fn cluster_simple() -> Result<(Client, Client, Client, Box<dyn FnOnce()>), Error> {
    let (mut clients, teardown) = cluster_with_clients(3)?;
    let mut a = clients.remove(0);
    let b = clients.remove(0);
    let c = clients.remove(0);
    assert!(clients.is_empty());

    // FIXME Wait for cluster to stabilize, see: https://github.com/erikgrinaker/toydb/issues/19
    std::thread::sleep(std::time::Duration::from_millis(2000));

    a.query("CREATE TABLE test (id INTEGER PRIMARY KEY, value STRING)")?;

    Ok((a, b, c, teardown))
}
