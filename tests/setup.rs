#![allow(clippy::implicit_hasher)]

use toydb::client::{Client, Pool};
use toydb::error::Result;
use toydb::server::Server;
use toydb::storage;

use futures_util::future::FutureExt as _;
use pretty_assertions::assert_eq;
use std::collections::HashMap;
use tempdir::TempDir;

// Movie data
pub fn movies() -> Vec<&'static str> {
    vec![
        "CREATE TABLE countries (
            id STRING PRIMARY KEY,
            name STRING NOT NULL
        )",
        "INSERT INTO countries VALUES
            ('fr', 'France'),
            ('ru', 'Russia'),
            ('us', 'United States of America')",
        "CREATE TABLE genres (
            id INTEGER PRIMARY KEY,
            name STRING NOT NULL
        )",
        "INSERT INTO genres VALUES
            (1, 'Science Fiction'),
            (2, 'Action'),
            (3, 'Comedy')",
        "CREATE TABLE studios (
            id INTEGER PRIMARY KEY,
            name STRING NOT NULL,
            country_id STRING REFERENCES countries
        )",
        "INSERT INTO studios VALUES
            (1, 'Mosfilm', 'ru'),
            (2, 'Lionsgate', 'us'),
            (3, 'StudioCanal', 'fr'),
            (4, 'Warner Bros', 'us')",
        "CREATE TABLE movies (
            id INTEGER PRIMARY KEY,
            title STRING NOT NULL,
            studio_id INTEGER NOT NULL REFERENCES studios,
            genre_id INTEGER NOT NULL REFERENCES genres,
            released INTEGER NOT NULL,
            rating FLOAT,
            ultrahd BOOLEAN
        )",
        "INSERT INTO movies VALUES
            (1, 'Stalker', 1, 1, 1979, 8.2, NULL),
            (2, 'Sicario', 2, 2, 2015, 7.6, TRUE),
            (3, 'Primer', 3, 1, 2004, 6.9, NULL),
            (4, 'Heat', 4, 2, 1995, 8.2, TRUE),
            (5, 'The Fountain', 4, 1, 2006, 7.2, FALSE),
            (6, 'Solaris', 1, 1, 1972, 8.1, NULL),
            (7, 'Gravity', 4, 1, 2013, 7.7, TRUE),
            (8, 'Blindspotting', 2, 3, 2018, 7.4, TRUE),
            (9, 'Birdman', 4, 3, 2014, 7.7, TRUE),
            (10, 'Inception', 4, 1, 2010, 8.8, TRUE)",
    ]
}

/// Simple data
pub fn simple() -> Vec<&'static str> {
    vec!["CREATE TABLE test (id INTEGER PRIMARY KEY, value STRING)"]
}

/// Sets up a test server
pub async fn server(
    id: &str,
    addr_sql: &str,
    addr_raft: &str,
    peers: HashMap<String, String>,
) -> Result<Teardown> {
    let dir = TempDir::new("toydb")?;
    let mut srv = Server::new(
        id,
        peers,
        Box::new(storage::log::Hybrid::new(&dir.path(), false)?),
        Box::new(storage::kv::Memory::new()),
    )
    .await?;

    srv = srv.listen(addr_sql, addr_raft).await?;
    let (task, abort) = srv.serve().remote_handle();
    tokio::spawn(task);

    Ok(Teardown::new(move || {
        std::mem::drop(abort);
        std::mem::drop(dir);
    }))
}

/// Sets up a server with a client
pub async fn server_with_client(queries: Vec<&str>) -> Result<(Client, Teardown)> {
    let teardown = server("test", "127.0.0.1:9605", "127.0.0.1:9705", HashMap::new()).await?;
    let client = Client::new("127.0.0.1:9605").await?;
    if !queries.is_empty() {
        client.execute("BEGIN").await?;
        for query in queries {
            client.execute(query).await?;
        }
        client.execute("COMMIT").await?;
    }
    Ok((client, teardown))
}

/// Sets up a server cluster
pub async fn cluster(nodes: HashMap<String, (String, String)>) -> Result<Teardown> {
    let mut teardown = Teardown::empty();
    for (id, (addr_sql, addr_raft)) in nodes.iter() {
        let peers = nodes
            .iter()
            .filter(|(i, _)| i != &id)
            .map(|(id, (_, raft))| (id.clone(), raft.clone()))
            .collect();
        teardown.merge(server(id, addr_sql, addr_raft, peers).await?);
    }
    Ok(teardown)
}

/// Sets up a server cluster with clients
pub async fn cluster_with_clients(
    size: u64,
    queries: Vec<&str>,
) -> Result<(Vec<Client>, Teardown)> {
    let mut nodes = HashMap::new();
    for i in 0..size {
        nodes.insert(
            format!("toydb{}", i),
            (format!("127.0.0.1:{}", 9605 + i), format!("127.0.0.1:{}", 9705 + i)),
        );
    }
    let teardown = cluster(nodes.clone()).await?;

    let mut clients = Vec::<Client>::new();
    for (id, (addr_sql, _)) in nodes {
        let client = Client::new(addr_sql).await?;
        assert_eq!(id, client.status().await?.raft.server);
        clients.push(client);
    }

    if !queries.is_empty() {
        let c = clients.get_mut(0).unwrap();
        c.execute("BEGIN").await?;
        for query in queries {
            c.execute(query).await?;
        }
        c.execute("COMMIT").await?;
    }

    Ok((clients, teardown))
}

/// Sets up a server cluster with a client pool
pub async fn cluster_with_pool(
    cluster_size: u64,
    pool_size: u64,
    queries: Vec<&str>,
) -> Result<(Pool, Teardown)> {
    let mut nodes = HashMap::new();
    for i in 0..cluster_size {
        nodes.insert(
            format!("toydb{}", i),
            (format!("127.0.0.1:{}", 9605 + i), format!("127.0.0.1:{}", 9705 + i)),
        );
    }
    let teardown = cluster(nodes.clone()).await?;

    let pool = Pool::new(nodes.into_iter().map(|(_, (addr, _))| addr).collect(), pool_size).await?;
    pool.get().await.status().await?;

    if !queries.is_empty() {
        let c = pool.get().await;
        c.execute("BEGIN").await?;
        for query in queries {
            c.execute(query).await?;
        }
        c.execute("COMMIT").await?;
    }

    Ok((pool, teardown))
}

/// Sets up a simple cluster with 3 clients and a test table
pub async fn cluster_simple() -> Result<(Client, Client, Client, Teardown)> {
    let (mut clients, teardown) = cluster_with_clients(3, simple()).await?;
    let a = clients.remove(0);
    let b = clients.remove(0);
    let c = clients.remove(0);

    Ok((a, b, c, teardown))
}

/// Tears down a test fixture when dropped.
pub struct Teardown {
    fns: Vec<Box<dyn FnOnce()>>,
}

impl Teardown {
    fn new<F: FnOnce() + 'static>(f: F) -> Self {
        let mut t = Self::empty();
        t.on_drop(f);
        t
    }

    fn empty() -> Self {
        Self { fns: Vec::new() }
    }

    fn on_drop<F: FnOnce() + 'static>(&mut self, f: F) {
        self.fns.push(Box::new(f))
    }

    fn merge(&mut self, mut other: Teardown) {
        while !other.fns.is_empty() {
            self.fns.push(other.fns.remove(0))
        }
    }
}

impl Drop for Teardown {
    fn drop(&mut self) {
        while !self.fns.is_empty() {
            self.fns.remove(0)()
        }
    }
}
