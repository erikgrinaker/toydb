use super::super::{assert_rows, setup};

use toydb::error::Result;
use toydb::sql::types::Value;

use futures::future::FutureExt as _;
use pretty_assertions::assert_eq;
use serial_test::serial;
use std::collections::HashSet;
use std::iter::FromIterator as _;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
#[allow(clippy::many_single_char_names)]
async fn get() -> Result<()> {
    let (pool, _teardown) = setup::cluster_with_pool(3, 5, setup::simple()).await?;

    // The clients are allocated to all servers
    let a = pool.get().await;
    let b = pool.get().await;
    let c = pool.get().await;
    let d = pool.get().await;
    let e = pool.get().await;

    let mut servers = HashSet::new();
    let mut ids = HashSet::new();
    for client in vec![a, b, c, d, e] {
        servers.insert(client.status().await?.raft.server);
        ids.insert(client.id());
    }
    assert_eq!(
        servers,
        HashSet::from_iter(vec!["toydb0".into(), "toydb1".into(), "toydb2".into()])
    );
    assert_eq!(ids, HashSet::from_iter(vec![0, 1, 2, 3, 4]));

    // Further clients won't be ready
    assert!(tokio::spawn(async move { pool.get().await.id() }).now_or_never().is_none());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn drop_rollback() -> Result<()> {
    let (pool, _teardown) = setup::cluster_with_pool(3, 1, setup::simple()).await?;

    // Starting a client and dropping it mid-transaction should work.
    let a = pool.get().await;
    assert_eq!(a.id(), 0);
    assert_eq!(a.txn(), None);
    a.execute("BEGIN").await?;
    a.execute("INSERT INTO test VALUES (1, 'a')").await?;
    assert_rows(
        a.execute("SELECT * FROM test").await?,
        vec![vec![Value::Integer(1), Value::String("a".into())]],
    );
    std::mem::drop(a);

    // Fetching the client again from the pool should have reset it.
    let a = pool.get().await;
    assert_eq!(a.id(), 0);
    assert_eq!(a.txn(), None);
    assert_rows(a.execute("SELECT * FROM test").await?, Vec::new());
    a.execute("BEGIN").await?;
    a.execute("INSERT INTO test VALUES (1, 'a')").await?;
    a.execute("COMMIT").await?;

    Ok(())
}
