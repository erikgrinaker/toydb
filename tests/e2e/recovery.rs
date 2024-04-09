use super::{assert_row, dataset, TestCluster};

use serial_test::serial;
use toydb::error::{Error, Result};
use toydb::sql::types::Value;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
// A client disconnect or termination should roll back its transaction.
async fn client_disconnect_rollback() -> Result<()> {
    let tc = TestCluster::run_with(5, dataset::TEST_TABLE).await?;
    let mut a = tc.connect_any().await?;
    let mut b = tc.connect_any().await?;

    a.execute("BEGIN").await?;
    a.execute("INSERT INTO test VALUES (1, 'a')").await?;
    std::mem::drop(a);

    // This would fail with a serialization error if the txn is not rolled back.
    b.execute("INSERT INTO test VALUES (1, 'b')").await?;
    assert_row(
        b.execute("SELECT * FROM test WHERE id = 1").await?,
        vec![Value::Integer(1), Value::String("b".into())],
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn client_commit_error() -> Result<()> {
    let tc = TestCluster::run_with(5, dataset::TEST_TABLE).await?;
    let mut a = tc.connect_any().await?;
    let mut b = tc.connect_any().await?;

    a.execute("BEGIN").await?;
    a.execute("INSERT INTO test VALUES (1, 'a')").await?;

    // When B gets a serialization error, it should still be in the txn and able to roll it back.
    b.execute("BEGIN").await?;
    b.execute("INSERT INTO test VALUES (2, 'b')").await?;
    assert_eq!(b.execute("INSERT INTO test VALUES (1, 'b')").await, Err(Error::Serialization));
    b.execute("ROLLBACK").await?;

    // Once rolled back, A should be able to write ID 2 and commit.
    a.execute("INSERT INTO test VALUES (2, 'a')").await?;
    a.execute("COMMIT").await?;

    Ok(())
}
