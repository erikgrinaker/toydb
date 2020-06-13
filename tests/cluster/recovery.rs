use super::super::{assert_row, setup};

use toydb::error::{Error, Result};
use toydb::sql::types::Value;

use serial_test::serial;

#[tokio::test(core_threads = 2)]
#[serial]
// A client disconnect or termination should roll back its transaction.
async fn client_disconnect_rollback() -> Result<()> {
    let (a, b, _, _teardown) = setup::cluster_simple().await?;

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

#[tokio::test(core_threads = 2)]
#[serial]
async fn client_commit_error() -> Result<()> {
    let (a, b, _, _teardown) = setup::cluster_simple().await?;

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
