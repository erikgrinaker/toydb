use super::super::{assert_row, setup};

use toydb::error::Result;
use toydb::sql::types::Value;

use serial_test::serial;

#[tokio::test(core_threads = 2)]
#[serial]
// A client disconnect or termination should roll back its transaction.
async fn client_rollback() -> Result<()> {
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
