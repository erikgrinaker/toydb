use super::{assert_row, dataset, TestCluster};

use serial_test::serial;
use toydb::error::{Error, Result};
use toydb::sql::types::Value;

#[test]
#[serial]
// A client disconnect or termination should roll back its transaction.
fn client_disconnect_rollback() -> Result<()> {
    let tc = TestCluster::run_with(5, dataset::TEST_TABLE)?;
    let mut a = tc.connect_any()?;
    let mut b = tc.connect_any()?;

    a.execute("BEGIN")?;
    a.execute("INSERT INTO test VALUES (1, 'a')")?;
    std::mem::drop(a);

    // This would fail with a serialization error if the txn is not rolled back.
    b.execute("INSERT INTO test VALUES (1, 'b')")?;
    assert_row(
        b.execute("SELECT * FROM test WHERE id = 1")?,
        vec![Value::Integer(1), Value::String("b".into())],
    );

    Ok(())
}

#[test]
#[serial]
fn client_commit_error() -> Result<()> {
    let tc = TestCluster::run_with(5, dataset::TEST_TABLE)?;
    let mut a = tc.connect_any()?;
    let mut b = tc.connect_any()?;

    a.execute("BEGIN")?;
    a.execute("INSERT INTO test VALUES (1, 'a')")?;

    // When B gets a serialization error, it should still be in the txn and able to roll it back.
    b.execute("BEGIN")?;
    b.execute("INSERT INTO test VALUES (2, 'b')")?;
    assert_eq!(b.execute("INSERT INTO test VALUES (1, 'b')"), Err(Error::Serialization));
    b.execute("ROLLBACK")?;

    // Once rolled back, A should be able to write ID 2 and commit.
    a.execute("INSERT INTO test VALUES (2, 'a')")?;
    a.execute("COMMIT")?;

    Ok(())
}
