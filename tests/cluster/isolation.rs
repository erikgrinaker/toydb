use super::super::{assert_row, assert_rows, setup};

use toydb::sql::types::Value;
use toydb::Error;

use serial_test::serial;

#[tokio::test(core_threads = 2)]
#[serial]
// A dirty write is when b overwrites an uncommitted value written by a.
async fn anomaly_dirty_write() -> Result<(), Error> {
    let (a, b, _, _teardown) = setup::cluster_simple().await?;

    a.execute("BEGIN").await?;
    a.execute("INSERT INTO test VALUES (1, 'a')").await?;

    assert_eq!(b.execute("INSERT INTO test VALUES (1, 'b')").await, Err(Error::Serialization));

    a.execute("COMMIT").await?;
    assert_row(
        a.execute("SELECT * FROM test WHERE id = 1").await?,
        vec![Value::Integer(1), Value::String("a".into())],
    );

    Ok(())
}

#[tokio::test(core_threads = 2)]
#[serial]
// A dirty read is when b can read an uncommitted value set by a.
async fn anomaly_dirty_read() -> Result<(), Error> {
    let (a, b, _, _teardown) = setup::cluster_simple().await?;

    a.execute("BEGIN").await?;
    a.execute("INSERT INTO test VALUES (1, 'a')").await?;

    assert_rows(b.execute("SELECT * FROM test").await?, vec![]);

    Ok(())
}

#[tokio::test(core_threads = 2)]
#[serial]
// A lost update is when a and b both read a value and update it, where b's update replaces a.
async fn anomaly_lost_update() -> Result<(), Error> {
    let (a, b, c, _teardown) = setup::cluster_simple().await?;

    c.execute("INSERT INTO test VALUES (1, 'c')").await?;

    a.execute("BEGIN").await?;
    b.execute("BEGIN").await?;

    a.execute("UPDATE test SET value = 'a' WHERE id = 1").await?;
    assert_eq!(
        b.execute("UPDATE test SET value = 'b' WHERE id = 1").await,
        Err(Error::Serialization)
    );
    a.execute("COMMIT").await?;

    assert_row(
        c.execute("SELECT * FROM test WHERE id = 1").await?,
        vec![Value::Integer(1), Value::String("a".into())],
    );

    Ok(())
}

#[tokio::test(core_threads = 2)]
#[serial]
// A fuzzy (or unrepeatable) read is when b sees a value change after a updates it.
async fn anomaly_fuzzy_read() -> Result<(), Error> {
    let (a, b, c, _teardown) = setup::cluster_simple().await?;

    c.execute("INSERT INTO test VALUES (1, 'c')").await?;

    a.execute("BEGIN").await?;
    b.execute("BEGIN").await?;

    assert_row(
        b.execute("SELECT * FROM test WHERE id = 1").await?,
        vec![Value::Integer(1), Value::String("c".into())],
    );
    a.execute("UPDATE test SET value = 'a' WHERE id = 1").await?;
    a.execute("COMMIT").await?;
    assert_row(
        b.execute("SELECT * FROM test WHERE id = 1").await?,
        vec![Value::Integer(1), Value::String("c".into())],
    );

    Ok(())
}

#[tokio::test(core_threads = 2)]
#[serial]
// Read skew is when a reads 1 and 2, but b modifies 2 in between the reads.
async fn anomaly_read_skew() -> Result<(), Error> {
    let (a, b, c, _teardown) = setup::cluster_simple().await?;

    c.execute("INSERT INTO test VALUES (1, 'c'), (2, 'c')").await?;

    a.execute("BEGIN").await?;
    b.execute("BEGIN").await?;

    assert_row(
        a.execute("SELECT * FROM test WHERE id = 1").await?,
        vec![Value::Integer(1), Value::String("c".into())],
    );
    b.execute("UPDATE test SET value = 'b' WHERE id = 2").await?;
    b.execute("COMMIT").await?;
    assert_row(
        a.execute("SELECT * FROM test WHERE id = 2").await?,
        vec![Value::Integer(2), Value::String("c".into())],
    );

    Ok(())
}

#[tokio::test(core_threads = 2)]
#[serial]
// A phantom read is when a reads entries matching some predicate, but a modification by
// b changes the entries that match the predicate such that a later read by a returns them.
async fn anomaly_phantom_read() -> Result<(), Error> {
    let (a, b, c, _teardown) = setup::cluster_simple().await?;

    c.execute("INSERT INTO test VALUES (1, 'true'), (2, 'false')").await?;

    a.execute("BEGIN").await?;
    b.execute("BEGIN").await?;

    assert_rows(
        a.execute("SELECT * FROM test WHERE value = 'true'").await?,
        vec![vec![Value::Integer(1), Value::String("true".into())]],
    );
    b.execute("UPDATE test SET value = 'true' WHERE id = 2").await?;
    b.execute("COMMIT").await?;
    assert_rows(
        a.execute("SELECT * FROM test WHERE value = 'true'").await?,
        vec![vec![Value::Integer(1), Value::String("true".into())]],
    );

    Ok(())
}

// FIXME We should test write skew, but we need to implement serializable snapshot isolation first.
