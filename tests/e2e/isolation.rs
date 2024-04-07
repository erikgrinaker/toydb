use super::{assert_row, assert_rows, dataset, TestCluster};

use serial_test::serial;
use toydb::error::{Error, Result};
use toydb::sql::types::Value;

#[test]
#[serial]
// A dirty write is when b overwrites an uncommitted value written by a.
fn dirty_write() -> Result<()> {
    let tc = TestCluster::run_with(5, dataset::TEST_TABLE)?;
    let mut a = tc.connect_any()?;
    let mut b = tc.connect_any()?;

    a.execute("BEGIN")?;
    a.execute("INSERT INTO test VALUES (1, 'a')")?;

    assert_eq!(b.execute("INSERT INTO test VALUES (1, 'b')"), Err(Error::Serialization));

    a.execute("COMMIT")?;
    assert_row(
        a.execute("SELECT * FROM test WHERE id = 1")?,
        vec![Value::Integer(1), Value::String("a".into())],
    );

    Ok(())
}

#[test]
#[serial]
// A dirty read is when b can read an uncommitted value set by a.
fn anomaly_dirty_read() -> Result<()> {
    let tc = TestCluster::run_with(5, dataset::TEST_TABLE)?;
    let mut a = tc.connect_any()?;
    let mut b = tc.connect_any()?;

    a.execute("BEGIN")?;
    a.execute("INSERT INTO test VALUES (1, 'a')")?;

    assert_rows(b.execute("SELECT * FROM test")?, vec![]);

    Ok(())
}

#[test]
#[serial]
// A lost update is when a and b both read a value and update it, where b's update replaces a.
fn anomaly_lost_update() -> Result<()> {
    let tc = TestCluster::run_with(5, dataset::TEST_TABLE)?;
    let mut a = tc.connect_any()?;
    let mut b = tc.connect_any()?;
    let mut c = tc.connect_any()?;

    c.execute("INSERT INTO test VALUES (1, 'c')")?;

    a.execute("BEGIN")?;
    b.execute("BEGIN")?;

    a.execute("UPDATE test SET value = 'a' WHERE id = 1")?;
    assert_eq!(b.execute("UPDATE test SET value = 'b' WHERE id = 1"), Err(Error::Serialization));
    a.execute("COMMIT")?;

    assert_row(
        c.execute("SELECT * FROM test WHERE id = 1")?,
        vec![Value::Integer(1), Value::String("a".into())],
    );

    Ok(())
}

#[test]
#[serial]
// A fuzzy (or unrepeatable) read is when b sees a value change after a updates it.
fn anomaly_fuzzy_read() -> Result<()> {
    let tc = TestCluster::run_with(5, dataset::TEST_TABLE)?;
    let mut a = tc.connect_any()?;
    let mut b = tc.connect_any()?;
    let mut c = tc.connect_any()?;

    c.execute("INSERT INTO test VALUES (1, 'c')")?;

    a.execute("BEGIN")?;
    b.execute("BEGIN")?;

    assert_row(
        b.execute("SELECT * FROM test WHERE id = 1")?,
        vec![Value::Integer(1), Value::String("c".into())],
    );
    a.execute("UPDATE test SET value = 'a' WHERE id = 1")?;
    a.execute("COMMIT")?;
    assert_row(
        b.execute("SELECT * FROM test WHERE id = 1")?,
        vec![Value::Integer(1), Value::String("c".into())],
    );

    Ok(())
}

#[test]
#[serial]
// Read skew is when a reads 1 and 2, but b modifies 2 in between the reads.
fn anomaly_read_skew() -> Result<()> {
    let tc = TestCluster::run_with(5, dataset::TEST_TABLE)?;
    let mut a = tc.connect_any()?;
    let mut b = tc.connect_any()?;
    let mut c = tc.connect_any()?;

    c.execute("INSERT INTO test VALUES (1, 'c'), (2, 'c')")?;

    a.execute("BEGIN")?;
    b.execute("BEGIN")?;

    assert_row(
        a.execute("SELECT * FROM test WHERE id = 1")?,
        vec![Value::Integer(1), Value::String("c".into())],
    );
    b.execute("UPDATE test SET value = 'b' WHERE id = 2")?;
    b.execute("COMMIT")?;
    assert_row(
        a.execute("SELECT * FROM test WHERE id = 2")?,
        vec![Value::Integer(2), Value::String("c".into())],
    );

    Ok(())
}

#[test]
#[serial]
// A phantom read is when a reads entries matching some predicate, but a modification by
// b changes the entries that match the predicate such that a later read by a returns them.
fn anomaly_phantom_read() -> Result<()> {
    let tc = TestCluster::run_with(5, dataset::TEST_TABLE)?;
    let mut a = tc.connect_any()?;
    let mut b = tc.connect_any()?;
    let mut c = tc.connect_any()?;

    c.execute("INSERT INTO test VALUES (1, 'true'), (2, 'false')")?;

    a.execute("BEGIN")?;
    b.execute("BEGIN")?;

    assert_rows(
        a.execute("SELECT * FROM test WHERE value = 'true'")?,
        vec![vec![Value::Integer(1), Value::String("true".into())]],
    );
    b.execute("UPDATE test SET value = 'true' WHERE id = 2")?;
    b.execute("COMMIT")?;
    assert_rows(
        a.execute("SELECT * FROM test WHERE value = 'true'")?,
        vec![vec![Value::Integer(1), Value::String("true".into())]],
    );

    Ok(())
}

// FIXME We should test write skew, but we need to implement serializable snapshot isolation first.
