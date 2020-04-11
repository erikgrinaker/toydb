use toydb::sql::types::Value;
use toydb::Error;

use super::super::setup;
use super::super::util::{assert_row, assert_rows};

use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
// A dirty write is when b overwrites an uncommitted value written by a.
fn anomaly_dirty_write() -> Result<(), Error> {
    let (mut a, mut b, _, teardown) = setup::cluster_simple()?;
    defer!(teardown());

    a.query("BEGIN")?;
    a.query("INSERT INTO test VALUES (1, 'a')")?;

    assert_eq!(b.query("INSERT INTO test VALUES (1, 'b')"), Err(Error::Serialization));

    a.query("COMMIT")?;
    assert_row(
        a.query("SELECT * FROM test WHERE id = 1")?,
        vec![Value::Integer(1), Value::String("a".into())],
    );

    Ok(())
}

#[test]
#[serial]
// A dirty read is when b can read an uncommitted value set by a.
fn anomaly_dirty_read() -> Result<(), Error> {
    let (mut a, mut b, _, teardown) = setup::cluster_simple()?;
    defer!(teardown());

    a.query("BEGIN")?;
    a.query("INSERT INTO test VALUES (1, 'a')")?;

    assert_rows(b.query("SELECT * FROM test")?, vec![]);

    Ok(())
}

#[test]
#[serial]
// A lost update is when a and b both read a value and update it, where b's update replaces a.
fn anomaly_lost_update() -> Result<(), Error> {
    let (mut a, mut b, mut c, teardown) = setup::cluster_simple()?;
    defer!(teardown());

    c.query("INSERT INTO test VALUES (1, 'c')")?;

    a.query("BEGIN")?;
    b.query("BEGIN")?;

    a.query("UPDATE test SET value = 'a' WHERE id = 1")?;
    assert_eq!(b.query("UPDATE test SET value = 'b' WHERE id = 1"), Err(Error::Serialization));
    a.query("COMMIT")?;

    assert_row(
        c.query("SELECT * FROM test WHERE id = 1")?,
        vec![Value::Integer(1), Value::String("a".into())],
    );

    Ok(())
}

#[test]
#[serial]
// A fuzzy (or unrepeatable) read is when b sees a value change after a updates it.
fn anomaly_fuzzy_read() -> Result<(), Error> {
    let (mut a, mut b, mut c, teardown) = setup::cluster_simple()?;
    defer!(teardown());

    c.query("INSERT INTO test VALUES (1, 'c')")?;

    a.query("BEGIN")?;
    b.query("BEGIN")?;

    assert_row(
        b.query("SELECT * FROM test WHERE id = 1")?,
        vec![Value::Integer(1), Value::String("c".into())],
    );
    a.query("UPDATE test SET value = 'a' WHERE id = 1")?;
    a.query("COMMIT")?;
    assert_row(
        b.query("SELECT * FROM test WHERE id = 1")?,
        vec![Value::Integer(1), Value::String("c".into())],
    );

    Ok(())
}

#[test]
#[serial]
// Read skew is when a reads 1 and 2, but b modifies 2 in between the reads.
fn anomaly_read_skew() -> Result<(), Error> {
    let (mut a, mut b, mut c, teardown) = setup::cluster_simple()?;
    defer!(teardown());

    c.query("INSERT INTO test VALUES (1, 'c'), (2, 'c')")?;

    a.query("BEGIN")?;
    b.query("BEGIN")?;

    assert_row(
        a.query("SELECT * FROM test WHERE id = 1")?,
        vec![Value::Integer(1), Value::String("c".into())],
    );
    b.query("UPDATE test SET value = 'b' WHERE id = 2")?;
    b.query("COMMIT")?;
    assert_row(
        a.query("SELECT * FROM test WHERE id = 2")?,
        vec![Value::Integer(2), Value::String("c".into())],
    );

    Ok(())
}

#[test]
#[serial]
// A phantom read is when a reads entries matching some predicate, but a modification by
// b changes the entries that match the predicate such that a later read by a returns them.
fn anomaly_phantom_read() -> Result<(), Error> {
    let (mut a, mut b, mut c, teardown) = setup::cluster_simple()?;
    defer!(teardown());

    c.query("INSERT INTO test VALUES (1, 'true'), (2, 'false')")?;

    a.query("BEGIN")?;
    b.query("BEGIN")?;

    assert_rows(
        a.query("SELECT * FROM test WHERE value = 'true'")?,
        vec![vec![Value::Integer(1), Value::String("true".into())]],
    );
    b.query("UPDATE test SET value = 'true' WHERE id = 2")?;
    b.query("COMMIT")?;
    assert_rows(
        a.query("SELECT * FROM test WHERE value = 'true'")?,
        vec![vec![Value::Integer(1), Value::String("true".into())]],
    );

    Ok(())
}

// FIXME We should test write skew, but we need to implement serializable snapshot isolation first.
