mod pool;

use super::{assert_row, assert_rows, setup};

use toydb::error::{Error, Result};
use toydb::raft;
use toydb::sql::engine::{Mode, Status};
use toydb::sql::execution::ResultSet;
use toydb::sql::schema;
use toydb::sql::types::{Column, DataType, Value};
use toydb::storage::kv;
use toydb::Client;

use pretty_assertions::assert_eq;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn get_table() -> Result<()> {
    let (c, _teardown) = setup::server_with_client(setup::movies()).await?;

    assert_eq!(
        c.get_table("unknown").await,
        Err(Error::Value("Table unknown does not exist".into()))
    );
    assert_eq!(
        c.get_table("movies").await?,
        schema::Table {
            name: "movies".into(),
            columns: vec![
                schema::Column {
                    name: "id".into(),
                    datatype: DataType::Integer,
                    primary_key: true,
                    nullable: false,
                    default: None,
                    unique: true,
                    index: false,
                    references: None,
                },
                schema::Column {
                    name: "title".into(),
                    datatype: DataType::String,
                    primary_key: false,
                    nullable: false,
                    default: None,
                    unique: false,
                    index: false,
                    references: None,
                },
                schema::Column {
                    name: "studio_id".into(),
                    datatype: DataType::Integer,
                    primary_key: false,
                    nullable: false,
                    default: None,
                    unique: false,
                    index: false,
                    references: Some("studios".into()),
                },
                schema::Column {
                    name: "genre_id".into(),
                    datatype: DataType::Integer,
                    primary_key: false,
                    nullable: false,
                    default: None,
                    unique: false,
                    index: false,
                    references: Some("genres".into()),
                },
                schema::Column {
                    name: "released".into(),
                    datatype: DataType::Integer,
                    primary_key: false,
                    nullable: false,
                    default: None,
                    unique: false,
                    index: false,
                    references: None,
                },
                schema::Column {
                    name: "rating".into(),
                    datatype: DataType::Float,
                    primary_key: false,
                    nullable: true,
                    default: Some(Value::Null),
                    unique: false,
                    index: false,
                    references: None,
                },
                schema::Column {
                    name: "ultrahd".into(),
                    datatype: DataType::Boolean,
                    primary_key: false,
                    nullable: true,
                    default: Some(Value::Null),
                    unique: false,
                    index: false,
                    references: None,
                },
            ]
        }
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn list_tables() -> Result<()> {
    let (c, _teardown) = setup::server_with_client(setup::movies()).await?;

    assert_eq!(c.list_tables().await?, vec!["countries", "genres", "movies", "studios"]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn status() -> Result<()> {
    let (c, _teardown) = setup::server_with_client(setup::movies()).await?;

    assert_eq!(
        c.status().await?,
        Status {
            raft: raft::Status {
                server: "test".into(),
                leader: "test".into(),
                term: 0,
                node_last_index: vec![("test".to_string(), 26)].into_iter().collect(),
                commit_index: 26,
                apply_index: 26,
                storage: "hybrid".into(),
                storage_size: 3239,
            },
            mvcc: kv::mvcc::Status { txns: 1, txns_active: 0, storage: "memory".into() },
        }
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn execute() -> Result<()> {
    let (c, _teardown) = setup::server_with_client(setup::movies()).await?;

    // SELECT
    let result = c.execute("SELECT * FROM genres").await?;
    assert_eq!(
        result,
        ResultSet::Query {
            columns: vec![Column { name: Some("id".into()) }, Column { name: Some("name".into()) }],
            rows: Box::new(std::iter::empty()),
        }
    );
    assert_rows(
        result,
        vec![
            vec![Value::Integer(1), Value::String("Science Fiction".into())],
            vec![Value::Integer(2), Value::String("Action".into())],
            vec![Value::Integer(3), Value::String("Comedy".into())],
        ],
    );

    let result = c.execute("SELECT * FROM genres WHERE FALSE").await?;
    assert_eq!(
        result,
        ResultSet::Query {
            columns: vec![Column { name: Some("id".into()) }, Column { name: Some("name".into()) }],
            rows: Box::new(std::iter::empty()),
        }
    );
    assert_rows(result, Vec::new());

    assert_eq!(
        c.execute("SELECT * FROM x").await,
        Err(Error::Value("Table x does not exist".into()))
    );

    // INSERT
    assert_eq!(
        c.execute("INSERT INTO genres VALUES (1, 'Western')").await,
        Err(Error::Value("Primary key 1 already exists for table genres".into())),
    );
    assert_eq!(
        c.execute("INSERT INTO genres VALUES (9, 'Western')").await,
        Ok(ResultSet::Create { count: 1 }),
    );
    assert_eq!(
        c.execute("INSERT INTO x VALUES (9, 'Western')").await,
        Err(Error::Value("Table x does not exist".into()))
    );

    // UPDATE
    assert_eq!(
        c.execute("UPDATE genres SET name = 'Horror' WHERE FALSE").await,
        Ok(ResultSet::Update { count: 0 }),
    );
    assert_eq!(
        c.execute("UPDATE genres SET name = 'Horror' WHERE id = 9").await,
        Ok(ResultSet::Update { count: 1 }),
    );
    assert_eq!(
        c.execute("UPDATE genres SET id = 1 WHERE id = 9").await,
        Err(Error::Value("Primary key 1 already exists for table genres".into()))
    );

    // DELETE
    assert_eq!(
        c.execute("DELETE FROM genres WHERE FALSE").await,
        Ok(ResultSet::Delete { count: 0 }),
    );
    assert_eq!(
        c.execute("DELETE FROM genres WHERE id = 9").await,
        Ok(ResultSet::Delete { count: 1 }),
    );
    assert_eq!(
        c.execute("DELETE FROM genres WHERE x = 1").await,
        Err(Error::Value("Unknown field x".into()))
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn execute_txn() -> Result<()> {
    let (c, _teardown) = setup::server_with_client(setup::movies()).await?;

    assert_eq!(c.txn(), None);

    // Committing a change in a txn should work
    assert_eq!(c.execute("BEGIN").await?, ResultSet::Begin { id: 2, mode: Mode::ReadWrite });
    assert_eq!(c.txn(), Some((2, Mode::ReadWrite)));
    c.execute("INSERT INTO genres VALUES (4, 'Drama')").await?;
    assert_eq!(c.execute("COMMIT").await?, ResultSet::Commit { id: 2 });
    assert_row(
        c.execute("SELECT * FROM genres WHERE id = 4").await?,
        vec![Value::Integer(4), Value::String("Drama".into())],
    );
    assert_eq!(c.txn(), None);

    // Rolling back a change in a txn should also work
    assert_eq!(c.execute("BEGIN").await?, ResultSet::Begin { id: 4, mode: Mode::ReadWrite });
    assert_eq!(c.txn(), Some((4, Mode::ReadWrite)));
    c.execute("INSERT INTO genres VALUES (5, 'Musical')").await?;
    assert_row(
        c.execute("SELECT * FROM genres WHERE id = 5").await?,
        vec![Value::Integer(5), Value::String("Musical".into())],
    );
    assert_eq!(c.execute("ROLLBACK").await?, ResultSet::Rollback { id: 4 });
    assert_rows(c.execute("SELECT * FROM genres WHERE id = 5").await?, Vec::new());
    assert_eq!(c.txn(), None);

    // Starting a read-only txn should block writes
    assert_eq!(
        c.execute("BEGIN READ ONLY").await?,
        ResultSet::Begin { id: 6, mode: Mode::ReadOnly }
    );
    assert_eq!(c.txn(), Some((6, Mode::ReadOnly)));
    assert_row(
        c.execute("SELECT * FROM genres WHERE id = 4").await?,
        vec![Value::Integer(4), Value::String("Drama".into())],
    );
    assert_eq!(c.execute("INSERT INTO genres VALUES (5, 'Musical')").await, Err(Error::ReadOnly));
    assert_row(
        c.execute("SELECT * FROM genres WHERE id = 4").await?,
        vec![Value::Integer(4), Value::String("Drama".into())],
    );
    assert_eq!(c.execute("COMMIT").await?, ResultSet::Commit { id: 6 });

    // Starting a time-travel txn should work, it shouldn't see recent changes, and it should
    // block writes
    assert_eq!(
        c.execute("BEGIN READ ONLY AS OF SYSTEM TIME 1").await?,
        ResultSet::Begin { id: 7, mode: Mode::Snapshot { version: 1 } }
    );
    assert_eq!(c.txn(), Some((7, Mode::Snapshot { version: 1 })));
    assert_rows(
        c.execute("SELECT * FROM genres").await?,
        vec![
            vec![Value::Integer(1), Value::String("Science Fiction".into())],
            vec![Value::Integer(2), Value::String("Action".into())],
            vec![Value::Integer(3), Value::String("Comedy".into())],
        ],
    );
    assert_eq!(c.execute("INSERT INTO genres VALUES (5, 'Musical')").await, Err(Error::ReadOnly));
    assert_eq!(c.execute("COMMIT").await?, ResultSet::Commit { id: 7 });

    // A txn should still be usable after an error occurs
    assert_eq!(c.execute("BEGIN").await?, ResultSet::Begin { id: 8, mode: Mode::ReadWrite });
    c.execute("INSERT INTO genres VALUES (5, 'Horror')").await?;
    assert_eq!(
        c.execute("INSERT INTO genres VALUES (5, 'Musical')").await,
        Err(Error::Value("Primary key 5 already exists for table genres".into()))
    );
    assert_eq!(c.txn(), Some((8, Mode::ReadWrite)));
    c.execute("INSERT INTO genres VALUES (6, 'Western')").await?;
    assert_eq!(c.execute("COMMIT").await?, ResultSet::Commit { id: 8 });
    assert_rows(
        c.execute("SELECT * FROM genres").await?,
        vec![
            vec![Value::Integer(1), Value::String("Science Fiction".into())],
            vec![Value::Integer(2), Value::String("Action".into())],
            vec![Value::Integer(3), Value::String("Comedy".into())],
            vec![Value::Integer(4), Value::String("Drama".into())],
            vec![Value::Integer(5), Value::String("Horror".into())],
            vec![Value::Integer(6), Value::String("Western".into())],
        ],
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn execute_txn_concurrent() -> Result<()> {
    let (a, _teardown) = setup::server_with_client(setup::movies()).await?;
    let b = Client::new("127.0.0.1:9605").await?;

    // Concurrent updates should throw a serialization failure on conflict.
    assert_eq!(a.execute("BEGIN").await?, ResultSet::Begin { id: 2, mode: Mode::ReadWrite });
    assert_eq!(b.execute("BEGIN").await?, ResultSet::Begin { id: 3, mode: Mode::ReadWrite });

    assert_row(
        a.execute("SELECT * FROM genres WHERE id = 1").await?,
        vec![Value::Integer(1), Value::String("Science Fiction".into())],
    );
    assert_row(
        b.execute("SELECT * FROM genres WHERE id = 1").await?,
        vec![Value::Integer(1), Value::String("Science Fiction".into())],
    );

    assert_eq!(
        a.execute("UPDATE genres SET name = 'x' WHERE id = 1").await,
        Ok(ResultSet::Update { count: 1 })
    );
    assert_eq!(
        b.execute("UPDATE genres SET name = 'y' WHERE id = 1").await,
        Err(Error::Serialization)
    );

    assert_eq!(a.execute("COMMIT").await, Ok(ResultSet::Commit { id: 2 }));
    assert_eq!(b.execute("ROLLBACK").await, Ok(ResultSet::Rollback { id: 3 }));

    assert_row(
        a.execute("SELECT * FROM genres WHERE id = 1").await?,
        vec![Value::Integer(1), Value::String("x".into())],
    );

    Ok(())
}
