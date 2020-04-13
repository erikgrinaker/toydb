use toydb::client::Client;
use toydb::server::Status;
use toydb::sql::engine::Mode;
use toydb::sql::execution::ResultSet;
use toydb::sql::schema;
use toydb::sql::types::{Column, DataType, Relation, Row, Value};
use toydb::Error;

use pretty_assertions::assert_eq;
use serial_test::serial;
use std::collections::HashMap;

#[allow(clippy::type_complexity)]
fn setup(queries: Vec<&str>) -> Result<(Client, Box<dyn FnOnce()>), Error> {
    let data_dir = tempdir::TempDir::new("toydb")?;
    let mut srv = toydb::Server::new("test", HashMap::new(), &data_dir.path().to_string_lossy())?;
    srv.listen("127.0.0.1:9605", 4)?;

    let mut client = toydb::Client::new("127.0.0.1", 9605)?;
    client.query("BEGIN")?;
    for query in queries {
        client.query(&query)?;
    }
    client.query("COMMIT")?;

    let client = toydb::Client::new("127.0.0.1", 9605)?;

    Ok((
        client,
        Box::new(move || {
            srv.shutdown().unwrap();
            std::mem::drop(data_dir)
        }),
    ))
}

#[allow(clippy::type_complexity)]
fn setup_movies() -> Result<(toydb::Client, Box<dyn FnOnce()>), Error> {
    setup(vec![
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
    ])
}

#[test]
#[serial]
fn get_table() -> Result<(), Error> {
    let (c, teardown) = setup_movies()?;
    defer!(teardown());

    assert_eq!(c.get_table("unknown"), Err(Error::Value("Table unknown does not exist".into())));
    assert_eq!(
        c.get_table("movies")?,
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

#[test]
#[serial]
fn list_tables() -> Result<(), Error> {
    let (c, teardown) = setup_movies()?;
    defer!(teardown());

    assert_eq!(c.list_tables()?, vec!["countries", "genres", "movies", "studios"]);
    Ok(())
}

#[test]
#[serial]
fn status() -> Result<(), Error> {
    let (c, teardown) = setup_movies()?;
    defer!(teardown());

    assert_eq!(
        c.status()?,
        Status { id: "test".into(), version: env!("CARGO_PKG_VERSION").into() }
    );
    Ok(())
}

#[test]
#[serial]
fn query() -> Result<(), Error> {
    // The SQL engine is thoroughly tested in a separate suite, this just exercises the
    // basic client/server integration.
    let (mut c, teardown) = setup_movies()?;
    defer!(teardown());

    // SELECT
    let result = c.query("SELECT * FROM genres")?;
    assert_eq!(
        result,
        ResultSet::Query {
            relation: Relation {
                columns: vec![
                    Column { relation: Some("genres".into()), name: Some("id".into()) },
                    Column { relation: Some("genres".into()), name: Some("name".into()) }
                ],
                rows: None
            }
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

    let result = c.query("SELECT * FROM genres WHERE FALSE")?;
    assert_eq!(
        result,
        ResultSet::Query {
            relation: Relation {
                columns: vec![
                    Column { relation: Some("genres".into()), name: Some("id".into()) },
                    Column { relation: Some("genres".into()), name: Some("name".into()) }
                ],
                rows: None
            }
        }
    );
    assert_rows(result, Vec::new());

    assert_eq!(c.query("SELECT * FROM x"), Err(Error::Value("Table x does not exist".into())));

    // INSERT
    assert_eq!(
        c.query("INSERT INTO genres VALUES (1, 'Western')"),
        Err(Error::Value("Primary key 1 already exists for table genres".into())),
    );
    assert_eq!(
        c.query("INSERT INTO genres VALUES (9, 'Western')"),
        Ok(ResultSet::Create { count: 1 }),
    );
    assert_eq!(
        c.query("INSERT INTO x VALUES (9, 'Western')"),
        Err(Error::Value("Table x does not exist".into()))
    );

    // UPDATE
    assert_eq!(
        c.query("UPDATE genres SET name = 'Horror' WHERE FALSE"),
        Ok(ResultSet::Update { count: 0 }),
    );
    assert_eq!(
        c.query("UPDATE genres SET name = 'Horror' WHERE id = 9"),
        Ok(ResultSet::Update { count: 1 }),
    );
    assert_eq!(
        c.query("UPDATE genres SET id = 1 WHERE id = 9"),
        Err(Error::Value("Primary key 1 already exists for table genres".into()))
    );

    // DELETE
    assert_eq!(c.query("DELETE FROM genres WHERE FALSE"), Ok(ResultSet::Delete { count: 0 }),);
    assert_eq!(c.query("DELETE FROM genres WHERE id = 9"), Ok(ResultSet::Delete { count: 1 }),);
    assert_eq!(
        c.query("DELETE FROM genres WHERE x = 1"),
        Err(Error::Value("Unknown field x".into()))
    );

    Ok(())
}

#[test]
#[serial]
fn query_txn() -> Result<(), Error> {
    let (mut c, teardown) = setup_movies()?;
    defer!(teardown());

    // Committing a change in a txn should work
    assert_eq!(c.txn(), None);
    assert_eq!(c.query("BEGIN")?, ResultSet::Begin { id: 2, mode: Mode::ReadWrite });
    assert_eq!(c.txn(), Some((2, Mode::ReadWrite)));

    c.query("INSERT INTO genres VALUES (4, 'Drama')")?;
    assert_eq!(c.txn(), Some((2, Mode::ReadWrite)));

    assert_eq!(c.query("COMMIT")?, ResultSet::Commit { id: 2 });
    assert_eq!(c.txn(), None);

    assert_rows(
        c.query("SELECT * FROM genres WHERE id = 4")?,
        vec![vec![Value::Integer(4), Value::String("Drama".into())]],
    );

    // Rolling back a change in a txn should also work
    assert_eq!(c.query("BEGIN")?, ResultSet::Begin { id: 4, mode: Mode::ReadWrite });
    c.query("INSERT INTO genres VALUES (5, 'Musical')")?;
    assert_rows(
        c.query("SELECT * FROM genres WHERE id = 5")?,
        vec![vec![Value::Integer(5), Value::String("Musical".into())]],
    );
    assert_eq!(c.query("ROLLBACK")?, ResultSet::Rollback { id: 4 });
    assert_eq!(c.txn(), None);
    assert_rows(c.query("SELECT * FROM genres WHERE id = 5")?, Vec::new());

    // Starting a read-only txn should block writes
    assert_eq!(c.query("BEGIN READ ONLY")?, ResultSet::Begin { id: 6, mode: Mode::ReadOnly });
    assert_eq!(c.txn(), Some((6, Mode::ReadOnly)));
    assert_rows(
        c.query("SELECT * FROM genres WHERE id = 4")?,
        vec![vec![Value::Integer(4), Value::String("Drama".into())]],
    );
    assert_eq!(c.query("INSERT INTO genres VALUES (5, 'Musical')"), Err(Error::ReadOnly));
    assert_eq!(c.txn(), Some((6, Mode::ReadOnly)));
    assert_rows(
        c.query("SELECT * FROM genres WHERE id = 4")?,
        vec![vec![Value::Integer(4), Value::String("Drama".into())]],
    );
    assert_eq!(c.query("COMMIT")?, ResultSet::Commit { id: 6 });
    assert_eq!(c.txn(), None);

    // Starting a time-travel txn should work, it shouldn't see recent changes, and it should
    // block writes
    assert_eq!(
        c.query("BEGIN READ ONLY AS OF SYSTEM TIME 1")?,
        ResultSet::Begin { id: 7, mode: Mode::Snapshot { version: 1 } }
    );
    assert_eq!(c.txn(), Some((7, Mode::Snapshot { version: 1 })));
    assert_rows(
        c.query("SELECT * FROM genres")?,
        vec![
            vec![Value::Integer(1), Value::String("Science Fiction".into())],
            vec![Value::Integer(2), Value::String("Action".into())],
            vec![Value::Integer(3), Value::String("Comedy".into())],
        ],
    );
    assert_eq!(c.query("INSERT INTO genres VALUES (5, 'Musical')"), Err(Error::ReadOnly));
    assert_eq!(c.query("COMMIT")?, ResultSet::Commit { id: 7 });
    assert_eq!(c.txn(), None);

    // A txn should still be usable after an error occurs
    assert_eq!(c.query("BEGIN")?, ResultSet::Begin { id: 8, mode: Mode::ReadWrite });
    c.query("INSERT INTO genres VALUES (5, 'Horror')")?;
    assert_eq!(
        c.query("INSERT INTO genres VALUES (5, 'Musical')"),
        Err(Error::Value("Primary key 5 already exists for table genres".into()))
    );
    c.query("INSERT INTO genres VALUES (6, 'Western')")?;
    assert_eq!(c.txn(), Some((8, Mode::ReadWrite)));
    assert_eq!(c.query("COMMIT")?, ResultSet::Commit { id: 8 });
    assert_eq!(c.txn(), None);
    assert_rows(
        c.query("SELECT * FROM genres")?,
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

#[test]
#[serial]
fn query_txn_concurrent() -> Result<(), Error> {
    let (mut a, teardown) = setup_movies()?;
    let mut b = toydb::Client::new("127.0.0.1", 9605)?;
    defer!(teardown());

    // Concurrent updates should throw a serialization failure on conflict.
    assert_eq!(a.query("BEGIN")?, ResultSet::Begin { id: 2, mode: Mode::ReadWrite });
    assert_eq!(b.query("BEGIN")?, ResultSet::Begin { id: 3, mode: Mode::ReadWrite });

    assert_rows(
        a.query("SELECT * FROM genres WHERE id = 1")?,
        vec![vec![Value::Integer(1), Value::String("Science Fiction".into())]],
    );
    assert_rows(
        b.query("SELECT * FROM genres WHERE id = 1")?,
        vec![vec![Value::Integer(1), Value::String("Science Fiction".into())]],
    );

    assert_eq!(
        a.query("UPDATE genres SET name = 'x' WHERE id = 1"),
        Ok(ResultSet::Update { count: 1 })
    );
    assert_eq!(b.query("UPDATE genres SET name = 'y' WHERE id = 1"), Err(Error::Serialization));

    assert_eq!(a.query("COMMIT"), Ok(ResultSet::Commit { id: 2 }));
    assert_eq!(b.query("ROLLBACK"), Ok(ResultSet::Rollback { id: 3 }));

    assert_rows(
        a.query("SELECT * FROM genres WHERE id = 1")?,
        vec![vec![Value::Integer(1), Value::String("x".into())]],
    );

    Ok(())
}

#[test]
#[serial]
fn with_txn() -> Result<(), Error> {
    let (mut a, teardown) = setup_movies()?;
    let mut b = toydb::Client::new("127.0.0.1", 9605)?;
    b.txn_retries = 4;
    defer!(teardown());

    // We first let with_txn() time out after retrying.
    let start = std::time::Instant::now();
    a.query("BEGIN")?;
    a.query("UPDATE genres SET name = 'x' WHERE id = 1")?;

    assert_eq!(
        b.with_txn(|t| {
            t.query("UPDATE genres SET name = 'y' WHERE id = 1")?;
            Ok(())
        }),
        Err(Error::Serialization)
    );
    assert_eq!(b.txn(), None);
    assert!(start.elapsed() > std::time::Duration::from_millis(500));
    a.query("ROLLBACK")?;

    // We then start two txns, and commit the first after 200 ms. Both should be committed, and we
    // should see both changes.
    assert_rows(
        a.query("SELECT id, released FROM movies WHERE id = 10")?,
        vec![vec![Value::Integer(10), Value::Integer(2010)]],
    );

    a.query("BEGIN")?;
    a.query("UPDATE movies SET released = released + 1 WHERE id = 10")?;
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(200));
        a.query("COMMIT").unwrap()
    });

    assert_matches!(
        b.with_txn(|t| {
            t.query("UPDATE movies SET released = released + 1 WHERE id = 10")?;
            Ok(())
        }),
        Ok(ResultSet::Commit { .. })
    );
    assert_eq!(b.txn(), None);

    assert_rows(
        b.query("SELECT id, released FROM movies WHERE id = 10")?,
        vec![vec![Value::Integer(10), Value::Integer(2012)]],
    );

    Ok(())
}

fn assert_rows(result: ResultSet, expect: Vec<Row>) {
    match result {
        ResultSet::Query { relation: Relation { rows: Some(rows), .. } } => {
            assert_eq!(rows.collect::<Result<Vec<_>, _>>().unwrap(), expect)
        }
        r => panic!("Unexpected result {:?}", r),
    }
}
