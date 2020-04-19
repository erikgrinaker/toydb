use super::setup;
use super::util::{assert_row, assert_rows};

use toydb::server::Status;
use toydb::sql::engine::Mode;
use toydb::sql::execution::ResultSet;
use toydb::sql::schema;
use toydb::sql::types::{Column, DataType, Relation, Value};
use toydb::Client;
use toydb::Error;

use pretty_assertions::assert_eq;
use serial_test::serial;

#[allow(clippy::type_complexity)]
async fn setup_movies() -> Result<(Client, Box<dyn FnOnce()>), Error> {
    setup::server_with_queries(vec![
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
    .await
}

#[tokio::test]
#[serial]
async fn get_table() -> Result<(), Error> {
    let (c, teardown) = setup_movies().await?;
    defer!(teardown());

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

#[tokio::test]
#[serial]
async fn list_tables() -> Result<(), Error> {
    let (c, teardown) = setup_movies().await?;
    defer!(teardown());

    assert_eq!(c.list_tables().await?, vec!["countries", "genres", "movies", "studios"]);
    Ok(())
}

#[tokio::test]
#[serial]
async fn status() -> Result<(), Error> {
    let (c, teardown) = setup_movies().await?;
    defer!(teardown());

    assert_eq!(
        c.status().await?,
        Status { id: "test".into(), version: env!("CARGO_PKG_VERSION").into() }
    );
    Ok(())
}

#[tokio::test]
#[serial]
async fn query() -> Result<(), Error> {
    // The SQL engine is thoroughly tested in a separate suite, this just exercises the
    // basic client/server integration.
    let (c, teardown) = setup_movies().await?;
    defer!(teardown());

    // SELECT
    let result = c.execute("SELECT * FROM genres").await?;
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

    let result = c.execute("SELECT * FROM genres WHERE FALSE").await?;
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

#[tokio::test]
#[serial]
async fn query_txn() -> Result<(), Error> {
    let (c, teardown) = setup_movies().await?;
    defer!(teardown());

    // Committing a change in a txn should work
    assert_eq!(c.execute("BEGIN").await?, ResultSet::Begin { id: 2, mode: Mode::ReadWrite });
    c.execute("INSERT INTO genres VALUES (4, 'Drama')").await?;
    assert_eq!(c.execute("COMMIT").await?, ResultSet::Commit { id: 2 });
    assert_row(
        c.execute("SELECT * FROM genres WHERE id = 4").await?,
        vec![Value::Integer(4), Value::String("Drama".into())],
    );

    // Rolling back a change in a txn should also work
    assert_eq!(c.execute("BEGIN").await?, ResultSet::Begin { id: 4, mode: Mode::ReadWrite });
    c.execute("INSERT INTO genres VALUES (5, 'Musical')").await?;
    assert_row(
        c.execute("SELECT * FROM genres WHERE id = 5").await?,
        vec![Value::Integer(5), Value::String("Musical".into())],
    );
    assert_eq!(c.execute("ROLLBACK").await?, ResultSet::Rollback { id: 4 });
    assert_rows(c.execute("SELECT * FROM genres WHERE id = 5").await?, Vec::new());

    // Starting a read-only txn should block writes
    assert_eq!(
        c.execute("BEGIN READ ONLY").await?,
        ResultSet::Begin { id: 6, mode: Mode::ReadOnly }
    );
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

#[tokio::test]
#[serial]
async fn query_txn_concurrent() -> Result<(), Error> {
    let (a, teardown) = setup_movies().await?;
    let b = Client::new("127.0.0.1:9605").await?;
    defer!(teardown());

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
