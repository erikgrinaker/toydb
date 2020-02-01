///! Tests for the SQL query engine. Runs SQL queries against an in-memory database,
///! and compares the results with golden files stored under src/sql/tests/results/
use super::super::types::Row;
use super::super::{Context, Engine, Parser, Plan, Transaction};
use crate::Error;
use goldenfile::Mint;
use std::io::Write;

macro_rules! test_query {
    ( $( $name:ident: $query:expr, )* ) => {
        $(
            test_query! { with []; $name: $query, }
        )*
    };
    ( with $setup:expr; $( $name:ident: $query:expr, )* ) => {
    $(
        #[test]
        fn $name() -> Result<(), Error> {
            let mut setup = $setup.to_vec();
            setup.extend(vec![
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
                    (2, 'Action')",
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
                    (1, 'Stalker', 1, 1, 1979, 8.2, FALSE),
                    (2, 'Sicario', 2, 2, 2015, 7.6, TRUE),
                    (3, 'Primer', 3, 1, 2004, 6.9, NULL),
                    (4, 'Heat', 4, 2, 1995, 8.2, TRUE),
                    (5, 'The Fountain', 4, 1, 2006, 7.2, FALSE)",
            ]);
            let engine = super::setup(setup)?;

            let mut mint = Mint::new("src/sql/tests/query");
            let mut f = mint.new_goldenfile(format!("{}", stringify!($name)))?;

            write!(f, "Query: {}\n\n", $query)?;

            write!(f, "AST: ")?;
            let ast = match Parser::new($query).parse() {
                Ok(ast) => ast,
                Err(err) => {
                    write!(f, "{:?}", err)?;
                    return Ok(())
                }
            };
            write!(f, "{:#?}\n\n", ast)?;

            write!(f, "Plan: ")?;
            let plan = match Plan::build(ast) {
                Ok(plan) => plan,
                Err(err) => {
                    write!(f, "{:?}", err)?;
                    return Ok(())
                }
            };
            write!(f, "{:#?}\n\n", plan)?;

            write!(f, "Optimized plan: ")?;
            let plan = match plan.optimize() {
                Ok(plan) => plan,
                Err(err) => {
                    write!(f, "{:?}", err)?;
                    return Ok(())
                }
            };
            write!(f, "{:#?}\n\n", plan)?;

            write!(f, "Query: {}\n\n", $query)?;

            write!(f, "Result:")?;
            let mut txn = engine.begin()?;
            let ctx = Context{txn: &mut txn};
            let result = match plan.execute(ctx) {
                Ok(result) => result,
                Err(err) => {
                    write!(f, " {:?}", err)?;
                    return Ok(())
                }
            };
            txn.commit()?;
            let columns = result.columns();
            let rows: Vec<Row> = match result.collect() {
                Ok(rows) => rows,
                Err(err) => {
                    write!(f, " {:?}", err)?;
                    return Ok(())
                }
            };
            if !columns.is_empty() || !rows.is_empty() {
                write!(f, " {:?}\n", columns)?;
                for row in rows {
                    write!(f, "{:?}\n", row)?;
                }
            } else {
                write!(f, " <none>\n")?;
            }

            Ok(())
        }
    )*
    }
}

test_query! {
    all: "SELECT * FROM movies",
    bare: "SELECT",
    trailing_comma: "SELECT 1,",
    unknown: "SELECT unknown",
    lowercase: "select 1",

    expr_dynamic: "SELECT 2020 - year AS age FROM movies",
    expr_static: "SELECT 1 + 2 * 3, 'abc' LIKE 'x%' AS nope",
    expr_mixed: "SELECT 1 + 2 * 3, 2020 - released AS age FROM movies",

    as_: r#"SELECT 1, 2 b, 3 AS c, 4 AS "👋", id AS "some id" FROM movies"#,
    as_bare: "SELECT 1 AS",
    as_all: "SELECT * AS all FROM movies",

    from_bare: "SELECT * FROM",
    from_unknown: "SELECT * FROM unknown",

    where_bare: "SELECT * FROM movies WHERE",
    where_true: "SELECT * FROM movies WHERE TRUE",
    where_false: "SELECT * FROM movies WHERE FALSE",
    where_null: "SELECT * FROM movies WHERE NULL",
    where_expr: "SELECT * FROM movies WHERE released >= 2000 AND ultrahd",
    where_float: "SELECT * FROM movies WHERE 3.14",
    where_integer: "SELECT * FROM movies WHERE 7",
    where_string: "SELECT * FROM movies WHERE 'abc'",
    where_multi: "SELECT * FROM movies WHERE TRUE, TRUE",
    where_unknown: "SELECT * FROM movies WHERE unknown",

    order: "SELECT * FROM movies ORDER BY released",
    order_asc: "SELECT * FROM movies ORDER BY released ASC",
    order_asc_lowercase: "SELECT * FROM movies ORDER BY released asc",
    order_desc: "SELECT * FROM movies ORDER BY released DESC",
    order_desc_lowercase: "SELECT * FROM movies ORDER BY released desc",
    order_expr: "SELECT id, title, released, released % 4 AS ord FROM movies ORDER BY released % 4 ASC",
    order_multi: "SELECT * FROM movies ORDER BY ultrahd ASC, id DESC",
    order_unknown: "SELECT * FROM movies ORDER BY unknown",
    order_unknown_dir: "SELECT * FROM movies ORDER BY id X",
    order_trailing_comma: "SELECT * FROM movies ORDER BY id,",
}
test_query! { with [
        "CREATE TABLE booleans (id INTEGER PRIMARY KEY, value BOOLEAN)",
        "INSERT INTO booleans VALUES (1, TRUE), (2, NULL), (3, FALSE)",
    ];
    order_boolean_asc: "SELECT * FROM booleans ORDER BY value ASC",
    order_boolean_desc: "SELECT * FROM booleans ORDER BY value DESC",
}
test_query! { with [
        "CREATE TABLE floats (id INTEGER PRIMARY KEY, value FLOAT)",
        "INSERT INTO floats VALUES (1, 3.14), (2, -2.718), (3, NULL), (4, 2.718), (5, 0.0)",
    ];
    order_float_asc: "SELECT * FROM floats ORDER BY value ASC",
    order_float_desc: "SELECT * FROM floats ORDER BY value DESC",
}
test_query! { with [
        "CREATE TABLE integers (id INTEGER PRIMARY KEY, value INTEGER)",
        "INSERT INTO integers VALUES (1, 7), (2, NULL), (3, -3), (4, 3), (5, 0)",
    ];
    order_integer_asc: "SELECT * FROM integers ORDER BY value ASC",
    order_integer_desc: "SELECT * FROM integers ORDER BY value DESC",
}
test_query! { with [
        "CREATE TABLE strings (id INTEGER PRIMARY KEY, value STRING)",
        "INSERT INTO strings VALUES
            (1, 'a'),
            (2, 'ab'),
            (3, 'aaa'),
            (4, 'A'),
            (5, NULL),
            (6, 'aA'),
            (7, 'åa'),
            (8, 'Åa')
        ",
    ];
    order_string_asc: "SELECT * FROM strings ORDER BY value ASC",
    order_string_desc: "SELECT * FROM strings ORDER BY value DESC",
}
test_query! {
    limit: "SELECT * FROM movies LIMIT 3",
    limit_zero: "SELECT * FROM movies LIMIT 0",
    limit_neg: "SELECT * FROM movies LIMIT -1",
    limit_large: "SELECT * FROM movies LIMIT 9223372036854775807",
    limit_expr: "SELECT * FROM movies LIMIT 1 + 2",
    limit_dynamic: "SELECT * FROM movies LIMIT 2000 - released",
    limit_offset: "SELECT * FROM movies LIMIT 2 OFFSET 1",
    limit_multi: "SELECT * FROM movies LIMIT 3, 4",
    limit_null: "SELECT * FROM movies LIMIT NULL",
    limit_boolean: "SELECT * FROM movies LIMIT TRUE",
    limit_float: "SELECT * FROM movies LIMIT 3.14",
    limit_string: "SELECT * FROM movies LIMIT 'abc'",

    offset: "SELECT * FROM movies OFFSET 3",
    offset_zero: "SELECT * FROM movies OFFSET 0",
    offset_neg: "SELECT * FROM movies OFFSET -1",
    offset_large: "SELECT * FROM movies OFFSET 9223372036854775807",
    offset_expr: "SELECT * FROM movies OFFSET 1 + 2",
    offset_dynamic: "SELECT * FROM movies OFFSET 2000 - released",
    offset_multi: "SELECT * FROM movies OFFSET 3, 4",
    offset_null: "SELECT * FROM movies OFFSET NULL",
    offset_boolean: "SELECT * FROM movies OFFSET TRUE",
    offset_float: "SELECT * FROM movies OFFSET 3.14",
    offset_string: "SELECT * FROM movies OFFSET 'abc'",

    join_cross: "SELECT * FROM movies CROSS JOIN genres",
    join_cross_multi: "SELECT * FROM movies CROSS JOIN genres CROSS JOIN countries CROSS JOIN studios",
}
