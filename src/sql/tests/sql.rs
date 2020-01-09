///! Tests for the SQL engine. Runs SQL queries against an in-memory database,
///! and compares the results with golden files stored under src/sql/tests/results/
use super::super::lexer::{Lexer, Token};
use super::super::types::Row;
use super::super::{Context, Engine, Parser, Plan, Transaction};
use crate::Error;
use goldenfile::Mint;
use std::io::Write;

macro_rules! test_sql {
    ( $( $name:ident: $sql:expr, )* ) => {
    $(
        #[test]
        fn $name() -> Result<(), Error> {
            let engine = super::setup(vec![
                "CREATE TABLE genres (
                    id INTEGER PRIMARY KEY,
                    name STRING NOT NULL
                )",
                "CREATE TABLE movies (
                    id INTEGER PRIMARY KEY,
                    title STRING NOT NULL,
                    genre_id INTEGER NOT NULL REFERENCES genres,
                    released INTEGER NOT NULL,
                    rating FLOAT,
                    bluray BOOLEAN
                )",
                "INSERT INTO genres VALUES
                    (1, 'Science Fiction'),
                    (2, 'Action')",
                "INSERT INTO movies VALUES
                    (1, 'Stalker', 1, 1979, 8.2, FALSE),
                    (2, 'Sicario', 2, 2015, 7.6, TRUE),
                    (3, 'Primer', 1, 2004, 6.9, NULL),
                    (4, 'Heat', 2, 1995, 8.2, TRUE),
                    (5, 'The Fountain', 1, 2006, 7.2, TRUE)",
            ])?;

            let mut mint = Mint::new("src/sql/tests/results");
            let mut f = mint.new_goldenfile(format!("{}", stringify!($name)))?;

            write!(f, "Query: {}\n\n", $sql)?;

            write!(f, "Tokens:\n")?;
            let tokens = match Lexer::new($sql).collect::<Result<Vec<Token>, Error>>() {
                Ok(tokens) => tokens,
                err => {
                    write!(f, "{:?}", err)?;
                    return Ok(())
                }
            };
            for token in tokens {
                write!(f, "  {:?}\n", token)?;
            }
            write!(f, "\n")?;

            write!(f, "AST: ")?;
            let ast = match Parser::new($sql).parse() {
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

            write!(f, "Query: {}\n\n", $sql)?;

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
                write!(f, " [{:?}]\n", columns)?;
                for row in rows {
                    write!(f, "{:?}\n", row)?;
                }
            } else {
                write!(f, " <none>\n")?;
            }

            write!(f, "\nStorage:")?;
            let txn = engine.begin()?;
            for table in &txn.list_tables()? {
                let schema = &txn.read_table(&table.name)?.unwrap();
                write!(f, "\n{}\n", schema.as_sql())?;
                for row in txn.scan(&table.name)? {
                    write!(f, "{:?}\n", row?)?;
                }
            }
            txn.rollback()?;

            Ok(())
        }
    )*
    }
}

test_sql! {
    delete_all: "DELETE FROM movies",
    delete_error_bare: "DELETE FROM",
    delete_error_multiple: "DELETE FROM movies, genres",
    delete_error_table: "DELETE FROM missing",
    delete_where: "DELETE FROM movies WHERE released >= 2000",
    delete_where_false: "DELETE FROM movies WHERE FALSE",
    delete_where_null: "DELETE FROM movies WHERE NULL",
    delete_where_true: "DELETE FROM movies WHERE TRUE",

    insert_default_null: "INSERT INTO movies VALUES (9, 'District 9', 1, 2009)",
    insert_expression: "INSERT INTO movies VALUES (2 * 5 - 1, 'District 9', 1 / 1, 2 * 1000 + 1 * 10 - --1, 793 / 1e2, TRUE OR FALSE)",
    insert_partial: "INSERT INTO movies (title, released, id, genre_id, rating) VALUES ('District 9', 2009, 9, 1, 7.9)",
    insert_values: "INSERT INTO genres VALUES (9, 'Western')",
    insert_error_columns_duplicate: "INSERT INTO genres (id, name, id) VALUES (9, 'Western', 9)",
    insert_error_columns_mismatch: "INSERT INTO genres (id) VALUES (9, 'Western')",
    insert_error_default_null_disallowed: "INSERT INTO movies VALUES (9)",
    insert_error_null_disallowed: "INSERT INTO genres VALUES (9, NULL)",

    select_all_from_table: "SELECT * FROM movies",
    select_aliases: "SELECT 1, 2 b, 3 AS c",
    select_case: "SELECT TiTlE AS Name FROM movies",
    select_error_bare: "SELECT",
    select_error_bare_as: "SELECT 1 AS, 2",
    select_error_bare_from: "SELECT 1 FROM",
    select_error_bare_where: "SELECT 1 FROM movies WHERE",
    select_error_trailing_comma: "SELECT 1, 2,",
    select_error_where_nonboolean: "SELECT * FROM movies WHERE 1",
    select_expr_where_false: "SELECT 1 WHERE FALSE",
    select_expr_where_true: "SELECT 1 WHERE TRUE",
    select_fields: "SELECT title, 2019 - released AS age, rating * 10 FROM movies",
    select_limit: "SELECT * FROM movies ORDER BY id LIMIT 2",
    select_limit_offset: "SELECT * FROM movies ORDER BY id LIMIT 2 OFFSET 2",
    select_offset: "SELECT * FROM movies ORDER BY id OFFSET 2",
    select_order: "SELECT * FROM movies ORDER BY bluray ASC, released DESC",
    select_order_projection: "SELECT 5 - id AS a, title FROM movies ORDER BY a",
    select_where: "SELECT * FROM movies WHERE released >= 2000 AND rating > 7",
    select_where_false: "SELECT * FROM movies WHERE FALSE",
    select_where_null: "SELECT * FROM movies WHERE NULL",
    select_where_true: "SELECT * FROM movies WHERE TRUE",

    update_all: "UPDATE movies SET title = 'X', released = 0, bluray = NULL",
    update_error_multiple_tables: "UPDATE movies, genres SET title = 'X'",
    update_error_no_set: "UPDATE movies",
    update_error_no_set_where: "UPDATE movies WHERE TRUE",
    update_false: "UPDATE movies SET title = 'X' WHERE FALSE",
    update_where: "UPDATE movies SET rating = NULL, bluray = NULL WHERE released < 2000",
}
