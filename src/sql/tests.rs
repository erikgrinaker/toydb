/**
 * Tests for the SQL engine. Runs SQL queries against an in-memory database,
 * and compares the results with golden files stored under src/sql/testdata/
 */
use super::lexer::{Lexer, Token};
use super::schema;
use super::types::{DataType, Row, Value};
use super::{Context, Parser, Plan, Storage};
use crate::kv;
use crate::Error;
use goldenfile::Mint;
use std::io::Write;

macro_rules! test_sql {
    ( $( $name:ident: $sql:expr, )* ) => {
    $(
        #[test]
        fn $name() {
            let mut storage = Storage::new(kv::Memory::new());
            storage.create_table(&schema::Table{
                name: "genres".into(),
                columns: vec![
                    schema::Column{
                        name: "id".into(),
                        datatype: DataType::Integer,
                        nullable: false,
                    },
                    schema::Column{
                        name: "name".into(),
                        datatype: DataType::String,
                        nullable: false,
                    },
                ],
                primary_key: 0,
            }).unwrap();
            storage.create_table(&schema::Table{
                name: "movies".into(),
                columns: vec![
                    schema::Column{
                        name: "id".into(),
                        datatype: DataType::Integer,
                        nullable: false,
                    },
                    schema::Column{
                        name: "title".into(),
                        datatype: DataType::String,
                        nullable: false,
                    },
                    schema::Column{
                        name: "genre_id".into(),
                        datatype: DataType::Integer,
                        nullable: false,
                    },
                    schema::Column{
                        name: "released".into(),
                        datatype: DataType::Integer,
                        nullable: false,
                    },
                    schema::Column{
                        name: "rating".into(),
                        datatype: DataType::Float,
                        nullable: true,
                    },
                    schema::Column{
                        name: "bluray".into(),
                        datatype: DataType::Boolean,
                        nullable: true,
                    },
                ],
                primary_key: 0,
            }).unwrap();
            storage.create_row("genres", vec![
                Value::Integer(1),
                Value::String("Science Fiction".into()),
            ]).unwrap();
            storage.create_row("genres", vec![
                Value::Integer(2),
                Value::String("Action".into()),
            ]).unwrap();
            storage.create_row("movies", vec![
                Value::Integer(1),
                Value::String("Stalker".into()),
                Value::Integer(1),
                Value::Integer(1979),
                Value::Float(8.2),
                Value::Boolean(false),
            ]).unwrap();
            storage.create_row("movies", vec![
                Value::Integer(2),
                Value::String("Sicario".into()),
                Value::Integer(2),
                Value::Integer(2015),
                Value::Float(7.6),
                Value::Boolean(true),
            ]).unwrap();
            storage.create_row("movies", vec![
                Value::Integer(3),
                Value::String("Primer".into()),
                Value::Integer(1),
                Value::Integer(2004),
                Value::Float(6.9),
                Value::Null,
            ]).unwrap();
            storage.create_row("movies", vec![
                Value::Integer(4),
                Value::String("Heat".into()),
                Value::Integer(2),
                Value::Integer(1995),
                Value::Float(8.2),
                Value::Boolean(true),
            ]).unwrap();
            storage.create_row("movies", vec![
                Value::Integer(5),
                Value::String("The Fountain".into()),
                Value::Integer(1),
                Value::Integer(2006),
                Value::Float(7.2),
                Value::Boolean(true),
            ]).unwrap();

            let mut mint = Mint::new("src/sql/testdata");
            let mut f = mint.new_goldenfile(format!("{}", stringify!($name))).unwrap();

            write!(f, "Query: {}\n\n", $sql).unwrap();

            write!(f, "Tokens:\n").unwrap();
            let tokens = match Lexer::new($sql).collect::<Result<Vec<Token>, Error>>() {
                Ok(tokens) => tokens,
                err => {
                    write!(f, "{:?}", err).unwrap();
                    return
                }
            };
            for token in tokens {
                write!(f, "  {:?}\n", token).unwrap();
            }
            write!(f, "\n").unwrap();

            write!(f, "AST: ").unwrap();
            let ast = match Parser::new($sql).parse() {
                Ok(ast) => ast,
                Err(err) => {
                    write!(f, "{:?}", err).unwrap();
                    return
                }
            };
            write!(f, "{:#?}\n\n", ast).unwrap();

            write!(f, "Plan: ").unwrap();
            let plan = match Plan::build(ast) {
                Ok(plan) => plan,
                Err(err) => {
                    write!(f, "{:?}", err).unwrap();
                    return
                }
            };
            write!(f, "{:#?}\n\n", plan).unwrap();

            write!(f, "Query: {}\n\n", $sql).unwrap();

            write!(f, "Result:").unwrap();
            let result = match plan.execute(Context{storage: Box::new(storage.clone())}) {
                Ok(result) => result,
                Err(err) => {
                    write!(f, " {:?}", err).unwrap();
                    return
                }
            };
            let columns = result.columns();
            let rows: Vec<Row> = match result.collect() {
                Ok(rows) => rows,
                Err(err) => {
                    write!(f, " {:?}", err).unwrap();
                    return
                }
            };
            if !columns.is_empty() || !rows.is_empty() {
                write!(f, " [{:?}]\n", columns).unwrap();
                for row in rows {
                    write!(f, "{:?}\n", row).unwrap();
                }
            } else {
                write!(f, " <none>\n").unwrap();
            }

            write!(f, "\nStorage:").unwrap();
            for table in &storage.list_tables().unwrap() {
                let schema = &storage.get_table(&table).unwrap().unwrap();
                write!(f, "\n{}\n", schema.to_query()).unwrap();
                for row in storage.scan_rows(&table) {
                    write!(f, "{:?}\n", row.unwrap()).unwrap();
                }
            }
        }
    )*
    }
}

test_sql! {
    create_table: r#"
        CREATE TABLE name (
            id INTEGER PRIMARY KEY,
            string VARCHAR NOT NULL,
            text VARCHAR,
            number INTEGER,
            decimal FLOAT,
            bool BOOLEAN NULL
        )"#,
    create_table_single_column: "CREATE TABLE name (id INTEGER PRIMARY KEY)",
    create_table_error_bare: "CREATE TABLE",
    create_table_error_empty_columns: "CREATE TABLE name ()",
    create_table_error_exists: "CREATE TABLE movies (id INTEGER PRIMARY KEY)",
    create_table_error_no_columns: "CREATE TABLE name",
    create_table_error_no_datatype: "CREATE TABLE name (id)",
    create_table_error_no_name: "CREATE TABLE (id INTEGER)",
    create_table_error_pk_missing: "CREATE TABLE name (id INTEGER)",
    create_table_error_pk_multiple: "CREATE TABLE name (id INTEGER PRIMARY KEY, name VARCHAR PRIMARY KEY)",
    create_table_error_pk_nullable: "CREATE TABLE name (id INTEGER PRIMARY KEY NULL)",

    delete_all: "DELETE FROM movies",
    delete_error_bare: "DELETE FROM",
    delete_error_multiple: "DELETE FROM movies, genres",
    delete_error_table: "DELETE FROM missing",
    delete_where: "DELETE FROM movies WHERE released >= 2000",
    delete_where_false: "DELETE FROM movies WHERE FALSE",
    delete_where_null: "DELETE FROM movies WHERE NULL",
    delete_where_true: "DELETE FROM movies WHERE TRUE",

    drop_table: "DROP TABLE movies",
    drop_table_error_bare: "DROP TABLE",
    drop_table_error_missing: "DROP TABLE missing",

    expr_datatypes: "SELECT NULL, TRUE, FALSE, 1, 3.14, 'Hi! 👋'",
    expr_literal_numbers: "SELECT 0, 1, -2, --3, +-4, 3.14, 293, 3.14e3, 2.718E-2",
    expr_literal_string_quotes: r#"SELECT 'Literal with ''single'' and "double" quotes'"#,

    insert_default_null: "INSERT INTO movies VALUES (9, 'District 9', 1, 2009)",
    insert_expression: "INSERT INTO movies VALUES (2 * 5 - 1, 'District 9', 1000 ^ 0, 2 * 1000 + 1 * 10 - --1, 793 / 1e2, TRUE OR FALSE)",
    insert_error_columns_duplicate: "INSERT INTO genres (id, name, id) VALUES (9, 'Western', 9)",
    insert_error_columns_mismatch: "INSERT INTO genres (id) VALUES (9, 'Western')",
    insert_error_datatype_conflict: "INSERT INTO genres VALUES (9, 3.14)",
    insert_error_default_null_disallowed: "INSERT INTO movies VALUES (9)",
    insert_error_null_disallowed: "INSERT INTO genres VALUES (9, NULL)",
    insert_error_pk_exists: "INSERT INTO genres VALUES (1, 'Western')",
    insert_error_pk_null: "INSERT INTO genres VALUES (NULL, 'Western')",
    insert_partial: "INSERT INTO movies (title, released, id, genre_id, rating) VALUES ('District 9', 2009, 9, 1, 7.9)",
    insert_values: "INSERT INTO genres VALUES (9, 'Western')",

    select_all_from_table: "SELECT * FROM movies",
    select_aliases: "SELECT 1, 2 b, 3 AS c",
    select_error_bare: "SELECT",
    select_error_bare_as: "SELECT 1 AS, 2",
    select_error_bare_from: "SELECT 1 FROM",
    select_error_bare_where: "SELECT 1 FROM movies WHERE",
    select_error_trailing_comma: "SELECT 1, 2,",
    select_error_where_nonboolean: "SELECT * FROM movies WHERE 1",
    select_expr_where_false: "SELECT 1 WHERE FALSE",
    select_expr_where_true: "SELECT 1 WHERE TRUE",
    select_fields: "SELECT title, 2019 - released AS age, rating * 10 FROM movies",
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
