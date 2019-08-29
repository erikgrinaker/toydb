use super::lexer::{Lexer, Token};
use super::{Parser, Planner, Storage};
use crate::kv;
use crate::Error;
use goldenfile::Mint;
use std::io::Write;

macro_rules! test_sql {
    ( $( $name:ident: $sql:expr, )* ) => {
    $(
        #[test]
        fn $name() {
            let storage = Storage::new(kv::Memory::new());
            let mut mint = Mint::new(format!("src/sql/testdata/{}", stringify!($name)));
            write!(mint.new_goldenfile("query.sql").unwrap(), "{}", $sql).unwrap();

            let mut f = mint.new_goldenfile("tokens").unwrap();
            match Lexer::new($sql).collect::<Result<Vec<Token>, Error>>() {
                Ok(tokens) => write!(f, "{:#?}", tokens).unwrap(),
                err => write!(f, "{:?}", err).unwrap(),
            };

            let mut f_ast = mint.new_goldenfile("ast").unwrap();
            let mut f_plan = mint.new_goldenfile("plan").unwrap();
            match Parser::new($sql).parse() {
                Ok(ast) => {
                    write!(f_ast, "{:#?}", ast).unwrap();
                    match Planner::new(Box::new(storage)).build(ast) {
                        Ok(plan) => write!(f_plan, "{:#?}", plan).unwrap(),
                        err => write!(f_plan, "{:?}", err).unwrap(),
                    };
                },
                err => {
                    write!(f_ast, "{:?}", err).unwrap();
                    write!(f_plan, "{:?}", err).unwrap();
                },
            };
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
    create_table_error_no_columns: "CREATE TABLE name",
    create_table_error_no_datatype: "CREATE TABLE name (id)",
    create_table_error_no_name: "CREATE TABLE (id INTEGER)",
    create_table_error_no_primary_key: "CREATE TABLE name (id INTEGER)",
    create_table_error_multiple_primary_key: "CREATE TABLE name (id INTEGER PRIMARY KEY, name VARCHAR PRIMARY KEY)",
    drop_table: "DROP TABLE name",
    drop_table_error_bare: "DROP TABLE",
    select_alone_aliases: "SELECT 1, 2 b, 3 AS c",
    select_alone_datatypes: "SELECT NULL, TRUE, FALSE, 1, 3.14, 'Hi! 👋'",
    select_alone_literal_numbers: "SELECT 0, 1, -2, --3, +-4, 3.14, 293, 3.14e3, 2.718E-2",
    select_alone_literal_string_quotes: r#"SELECT 'Literal with ''single'' and "double" quotes'"#,
    select_error_bare: "SELECT",
    select_error_bare_as: "SELECT 1 AS, 2",
    select_error_trailing_comma: "SELECT 1, 2,",
}
