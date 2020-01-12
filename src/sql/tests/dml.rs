///! DML-related tests, using an in-memory database against golden files in src/sql/tests/dml/
///! Note that schema-related tests are in schema.rs, this is just for the basic DML functionality
use super::super::{Engine, Transaction};
use crate::Error;
use goldenfile::Mint;
use std::io::Write;

macro_rules! test_dml {
    ( $( $name:ident: $query:expr, )* ) => {
        $(
            test_schema! { with []; $name: $query, }
        )*
    };
    ( with $setup:expr; $( $name:ident: $query:expr, )* ) => {
        $(
            #[test]
            fn $name() -> Result<(), Error> {
                let setup: &[&str] = &$setup;
                let engine = super::setup(setup.into())?;
                let mut mint = Mint::new("src/sql/tests/dml");
                let mut f = mint.new_goldenfile(stringify!($name))?;

                write!(f, "Query: {}\n", $query.trim())?;
                match engine.session(None)?.execute($query) {
                    Ok(resultset) => {
                        write!(f, "Effect:")?;
                        if let Some(effect) = resultset.effect() {
                            write!(f, " {:?}", effect)?;
                        }
                        write!(f, "\n\n")?;
                    },
                    Err(err) => write!(f, "Error: {:?}\n\n", err)?,
                };

                write!(f, "Storage:")?;
                let txn = engine.begin()?;
                for table in txn.scan_tables()? {
                    write!(f, "\n{}\n", table.as_sql())?;
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

test_dml! { with [
        "CREATE TABLE test (
            id INTEGER PRIMARY KEY DEFAULT 0,
            name STRING,
            value INTEGER
        )",
        "CREATE TABLE other (id INTEGER PRIMARY KEY)"
    ];

    insert_full: "INSERT INTO test (id, name, value) VALUES (1, 'a', 101)",
    insert_full_multiple: "INSERT INTO test (id, name, value) VALUES (1, 'a', 101), (2, 'b', 102), (3, 'c', 103)",
    insert_full_order: "INSERT INTO test (name, value, id) VALUES ('a', 101, 1)",
    insert_full_trailing_comma: "INSERT INTO test (id, name, value) VALUES (1, 'a', 101), (2, 'b', 102),",
    insert_expression: "INSERT INTO test (id, name, value) VALUES (1, 'a', 1 + 2 * 3)",
    insert_no_columns: "INSERT INTO test VALUES (1, 'a', 101)",
    insert_no_columns_multiple: "INSERT INTO test VALUES (1, 'a', 101), (2, 'b', 102), (3, 'c', 103)",
    insert_partial: "INSERT INTO test VALUES (1, 'a')",
    insert_partial_columns: "INSERT INTO test (id, name, value) VALUES (1, 'a')",
    insert_partial_vary: "INSERT INTO test VALUES (1, 'a', 100), (2, 'b'), (3)",
    insert_extra: "INSERT INTO test VALUES (1, 'a', 100, NULL)",
    insert_extra_columns: "INSERT INTO test (id, name) VALUES (1, 'a', 100)",
    insert_empty_columns: "INSERT INTO test ()",
    insert_empty_values: "INSERT INTO test VALUES ()",
    insert_empty_both: "INSERT INTO test () VALUES ()",
    insert_missing_column: "INSERT INTO test (id, missing) VALUES (0, 'x')",
    insert_missing_table: "INSERT INTO missing (id) VALUES (0)",
    insert_multiple_tables: "INSERT INTO test, other VALUES (1)",
    insert_case: "INSERT INTO TeSt (ID, Name) VALUES (1, 'a')",
    insert_bare: "INSERT INTO test",
    insert_bare_no_table: "INSERT INTO",
    insert_bare_values: "INSERT INTO test VALUES",
}
