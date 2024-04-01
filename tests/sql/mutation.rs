//! Mutation tests, using an in-memory database against golden files in tests/sql/mutation/
use toydb::error::Result;
use toydb::sql::engine::{Engine as _, Transaction as _};
use toydb::sql::schema::Catalog as _;

use goldenfile::Mint;
use std::io::Write;

macro_rules! test_mutation {
    ( $( $name:ident: $query:expr, )* ) => {
        $(
            test_schema! { with []; $name: $query, }
        )*
    };
    ( with $setup:expr; $( $name:ident: $query:expr, )* ) => {
        $(
            #[test]
            fn $name() -> Result<()> {
                let setup: &[&str] = &$setup;
                let engine = super::setup(setup.into())?;
                let mut mint = Mint::new("tests/sql/mutation");
                let mut f = mint.new_goldenfile(stringify!($name))?;

                write!(f, "Query: {}\n", $query.trim())?;
                match engine.session().execute($query) {
                    Ok(resultset) => {
                        write!(f, "Result: {:?}\n\n", resultset)?;
                    },
                    Err(err) => write!(f, "Error: {:?}\n\n", err)?,
                };

                write!(f, "Storage:")?;
                let txn = engine.begin()?;
                for table in txn.scan_tables()? {
                    write!(f, "\n{}\n", table)?;
                    for row in txn.scan(&table.name, None)? {
                        write!(f, "{:?}\n", row?)?;
                    }

                    for column in table.columns.iter().filter(|c| c.index) {
                        write!(f, "\nIndex {}.{}\n", table.name, column.name)?;
                        let mut scan = txn.scan_index(&table.name, &column.name)?;
                        while let Some((value, pks)) = scan.next().transpose()? {
                            let mut pks = pks.into_iter().collect::<Vec<_>>();
                            pks.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                            write!(f, "{:?} => {:?}\n", value, pks)?;
                        }
                    }
                }
                txn.rollback()?;

                Ok(())
            }
        )*
    }
}

test_mutation! { with [
        "CREATE TABLE test (
            id INTEGER PRIMARY KEY DEFAULT 0,
            name STRING INDEX,
            value INTEGER
        )",
        "INSERT INTO test VALUES (1, 'a', 101), (2, 'b', 102), (3, 'c', 103)",
        "CREATE TABLE other (id INTEGER PRIMARY KEY)",
        "INSERT INTO other VALUES (1), (2), (3)",
    ];

    delete_all: "DELETE FROM test",
    delete_where: "DELETE FROM test WHERE id = 1",
    delete_where_and: "DELETE FROM test WHERE id = 1 AND name = 'a'",
    delete_where_expr: "DELETE FROM test WHERE id = 3 - 2 AND name LIKE 'a%'",
    delete_where_true: "DELETE FROM test WHERE TRUE",
    delete_where_false: "DELETE FROM test WHERE FALSE",
    delete_where_null: "DELETE FROM test WHERE NULL",
    delete_where_float: "DELETE FROM test WHERE 3.14",
    delete_where_integer: "DELETE FROM test WHERE 1",
    delete_where_string: "DELETE FROM test WHERE 'a'",
    delete_case: "DELETE FROM TeSt WHERE ID = 1",
    delete_missing_column_where: "DELETE FROM test WHERE missing = TRUE",
    delete_missing_table: "DELETE FROM missing",
    delete_multiple_tables: "DELETE FROM test, other WHERE id = 1",
    delete_bare: "DELETE",
    delete_bare_from: "DELETE FROM",
    delete_bare_where: "DELETE FROM test WHERE",
}

test_mutation! { with [
        "CREATE TABLE test (
            id INTEGER PRIMARY KEY DEFAULT 0,
            name STRING INDEX,
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

test_mutation! { with [
        "CREATE TABLE test (
            id INTEGER PRIMARY KEY DEFAULT 0,
            name STRING INDEX,
            value INTEGER
        )",
        "INSERT INTO test VALUES (1, 'a', 100), (2, 'b', 102), (3, 'c', 103)",
        "CREATE TABLE other (id INTEGER PRIMARY KEY)",
        "INSERT INTO other VALUES (1), (2), (3)",
    ];

    update_all: "UPDATE test SET name = 'x', value = 999",
    update_where: "UPDATE test SET name = 'x' WHERE id = 1",
    update_where_and: "UPDATE test SET name = 'x' WHERE id = 1 AND name = 'a'",
    update_where_expr: "UPDATE test SET name = 'x' WHERE id = 3 - 2 AND name LIKE 'a%'",
    update_where_true: "UPDATE test SET name = 'x' WHERE TRUE",
    update_where_false: "UPDATE test SET name = 'x' WHERE FALSE",
    update_where_null: "UPDATE test SET name = 'x' WHERE NULL",
    update_where_float: "UPDATE test SET name = 'x' WHERE 3.14",
    update_where_integer: "UPDATE test SET name = 'x' WHERE 1",
    update_where_string: "UPDATE test SET name = 'x' WHERE 'a'",
    update_where_full: "UPDATE test SET id = 9, name = 'x', value = 999 WHERE id = 1",
    update_case: "UPDATE TeSt SET Name = 'x' WHERE ID = 1",
    update_missing_column_set: "UPDATE test SET missing = 0",
    update_missing_column_where: "UPDATE test SET name = 'x' WHERE missing = TRUE",
    update_missing_table: "UPDATE missing SET id = 0",
    update_multiple_tables: "UPDATE test, other SET id = 9 WHERE id = 1",
    update_bare: "UPDATE test",
    update_bare_set: "UPDATE test SET",
    update_bare_where: "UPDATE test SET name = 'x' WHERE",
    update_bare_no_table: "UPDATE",
}
