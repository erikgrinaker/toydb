///! Schema-related tests, using an in-memory database against golden files in src/sql/tests/schema/
use super::super::{Engine, Transaction};
use crate::Error;
use goldenfile::Mint;
use std::io::Write;

macro_rules! test_schema {
    ( $( $name:ident: $queries:expr, )* ) => {
        $(
            #[test]
            fn $name() -> Result<(), Error> {
                let engine = super::setup(vec![])?;
                let mut mint = Mint::new("src/sql/tests/schema");
                let mut f = mint.new_goldenfile(stringify!($name))?;

                for query in &$queries {
                    write!(f, "Query: {}\n", query.trim())?;
                    match super::execute(&engine, query) {
                        // FIXME We need to output something sensible here.
                        Ok(_) => write!(f, "Result: ok\n\n")?,
                        Err(err) => write!(f, "Error: {:?}\n\n", err)?,
                    };
                }

                write!(f, "Storage:")?;
                let txn = engine.begin()?;
                for table in &txn.list_tables()? {
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

test_schema! {
    create_table_bare: ["CREATE TABLE"],
    create_table_datatype: [r#"
        CREATE TABLE name (
            id INTEGER PRIMARY KEY,
            bool_ BOOL,
            boolean_ BOOLEAN,
            char_ CHAR,
            double_ DOUBLE,
            float_ FLOAT,
            int_ INT,
            integer_ INTEGER,
            string_ STRING,
            text_ TEXT,
            varchar_ VARCHAR
        )
    "#],
    create_table_datatype_missing: ["CREATE TABLE name (id)"],
    create_table_datatype_null: ["CREATE TABLE name (id INTEGER PRIMARY KEY, value NULL)"],
    create_table_name_alphanumeric: ["CREATE TABLE a_123 (id INTEGER PRIMARY KEY)"],
    create_table_name_case: ["CREATE TABLE mIxEd_cAsE (ÄÅÆ STRING PRIMARY KEY)"],
    create_table_name_emoji: ["CREATE TABLE 👋 (id INTEGER PRIMARY KEY)"],
    create_table_name_japanese: ["CREATE TABLE 表 (身元 INTEGER PRIMARY KEY, 名前 STRING)"],
    create_table_name_missing: ["CREATE TABLE (id INTEGER PRIMARY KEY)"],
    create_table_name_quote_single: [r#"CREATE TABLE 'name' (id INTEGER PRIMARY KEY)"#],
    create_table_name_quote_double: [r#"CREATE TABLE "name" (id INTEGER PRIMARY KEY)"#],
    create_table_name_underscore_prefix: ["CREATE TABLE _name (id INTEGER PRIMARY KEY)"],
    create_table_columns_empty: ["CREATE TABLE name ()"],
    create_table_columns_missing: ["CREATE TABLE name"],
    create_table_pk_missing: ["CREATE TABLE name (id INTEGER)"],
    create_table_pk_multiple: ["CREATE TABLE name (id INTEGER PRIMARY KEY, name STRING PRIMARY KEY)"],
    create_table_pk_nullable: ["CREATE TABLE name (id INTEGER PRIMARY KEY NULL)"],
    create_table_exists: [
        "CREATE TABLE name (id INTEGER PRIMARY KEY)",
        "CREATE TABLE name (id INTEGER PRIMARY KEY)",
    ],

    drop_table: [
        "CREATE TABLE a (id INTEGER PRIMARY KEY)",
        "INSERT INTO a VALUES (11), (12), (13)",
        "CREATE TABLE b (id INTEGER PRIMARY KEY)",
        "INSERT INTO b VALUES (21), (22), (23)",
        "DROP TABLE a",
    ],
    drop_table_bare: ["DROP TABLE"],
    drop_table_missing: ["DROP TABLE name"],
    drop_table_multiple: [
        "CREATE TABLE a (id INTEGER PRIMARY KEY)",
        "CREATE TABLE b (id INTEGER PRIMARY KEY)",
        "CREATE TABLE c (id INTEGER PRIMARY KEY)",
        "DROP TABLE a, c",
    ],
}
