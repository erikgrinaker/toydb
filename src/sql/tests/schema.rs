///! Schema-related tests, using an in-memory database against golden files in src/sql/tests/schema/
use super::super::{Engine, Transaction};
use crate::Error;
use goldenfile::Mint;
use std::io::Write;

macro_rules! test_schema {
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
                let mut mint = Mint::new("src/sql/tests/schema");
                let mut f = mint.new_goldenfile(stringify!($name))?;

                write!(f, "Query: {}\n", $query.trim())?;
                match super::execute(&engine, $query) {
                    // FIXME We need to output something sensible here.
                    Ok(_) => write!(f, "\n")?,
                    Err(err) => write!(f, "Error: {:?}\n\n", err)?,
                };

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
    create_table_bare: "CREATE TABLE",
    create_table_datatype: r#"
        CREATE TABLE name (
            id INTEGER PRIMARY KEY,
            "bool" BOOL,
            "boolean" BOOLEAN,
            "char" CHAR,
            "double" DOUBLE,
            "float" FLOAT,
            "int" INT,
            "integer" INTEGER,
            "string" STRING,
            "text" TEXT,
            "varchar" VARCHAR
        )
    "#,
    create_table_datatype_missing: "CREATE TABLE name (id)",
    create_table_datatype_null: "CREATE TABLE name (id INTEGER PRIMARY KEY, value NULL)",
    create_table_name_alphanumeric: "CREATE TABLE a_123 (id INTEGER PRIMARY KEY)",
    create_table_name_case: "CREATE TABLE mIxEd_cAsE (ÄÅÆ STRING PRIMARY KEY)",
    create_table_name_emoji: "CREATE TABLE 👋 (🆔 INTEGER PRIMARY KEY)",
    create_table_name_emoji_quoted: r#"CREATE TABLE "👋" ("🆔" INTEGER PRIMARY KEY)"#,
    create_table_name_japanese: "CREATE TABLE 表 (身元 INTEGER PRIMARY KEY, 名前 STRING)",
    create_table_name_keyword: "CREATE TABLE table (id INTEGER PRIMARY KEY)",
    create_table_name_keyword_quoted: r#"CREATE TABLE "table" (id INTEGER PRIMARY KEY)"#,
    create_table_name_missing: "CREATE TABLE (id INTEGER PRIMARY KEY)",
    create_table_name_quote_single: r#"CREATE TABLE 'name' (id INTEGER PRIMARY KEY)"#,
    create_table_name_quote_double: r#"CREATE TABLE "name" (id INTEGER PRIMARY KEY)"#,
    create_table_name_quote_double_unterminated: r#"CREATE TABLE "name (id INTEGER PRIMARY KEY)"#,
    create_table_name_quote_double_escaped: r#"CREATE TABLE "name with "" quote" (id INTEGER PRIMARY KEY)"#,
    create_table_name_quote_double_single: r#"CREATE TABLE "name with ' quote" (id INTEGER PRIMARY KEY)"#,
    create_table_name_underscore_prefix: "CREATE TABLE _name (id INTEGER PRIMARY KEY)",
    create_table_columns_empty: "CREATE TABLE name ()",
    create_table_columns_missing: "CREATE TABLE name",
    create_table_pk_missing: "CREATE TABLE name (id INTEGER)",
    create_table_pk_multiple: "CREATE TABLE name (id INTEGER PRIMARY KEY, name STRING PRIMARY KEY)",
    create_table_pk_nullable: "CREATE TABLE name (id INTEGER PRIMARY KEY NULL)",
}
test_schema! { with ["CREATE TABLE name (id INTEGER PRIMARY KEY)"];
    create_table_exists: "CREATE TABLE name (id INTEGER PRIMARY KEY)",
}

test_schema! { with [
        "CREATE TABLE a (id INTEGER PRIMARY KEY)",
        "INSERT INTO a VALUES (11), (12), (13)",
        "CREATE TABLE b (id INTEGER PRIMARY KEY)",
        "INSERT INTO b VALUES (21), (22), (23)",
        "CREATE TABLE c (id INTEGER PRIMARY KEY)",
        "INSERT INTO c VALUES (31), (32), (33)",
    ];
    drop_table: "DROP TABLE a",
    drop_table_bare: "DROP TABLE",
    drop_table_missing: "DROP TABLE name",
    drop_table_multiple: "DROP TABLE a, c",
}

test_schema! { with [
        r#"CREATE TABLE types (
            id INTEGER PRIMARY KEY,
            "boolean" BOOLEAN,
            "float" FLOAT,
            "integer" INTEGER,
            "string" STRING
        )"#
    ];
    insert_boolean_false: r#"INSERT INTO types (id, "boolean") VALUES (0, FALSE)"#,
    insert_boolean_true: r#"INSERT INTO types (id, "boolean") VALUES (0, TRUE)"#,
    insert_boolean_null: r#"INSERT INTO types (id, "boolean") VALUES (0, NULL)"#,
    insert_boolean_float: r#"INSERT INTO types (id, "boolean") VALUES (0, 3.14)"#,
    insert_boolean_integer: r#"INSERT INTO types (id, "boolean") VALUES (0, 1)"#,
    insert_boolean_string: r#"INSERT INTO types (id, "boolean") VALUES (0, 'abc')"#,
    insert_boolean_string_empty: r#"INSERT INTO types (id, "boolean") VALUES (0, '')"#,

    insert_float: r#"INSERT INTO types (id, "float") VALUES (0, 3.14)"#,
    insert_float_min: r#"INSERT INTO types (id, "float") VALUES (0, 1.23456789012345e-307)"#,
    insert_float_min_negative: r#"INSERT INTO types (id, "float") VALUES (0, -1.23456789012345e-307)"#,
    insert_float_min_round: r#"INSERT INTO types (id, "float") VALUES (0, 1.23456789012345e-323)"#,
    insert_float_max: r#"INSERT INTO types (id, "float") VALUES (0, 1.23456789012345e308)"#,
    insert_float_max_negative: r#"INSERT INTO types (id, "float") VALUES (0, -1.23456789012345e308)"#,
    insert_float_infinity: r#"INSERT INTO types (id, "float") VALUES (0, INFINITY)"#,
    insert_float_infinity_negative: r#"INSERT INTO types (id, "float") VALUES (0, -INFINITY)"#,
    insert_float_nan: r#"INSERT INTO types (id, "float") VALUES (0, NAN)"#,
    insert_float_null: r#"INSERT INTO types (id, "float") VALUES (0, NULL)"#,
    insert_float_boolean: r#"INSERT INTO types (id, "float") VALUES (0, FALSE)"#,
    insert_float_integer: r#"INSERT INTO types (id, "float") VALUES (0, 1)"#,
    insert_float_string: r#"INSERT INTO types (id, "float") VALUES (0, 'a')"#,
    insert_float_string_empty: r#"INSERT INTO types (id, "float") VALUES (0, '')"#,

    insert_integer: r#"INSERT INTO types (id, "integer") VALUES (0, 1)"#,
    insert_integer_max: r#"INSERT INTO types (id, "integer") VALUES (0, 9223372036854775807)"#,
    insert_integer_min: r#"INSERT INTO types (id, "integer") VALUES (0, -9223372036854775807)"#,
    insert_integer_null: r#"INSERT INTO types (id, "integer") VALUES (0, NULL)"#,
    insert_integer_boolean: r#"INSERT INTO types (id, "integer") VALUES (0, FALSE)"#,
    insert_integer_float: r#"INSERT INTO types (id, "integer") VALUES (0, 1.0)"#,
    insert_integer_float_infinity: r#"INSERT INTO types (id, "integer") VALUES (0, INFINITY)"#,
    insert_integer_float_nan: r#"INSERT INTO types (id, "integer") VALUES (0, NAN)"#,
    insert_integer_string: r#"INSERT INTO types (id, "integer") VALUES (0, 'a')"#,
    insert_integer_string_empty: r#"INSERT INTO types (id, "integer") VALUES (0, '')"#,

    insert_string: r#"INSERT INTO types (id, "string") VALUES (0, 'abc')"#,
    insert_string_empty: r#"INSERT INTO types (id, "string") VALUES (0, '')"#,
    insert_string_unicode: r#"INSERT INTO types (id, "string") VALUES (0, ' Hi! 👋')"#,
    insert_string_1024: &format!(r#"INSERT INTO types (id, "string") VALUES (0, '{}')"#, "a".repeat(1024)),
    insert_string_1025: &format!(r#"INSERT INTO types (id, "string") VALUES (0, '{}')"#, "a".repeat(1025)),
    insert_string_1024_unicode: &format!(r#"INSERT INTO types (id, "string") VALUES (0, '{}')"#, "𐍈".repeat(256)),
    insert_string_1025_unicode: &format!(r#"INSERT INTO types (id, "string") VALUES (0, '{}x')"#, "𐍈".repeat(256)),
    insert_string_null: r#"INSERT INTO types (id, "string") VALUES (0, NULL)"#,
    insert_string_boolean: r#"INSERT INTO types (id, "string") VALUES (0, FALSE)"#,
    insert_string_float: r#"INSERT INTO types (id, "string") VALUES (0, 3.14)"#,
    insert_string_integer: r#"INSERT INTO types (id, "string") VALUES (0, 1)"#,
}

test_schema! { with [
        // FIXME Support non-integer primary keys
        //r#"CREATE TABLE "boolean" (pk BOOLEAN PRIMARY KEY)"#,
        //r#"INSERT INTO "boolean" VALUES (TRUE)"#,
        //r#"CREATE TABLE "float" (pk FLOAT PRIMARY KEY)"#,
        //r#"INSERT INTO "float" VALUES (3.14)"#,
        r#"CREATE TABLE "integer" (pk INTEGER PRIMARY KEY)"#,
        r#"INSERT INTO "integer" VALUES (1)"#,
        //r#"CREATE TABLE "string" (pk STRING PRIMARY KEY)"#,
        //r#"INSERT INTO "string" VALUES ('a')"#,
    ];
    insert_pk_integer: r#"INSERT INTO "integer" VALUES (2)"#,
    insert_pk_integer_conflict: r#"INSERT INTO "integer" VALUES (1)"#,
    insert_pk_integer_zero: r#"INSERT INTO "integer" VALUES (0)"#,
    insert_pk_integer_negative: r#"INSERT INTO "integer" VALUES (-1)"#,
    insert_pk_integer_null: r#"INSERT INTO "integer" VALUES (NULL)"#,
}
