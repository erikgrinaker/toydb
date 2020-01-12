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
    create_table_pk_default: "CREATE TABLE name (id INTEGER PRIMARY KEY DEFAULT 1)",
    create_table_pk_unique: "CREATE TABLE name (id INTEGER PRIMARY KEY UNIQUE)",

    create_table_null: "CREATE TABLE name (id INTEGER PRIMARY KEY, value STRING NULL)",
    create_table_null_not: "CREATE TABLE name (id INTEGER PRIMARY KEY, value STRING NOT NULL)",
    create_table_null_not_null: "CREATE TABLE name (id INTEGER PRIMARY KEY, value STRING NULL NOT NULL)",
    create_table_null_default: "CREATE TABLE name (id INTEGER PRIMARY KEY, value STRING)",

    create_table_default_boolean: "CREATE TABLE name (id INTEGER PRIMARY KEY, value BOOLEAN DEFAULT TRUE)",
    create_table_default_float: "CREATE TABLE name (id INTEGER PRIMARY KEY, value FLOAT DEFAULT 3.14)",
    create_table_default_integer: "CREATE TABLE name (id INTEGER PRIMARY KEY, value INTEGER DEFAULT 7)",
    create_table_default_string: "CREATE TABLE name (id INTEGER PRIMARY KEY, value STRING DEFAULT 'foo')",
    create_table_default_null: "CREATE TABLE name (id INTEGER PRIMARY KEY, value STRING DEFAULT NULL)",
    create_table_default_null_not: "CREATE TABLE name (id INTEGER PRIMARY KEY, value STRING NOT NULL DEFAULT NULL)",
    create_table_default_expr: "CREATE TABLE name (id INTEGER PRIMARY KEY, value INTEGER DEFAULT 1 + 2 * 3)",
    create_table_default_conflict: "CREATE TABLE name (id INTEGER PRIMARY KEY, value STRING DEFAULT 7)",
    create_table_default_conflict_float_integer: "CREATE TABLE name (id INTEGER PRIMARY KEY, value FLOAT DEFAULT 7)",
    create_table_default_conflict_integer_float: "CREATE TABLE name (id INTEGER PRIMARY KEY, value INTEGER DEFAULT 3.14)",

    create_table_unique: "CREATE TABLE name (id INTEGER PRIMARY KEY, value STRING UNIQUE)",
    create_table_unique_null: "CREATE TABLE name (id INTEGER PRIMARY KEY, value STRING NULL UNIQUE)",
    create_table_unique_not_null: "CREATE TABLE name (id INTEGER PRIMARY KEY, value STRING NOT NULL UNIQUE)",
    create_table_unique_default: "CREATE TABLE name (id INTEGER PRIMARY KEY, value STRING DEFAULT 'foo' UNIQUE)",
}
test_schema! { with ["CREATE TABLE test (id INTEGER PRIMARY KEY)"];
    create_table_exists: "CREATE TABLE test (id INTEGER PRIMARY KEY)",

    create_table_ref: "CREATE TABLE other (id INTEGER PRIMARY KEY, test_id INTEGER REFERENCES test)",
    create_table_ref_multiple: "CREATE TABLE other (
        id INTEGER PRIMARY KEY,
        test_id_a INTEGER REFERENCES test,
        test_id_b INTEGER REFERENCES test,
        test_id_c INTEGER REFERENCES test
    )",
    create_table_ref_missing: "CREATE TABLE other (id INTEGER PRIMARY KEY, missing_id INTEGER REFERENCES missing)",
    create_table_ref_type: "CREATE TABLE other (id INTEGER PRIMARY KEY, test_id STRING REFERENCES test)",
    create_table_ref_self: "CREATE TABLE other (id INTEGER PRIMARY KEY, self_id INTEGER REFERENCES other)",
    create_table_ref_self_type: "CREATE TABLE other (id INTEGER PRIMARY KEY, self_id STRING REFERENCES other)",
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
        "CREATE TABLE target (id INTEGER PRIMARY KEY)",
        "CREATE TABLE source (id INTEGER PRIMARY KEY, target_id INTEGER REFERENCES target)",
        "CREATE TABLE self (id INTEGER PRIMARY KEY, self_id INTEGER REFERENCES self)",
    ];
    drop_table_ref_source: "DROP TABLE source",
    drop_table_ref_target: "DROP TABLE target",
    drop_table_ref_self: "DROP TABLE self",
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
        "CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)",
        "INSERT INTO test VALUES (1, 7)",
    ];
    update_integer: "UPDATE test SET value = 3",
    update_integer_conflict: "UPDATE test SET value = 'abc'",
    update_integer_float: "UPDATE test SET value = 3.14",
    update_integer_null: "UPDATE test SET value = NULL",
}

test_schema! { with [
        r#"CREATE TABLE "boolean" (pk BOOLEAN PRIMARY KEY)"#,
        r#"INSERT INTO "boolean" VALUES (FALSE)"#,
    ];
    insert_pk_boolean: r#"INSERT INTO "boolean" VALUES (TRUE)"#,
    insert_pk_boolean_conflict: r#"INSERT INTO "boolean" VALUES (FALSE)"#,
    insert_pk_boolean_null: r#"INSERT INTO "boolean" VALUES (NULL)"#,

    update_pk_boolean: r#"UPDATE "boolean" SET pk = TRUE WHERE pk = FALSE"#,
    update_pk_boolean_null: r#"UPDATE "boolean" SET pk = NULL WHERE pk = FALSE"#,
}

test_schema! { with [
        r#"CREATE TABLE "float" (pk FLOAT PRIMARY KEY)"#,
        r#"INSERT INTO "float" VALUES (3.14), (2.718), (NAN), (INFINITY)"#,
    ];
    insert_pk_float: r#"INSERT INTO "float" VALUES (1.618)"#,
    insert_pk_float_conflict: r#"INSERT INTO "float" VALUES (3.14)"#,
    insert_pk_float_zero: r#"INSERT INTO "float" VALUES (0.0)"#,
    insert_pk_float_negative: r#"INSERT INTO "float" VALUES (-3.14)"#,
    insert_pk_float_nan: r#"INSERT INTO "float" VALUES (NAN)"#,
    insert_pk_float_infinity: r#"INSERT INTO "float" VALUES (INFINITY)"#,
    insert_pk_float_null: r#"INSERT INTO "float" VALUES (NULL)"#,

    update_pk_float: r#"UPDATE "float" SET pk = 1.618 WHERE pk = 3.14"#,
    update_pk_float_conflict: r#"UPDATE "float" SET pk = 2.718 WHERE pk = 3.14"#,
    update_pk_float_conflict_all: r#"UPDATE "float" SET pk = 3.14"#,
    update_pk_float_null: r#"UPDATE "float" SET pk = NULL WHERE pk = 3.14"#,
}

test_schema! { with [
        r#"CREATE TABLE "integer" (pk INTEGER PRIMARY KEY)"#,
        r#"INSERT INTO "integer" VALUES (1), (2)"#,
    ];
    insert_pk_integer: r#"INSERT INTO "integer" VALUES (3)"#,
    insert_pk_integer_conflict: r#"INSERT INTO "integer" VALUES (1)"#,
    insert_pk_integer_zero: r#"INSERT INTO "integer" VALUES (0)"#,
    insert_pk_integer_negative: r#"INSERT INTO "integer" VALUES (-1)"#,
    insert_pk_integer_null: r#"INSERT INTO "integer" VALUES (NULL)"#,

    update_pk_integer: r#"UPDATE "integer" SET pk = 3 WHERE pk = 2"#,
    update_pk_integer_conflict: r#"UPDATE "integer" SET pk = 1 WHERE pk = 2"#,
    update_pk_integer_conflict_all: r#"UPDATE "integer" SET pk = 1"#,
    update_pk_integer_null: r#"UPDATE "integer" SET pk = NULL WHERE pk = 2"#,
}

test_schema! { with [
        r#"CREATE TABLE "string" (pk STRING PRIMARY KEY)"#,
        r#"INSERT INTO "string" VALUES ('foo'), ('bar')"#,
    ];
    insert_pk_string: r#"INSERT INTO "string" VALUES ('baz')"#,
    insert_pk_string_case: r#"INSERT INTO "string" VALUES ('Foo')"#,
    insert_pk_string_conflict: r#"INSERT INTO "string" VALUES ('foo')"#,
    insert_pk_string_empty: r#"INSERT INTO "string" VALUES ('')"#,
    insert_pk_string_null: r#"INSERT INTO "string" VALUES (NULL)"#,

    update_pk_string: r#"UPDATE "string" SET pk = 'baz' WHERE pk = 'foo'"#,
    update_pk_string_case: r#"UPDATE "string" SET pk = 'Bar' WHERE pk = 'foo'"#,
    update_pk_string_conflict: r#"UPDATE "string" SET pk = 'bar' WHERE pk = 'foo'"#,
    update_pk_string_conflict_all: r#"UPDATE "string" SET pk = 'foo'"#,
    update_pk_string_null: r#"UPDATE "string" SET pk = NULL WHERE pk = 'foo'"#,
}

test_schema! { with [
    r#"CREATE TABLE nulls (
        id INTEGER PRIMARY KEY,
        "null" BOOLEAN NULL,
        not_null BOOLEAN NOT NULL,
        "default" BOOLEAN
    )"#];
    insert_nulls: r#"INSERT INTO nulls (id, "null", not_null, "default") VALUES (1, NULL, TRUE, NULL)"#,
    insert_nulls_default: r#"INSERT INTO nulls (id, "null", not_null) VALUES (1, NULL, TRUE)"#,
    insert_nulls_required: r#"INSERT INTO nulls (id, "null", not_null, "default") VALUES (1, NULL, NULL, NULL)"#,
}

test_schema! { with [
    r#"CREATE TABLE defaults (
        id INTEGER PRIMARY KEY,
        required BOOLEAN NOT NULL,
        "null" BOOLEAN,
        "boolean" BOOLEAN DEFAULT TRUE,
        "float" FLOAT DEFAULT 3.14,
        "integer" INTEGER DEFAULT 7,
        "string" STRING DEFAULT 'foo'
    )"#];
    insert_default: "INSERT INTO defaults (id, required) VALUES (1, TRUE)",
    insert_default_unnamed: "INSERT INTO defaults VALUES (1, TRUE)",
    insert_default_missing: "INSERT INTO defaults (id) VALUES (1)",
    insert_default_override: "INSERT INTO defaults VALUES (1, TRUE, TRUE, FALSE, 2.718, 3, 'bar')",
    insert_default_override_null: "INSERT INTO defaults VALUES (1, TRUE, NULL, NULL, NULL, NULL, NULL)",
}

test_schema! { with [
        r#"CREATE TABLE "unique" (
            id INTEGER PRIMARY KEY,
            "boolean" BOOLEAN UNIQUE,
            "float" FLOAT UNIQUE,
            "integer" INTEGER UNIQUE,
            "string" STRING UNIQUE
        )"#,
        r#"INSERT INTO "unique" VALUES (0, NULL, NULL, NULL, NULL)"#,
        r#"INSERT INTO "unique" VALUES (1, TRUE, 3.14, 7, 'foo')"#,
    ];
    insert_unique_boolean: r#"INSERT INTO "unique" (id, "boolean") VALUES (2, FALSE)"#,
    insert_unique_boolean_duplicate: r#"INSERT INTO "unique" (id, "boolean") VALUES (2, TRUE)"#,
    insert_unique_float: r#"INSERT INTO "unique" (id, "float") VALUES (2, 2.718)"#,
    insert_unique_float_duplicate: r#"INSERT INTO "unique" (id, "float") VALUES (2, 3.14)"#,
    insert_unique_integer: r#"INSERT INTO "unique" (id, "integer") VALUES (2, 3)"#,
    insert_unique_integer_duplicate: r#"INSERT INTO "unique" (id, "integer") VALUES (2, 7)"#,
    insert_unique_string: r#"INSERT INTO "unique" (id, "string") VALUES (2, 'bar')"#,
    insert_unique_string_duplicate: r#"INSERT INTO "unique" (id, "string") VALUES (2, 'foo')"#,
    insert_unique_string_case: r#"INSERT INTO "unique" (id, "string") VALUES (2, 'Foo')"#,
    insert_unique_nulls: r#"INSERT INTO "unique" VALUES (2, NULL, NULL, NULL, NULL)"#,

    update_unique_boolean: r#"UPDATE "unique" SET "boolean" = FALSE WHERE id = 0"#,
    update_unique_boolean_duplicate: r#"UPDATE "unique" SET "boolean" = TRUE WHERE id = 0"#,
    update_unique_boolean_same: r#"UPDATE "unique" SET "boolean" = TRUE WHERE id = 1"#,
    update_unique_nulls: r#"UPDATE "unique" SET "boolean" = NULL, "float" = NULL, "integer" = NULL, "string" = NULL WHERE id = 1"#,
}

test_schema! { with [
        "CREATE TABLE target (id BOOLEAN PRIMARY KEY)",
        "INSERT INTO target VALUES (TRUE)",
        "CREATE TABLE source (id INTEGER PRIMARY KEY, target_id BOOLEAN REFERENCES target)",
    ];
    insert_ref_boolean: "INSERT INTO source VALUES (1, TRUE)",
    insert_ref_boolean_null: "INSERT INTO source VALUES (1, NULL)",
    insert_ref_boolean_missing: "INSERT INTO source VALUES (1, FALSE)",
}

test_schema! { with [
        "CREATE TABLE target (id FLOAT PRIMARY KEY)",
        "INSERT INTO target VALUES (3.14), (2.718)",
        "CREATE TABLE source (id INTEGER PRIMARY KEY, target_id FLOAT REFERENCES target)",
    ];
    insert_ref_float: "INSERT INTO source VALUES (1, 3.14)",
    insert_ref_float_null: "INSERT INTO source VALUES (1, NULL)",
    insert_ref_float_missing: "INSERT INTO source VALUES (1, 1.618)",
}

test_schema! { with [
        "CREATE TABLE target (id INTEGER PRIMARY KEY)",
        "INSERT INTO target VALUES (1), (2), (3)",
        "CREATE TABLE source (id INTEGER PRIMARY KEY, target_id INTEGER REFERENCES target)",
    ];
    insert_ref_integer: "INSERT INTO source VALUES (1, 1)",
    insert_ref_integer_null: "INSERT INTO source VALUES (1, NULL)",
    insert_ref_integer_missing: "INSERT INTO source VALUES (1, 7)",
}

test_schema! { with [
        "CREATE TABLE target (id STRING PRIMARY KEY)",
        "INSERT INTO target VALUES ('foo'), ('bar')",
        "CREATE TABLE source (id INTEGER PRIMARY KEY, target_id STRING REFERENCES target)",
    ];
    insert_ref_string: "INSERT INTO source VALUES (1, 'foo')",
    insert_ref_string_case: "INSERT INTO source VALUES (1, 'Foo')",
    insert_ref_string_null: "INSERT INTO source VALUES (1, NULL)",
    insert_ref_string_missing: "INSERT INTO source VALUES (1, 'baz')",
}

test_schema! { with [
        "CREATE TABLE target (id INTEGER PRIMARY KEY, value STRING)",
        "INSERT INTO target VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        "CREATE TABLE source (id INTEGER PRIMARY KEY, target_id INTEGER REFERENCES target)",
        "INSERT INTO source VALUES (1, 1), (2, 2), (4, NULL)",
    ];
    delete_ref_conflict: "DELETE FROM target WHERE id = 1",
    delete_ref_noref: "DELETE FROM target WHERE id = 3",
    delete_ref_source: "DELETE FROM source WHERE id = 1",

    update_ref_value: "UPDATE target SET value = 'x' WHERE id = 1",
    update_ref_pk: "UPDATE target SET id = 9 WHERE id = 1",
    update_ref_pk_noref: "UPDATE target SET id = 9 WHERE id = 3",
    update_ref_source: "UPDATE source SET target_id = 1 WHERE id = 4",
    update_ref_source_missing: "UPDATE source SET target_id = 9 WHERE id = 4",
    update_ref_source_null: "UPDATE source SET target_id = NULL WHERE id = 2",
}

test_schema! { with [
        "CREATE TABLE self (id INTEGER PRIMARY KEY, self_id INTEGER REFERENCES self, value STRING)",
        "INSERT INTO self VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 3, 'c'), (4, NULL, 'd')",
    ];
    delete_ref_self: "DELETE FROM self WHERE id = 3",
    delete_ref_self_all: "DELETE FROM self",
    delete_ref_self_conflict: "DELETE FROM self WHERE id = 1",

    insert_ref_self: "INSERT INTO self VALUES (5, 1, 'e')",
    insert_ref_self_null: "INSERT INTO self VALUES (5, NULL, 'e')",
    insert_ref_self_missing: "INSERT INTO self VALUES (5, 9, 'e')",
    insert_ref_self_self: "INSERT INTO self VALUES (5, 5, 'e')",

    update_ref_self_value: "UPDATE self SET value = 'x' WHERE id = 1",
    update_ref_self_pk: "UPDATE self SET id = 9 WHERE id = 1",
    update_ref_self_pk_noref: "UPDATE self SET id = 9 WHERE id = 2",
    update_ref_self_self: "UPDATE self SET self_id = 2 WHERE id = 2",
}
