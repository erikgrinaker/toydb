///! Tests for the SQL query engine. Runs SQL queries against an in-memory database,
///! and compares the results with golden files stored under tests/sql/query/
use toydb::error::{Error, Result};
use toydb::sql::engine::{Engine, Mode, Transaction};
use toydb::sql::execution::ResultSet;
use toydb::sql::parser::Parser;
use toydb::sql::plan::Plan;
use toydb::sql::types::Row;

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
        fn $name() -> Result<()> {
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
                    (2, 'Action'),
                    (3, 'Comedy')",
                "CREATE TABLE studios (
                    id INTEGER PRIMARY KEY,
                    name STRING NOT NULL,
                    country_id STRING INDEX REFERENCES countries
                )",
                "INSERT INTO studios VALUES
                    (1, 'Mosfilm', 'ru'),
                    (2, 'Lionsgate', 'us'),
                    (3, 'StudioCanal', 'fr'),
                    (4, 'Warner Bros', 'us')",
                "CREATE TABLE movies (
                    id INTEGER PRIMARY KEY,
                    title STRING NOT NULL,
                    studio_id INTEGER NOT NULL INDEX REFERENCES studios,
                    genre_id INTEGER NOT NULL INDEX REFERENCES genres,
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
            ]);
            let engine = super::setup(setup)?;

            let mut mint = Mint::new("tests/sql/query");
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

            let mut txn = engine.begin(Mode::ReadWrite)?;

            write!(f, "Plan: ")?;
            let plan = match Plan::build(ast, &mut txn) {
                Ok(plan) => plan,
                Err(err) => {
                    write!(f, "{:?}", err)?;
                    return Ok(())
                }
            };
            write!(f, "{:#?}\n\n", plan)?;

            write!(f, "Optimized plan: ")?;
            let plan = match plan.optimize(&mut txn) {
                Ok(plan) => plan,
                Err(err) => {
                    write!(f, "{:?}", err)?;
                    return Ok(())
                }
            };
            write!(f, "{:#?}\n\n", plan)?;

            write!(f, "Query: {}\n\n", $query)?;

            write!(f, "Result:")?;
            let result = match plan.execute(&mut txn) {
                Ok(result) => result,
                Err(err) => {
                    write!(f, " {:?}", err)?;
                    return Ok(())
                }
            };
            txn.commit()?;
            match result {
                ResultSet::Query{columns, rows} => {
                    let rows: Vec<Row> = match rows.collect() {
                        Ok(rows) => rows,
                        Err(err) => {
                            write!(f, " {:?}", err)?;
                            return Ok(())
                        }
                    };
                    if !columns.is_empty() || !rows.is_empty() {
                        write!(f, " {:?}\n", columns
                            .into_iter()
                            .map(|c| c.name.unwrap_or_else(|| "?".to_string()))
                            .collect::<Vec<_>>())?;
                        for row in rows {
                            write!(f, "{:?}\n", row)?;
                        }
                    } else {
                        write!(f, " <none>\n")?;
                    }
                }
                result => return Err(Error::Internal(format!("Unexpected result {:?}", result))),
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
    lowercase: "select 1",

    field_single: "SELECT id FROM movies",
    field_multi: "SELECT id, title FROM movies",
    field_ambiguous: "SELECT id FROM movies, genres",
    field_qualified: "SELECT movies.id FROM movies",
    field_qualified_multi: "SELECT movies.id, genres.id FROM movies, genres",
    field_qualified_nested: "SELECT movies.id.value FROM movies",
    field_unknown: "SELECT unknown FROM movies",
    field_unknown_aliased: "SELECT movies.id FROM movies AS m",
    field_unknown_qualified: "SELECT movies.unknown FROM movies",
    field_unknown_table: "SELECT unknown.id FROM movies",
    field_aliased: "SELECT m.id, g.id FROM movies AS m, genres g",

    expr_dynamic: "SELECT 2020 - year AS age FROM movies",
    expr_static: "SELECT 1 + 2 * 3, 'abc' LIKE 'x%' AS nope",
    expr_mixed: "SELECT 1 + 2 * 3, 2020 - released AS age FROM movies",

    as_: r#"SELECT 1, 2 b, 3 AS c, 4 AS "👋", id AS "some id" FROM movies"#,
    as_bare: "SELECT 1 AS",
    as_all: "SELECT * AS all FROM movies",
    as_duplicate: "SELECT 1 AS a, 2 AS a",
    as_qualified: r#"SELECT 1 AS a.b FROM movies"#,

    from_bare: "SELECT * FROM",
    from_multiple: "SELECT * FROM movies, genres, countries",
    from_unknown: "SELECT * FROM unknown",
    from_alias_duplicate: "SELECT * FROM movies a, genres a",
    from_alias_duplicate_join: "SELECT * FROM movies a JOIN genres a ON TRUE",
    from_duplicate: "SELECT * FROM movies, movies",

    where_bare: "SELECT * FROM movies WHERE",
    where_true: "SELECT * FROM movies WHERE TRUE",
    where_false: "SELECT * FROM movies WHERE FALSE",
    where_null: "SELECT * FROM movies WHERE NULL",
    where_expr: "SELECT * FROM movies WHERE released >= 2000 AND ultrahd",
    where_float: "SELECT * FROM movies WHERE 3.14",
    where_integer: "SELECT * FROM movies WHERE 7",
    where_string: "SELECT * FROM movies WHERE 'abc'",
    where_multi: "SELECT * FROM movies WHERE TRUE, TRUE",
    where_pk: "SELECT * FROM movies WHERE id = 3",
    where_pk_or: "SELECT * FROM movies WHERE id = 3 OR id = 5 OR id = 7",
    where_pk_or_partial: "SELECT * FROM movies WHERE (id = 2 OR id = 3 OR id = 4 OR id = 5) AND genre_id = 1",
    where_index: "SELECT * FROM movies WHERE genre_id = 2 ORDER BY id",
    where_index_or: "SELECT * FROM movies WHERE genre_id = 2 OR genre_id = 3 OR genre_id = 4 OR genre_id = 5 ORDER BY id",
    where_index_or_partial: "SELECT * FROM movies WHERE (genre_id = 2 OR genre_id = 3) AND studio_id = 2 ORDER BY id",
    where_field_unknown: "SELECT * FROM movies WHERE unknown",
    where_field_qualified: "SELECT movies.id, genres.id FROM movies, genres WHERE movies.id >= 3 AND genres.id = 1",
    where_field_ambiguous: "SELECT movies.id, genres.id FROM movies, genres WHERE id >= 3",
    where_field_aliased_select: "SELECT m.id AS movie_id, g.id AS genre_id FROM movies m, genres g WHERE movie_id >= 3 AND genre_id = 1",
    where_field_aliased_table: "SELECT m.id, g.id FROM movies m, genres g WHERE m.id >= 3 AND g.id = 1",
    where_join_inner: "SELECT * FROM movies, genres WHERE movies.genre_id = genres.id",

    order: "SELECT * FROM movies ORDER BY released",
    order_asc: "SELECT * FROM movies ORDER BY released ASC",
    order_asc_lowercase: "SELECT * FROM movies ORDER BY released asc",
    order_desc: "SELECT * FROM movies ORDER BY released DESC",
    order_desc_lowercase: "SELECT * FROM movies ORDER BY released desc",
    order_expr: "SELECT id, title, released, released % 4 AS ord FROM movies ORDER BY released % 4 ASC",
    order_multi: "SELECT * FROM movies ORDER BY ultrahd ASC, id DESC",
    order_noselect: "SELECT id, title FROM movies ORDER BY released",
    order_unknown_dir: "SELECT * FROM movies ORDER BY id X",
    order_field_unknown: "SELECT * FROM movies ORDER BY unknown",
    order_field_qualified: "SELECT movies.id, title, name FROM movies, genres WHERE movies.genre_id = genres.id ORDER BY genres.name, movies.title",
    order_field_aliased: "SELECT movies.id, title, genres.name AS genre FROM movies, genres WHERE movies.genre_id = genres.id ORDER BY genre, title",
    order_field_ambiguous: "SELECT * FROM movies, genres WHERE movies.genre_id = genres.id ORDER BY id",
    order_trailing_comma: "SELECT * FROM movies ORDER BY id,",
    order_aggregate: "SELECT studio_id, MAX(rating) FROM movies GROUP BY studio_id ORDER BY MAX(rating)",
    order_aggregate_noselect: "SELECT studio_id, MAX(rating) FROM movies GROUP BY studio_id ORDER BY MIN(rating)",
    order_group_by_noselect: "SELECT MAX(rating) FROM movies GROUP BY studio_id ORDER BY studio_id",
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
        "INSERT INTO floats VALUES (1, 3.14), (2, -2.718), (3, NULL), (4, 1.618), (5, 0.0)",
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
    join_cross_alias: r#"
        SELECT m.id, m.title, g.id, g.name, c.id, c.name
        FROM movies AS m CROSS JOIN genres g CROSS JOIN countries c
        WHERE m.id >= 3 AND g.id = 2 AND c.id != 'us'
    "#,
    join_cross_multi: "SELECT * FROM movies CROSS JOIN genres CROSS JOIN countries CROSS JOIN studios",
    join_cross_on: "SELECT * FROM movies CROSS JOIN genres ON movies.genre_id = genres.id",

    join_inner: "SELECT * FROM movies INNER JOIN genres ON movies.genre_id = genres.id",
    join_inner_implicit: "SELECT * FROM movies JOIN genres ON movies.genre_id = genres.id",
    join_inner_on_true: "SELECT * FROM movies INNER JOIN genres ON TRUE",
    join_inner_on_false: "SELECT * FROM movies INNER JOIN genres ON FALSE",
    join_inner_on_aliased: "SELECT * FROM movies m INNER JOIN genres g ON m.genre_id = g.id",
    join_inner_on_multi: "SELECT * FROM movies INNER JOIN genres ON movies.genre_id = genres.id AND movies.id = genres.id",
    join_inner_on_missing: "SELECT * FROM movies INNER JOIN genres",
    join_inner_on_where: "SELECT * FROM movies INNER JOIN genres ON movies.genre_id = genres.id WHERE movies.id >= 3",
    join_inner_index_transverse: "SELECT * FROM movies m INNER JOIN genres g ON m.genre_id = g.id AND g.id = 4",
    join_inner_multi: r#"
        SELECT movies.title, genres.name AS genre, studios.name AS studio
        FROM movies
            INNER JOIN genres ON movies.genre_id = genres.id
            INNER JOIN studios ON movies.studio_id = studios.id"#,
    join_inner_multi_index: r#"
        SELECT m.title, g.name AS genre, s.name AS studio
        FROM movies m
            INNER JOIN genres g ON m.genre_id = g.id AND g.id = 1
            INNER JOIN studios s ON m.studio_id = s.id AND s.id = 4"#,

    join_left: "SELECT m.id AS movie_id, g.id AS genre_id FROM movies m LEFT JOIN genres g ON m.id = g.id",
    join_left_outer: "SELECT m.id AS movie_id, g.id AS genre_id FROM movies m LEFT OUTER JOIN genres g ON m.id = g.id",
    join_left_truncate: "SELECT g.id AS genre_id, m.id AS movie_id FROM genres g LEFT JOIN movies m ON m.id = g.id",

    join_right: "SELECT g.id AS genre_id, m.id AS movie_id FROM genres g RIGHT JOIN movies m ON m.id = g.id",
    join_right_outer: "SELECT g.id AS genre_id, m.id AS movie_id FROM genres g RIGHT OUTER JOIN movies m ON m.id = g.id",
    join_right_truncate: "SELECT m.id AS movie_id, g.id AS genre_id FROM movies m RIGHT JOIN genres g ON m.id = g.id",

    agg_count_star: "SELECT COUNT(*) FROM movies",
    agg_expr: "SELECT SUM(rating * 10) / COUNT(*) FROM movies",
    agg_nested: "SELECT MAX(MIN(rating)) FROM movies",
    agg_ungrouped: "SELECT studio_id, COUNT(*) FROM movies",
    agg_norows: "SELECT MIN(id), MAX(id), SUM(id), COUNT(id), AVG(id) FROM movies WHERE FALSE",
    agg_norows_group: "SELECT MIN(id), MAX(id), SUM(id), COUNT(id), AVG(id) FROM movies WHERE FALSE GROUP BY id",
    agg_const: "SELECT MIN(3), MAX(3), SUM(3), COUNT(3), AVG(3)",
    agg_const_from: "SELECT MIN(3), MAX(3), SUM(3), COUNT(3), AVG(3) FROM genres",
}
test_query! { with [
        "CREATE TABLE booleans (id INTEGER PRIMARY KEY, b BOOLEAN)",
        "INSERT INTO booleans VALUES (1, TRUE), (2, NULL), (3, FALSE)",
    ];
    agg_boolean: "SELECT MIN(b), MAX(b), SUM(b), COUNT(b), AVG(b) FROM booleans WHERE b IS NOT NULL",
    agg_boolean_null: "SELECT MIN(b), MAX(b), SUM(b), COUNT(b), AVG(b) FROM booleans",
}
test_query! { with [
        "CREATE TABLE floats (id INTEGER PRIMARY KEY, f FLOAT)",
        "INSERT INTO floats VALUES (1, 3.14), (2, -2.718), (3, NULL), (4, 1.618), (5, 0.0)",
    ];
    agg_float: "SELECT MIN(f), MAX(f), SUM(f), COUNT(f), AVG(f) FROM floats WHERE f IS NOT NULL",
    agg_float_null: "SELECT MIN(f), MAX(f), SUM(f), COUNT(f), AVG(f) FROM floats",
}
test_query! { with [
        "CREATE TABLE integers (id INTEGER PRIMARY KEY, i INTEGER)",
        "INSERT INTO integers VALUES (1, 7), (2, NULL), (3, -3), (4, 5), (5, 0)",
    ];
    agg_integer: "SELECT MIN(i), MAX(i), SUM(i), COUNT(i), AVG(i) FROM integers WHERE i IS NOT NULL",
    agg_integer_null: "SELECT MIN(i), MAX(i), SUM(i), COUNT(i), AVG(i) FROM integers",
}
test_query! { with [
        "CREATE TABLE strings (id INTEGER PRIMARY KEY, s STRING)",
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
    agg_string: "SELECT MIN(s), MAX(s), SUM(s), COUNT(s), AVG(s) FROM strings WHERE s IS NOT NULL",
    agg_string_null: "SELECT MIN(s), MAX(s), SUM(s), COUNT(s), AVG(s) FROM strings",
}
test_query! {
    group_simple: "SELECT studio_id, MAX(rating) FROM movies GROUP BY studio_id ORDER BY studio_id",
    group_noselect: "SELECT MAX(rating) AS best FROM movies GROUP BY studio_id ORDER BY best DESC",
    group_noaggregate: "SELECT title FROM movies GROUP BY title ORDER BY title ASC",
    group_unknown: "SELECT COUNT(*) FROM movies GROUP BY unknown",
    group_join: "SELECT s.name, COUNT(*) FROM movies m JOIN studios s ON m.studio_id = s.id GROUP BY s.name ORDER BY s.name ASC",
    group_expr: "SELECT MAX(rating) AS rating FROM movies GROUP BY studio_id * 2 ORDER BY rating",
    group_expr_aliased: "SELECT studio_id * 2 AS twice, MAX(rating) FROM movies GROUP BY twice ORDER BY twice",
    group_expr_both: "SELECT studio_id * 2, MAX(rating) AS rating FROM movies GROUP BY studio_id * 2 ORDER BY rating",
    group_expr_extended: "SELECT studio_id * 2 + 1, MAX(rating) AS rating FROM movies GROUP BY studio_id * 2 ORDER BY rating",
    group_expr_select: "SELECT studio_id * 2, MAX(rating) AS rating FROM movies GROUP BY studio_id ORDER BY rating",
    group_expr_aggr: "SELECT studio_id, SUM(rating * 10) / COUNT(*) FROM movies GROUP BY studio_id ORDER BY studio_id",
    group_expr_aggr_selfref: "SELECT studio_id, SUM(rating * 10) / COUNT(*) + studio_id FROM movies GROUP BY studio_id ORDER BY studio_id",
    group_expr_aggr_nogroupref: "SELECT studio_id, SUM(rating * 10) / COUNT(*) + id FROM movies GROUP BY studio_id ORDER BY studio_id",
    group_expr_multigroup: "SELECT studio_id + genre_id AS multi, MAX(rating) AS rating FROM movies GROUP BY studio_id, genre_id ORDER BY rating, multi",

    having: "SELECT studio_id, MAX(rating) AS rating FROM movies GROUP BY studio_id HAVING rating > 8 ORDER BY studio_id",
    having_aggr: "SELECT studio_id, MAX(rating) FROM movies GROUP BY studio_id HAVING MIN(rating) > 7 ORDER BY studio_id",
    having_aggr_expr: "SELECT studio_id, MAX(rating) FROM movies GROUP BY studio_id HAVING MAX(rating) - MIN(rating) < 1 ORDER BY studio_id",
    having_aggr_nested: "SELECT studio_id, MAX(rating) AS best FROM movies GROUP BY studio_id HAVING MIN(best) > 7 ORDER BY studio_id",
    having_nogroup: "SELECT id, rating FROM movies HAVING rating > 8 ORDER BY id",
    having_noselect: "SELECT studio_id FROM movies GROUP BY studio_id HAVING MAX(rating) > 8 ORDER BY studio_id",
    having_noaggr: "SELECT studio_id, MAX(rating) AS rating FROM movies GROUP BY studio_id HAVING studio_id >= 3 ORDER BY studio_id",
}
