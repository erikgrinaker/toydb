use toydb::sql::execution::ResultSet;
use toydb::sql::types::{Relation, Row};

use pretty_assertions::assert_eq;

/// Asserts that a resultset contains the expected rows.
pub fn assert_rows(result: ResultSet, expect: Vec<Row>) {
    match result {
        ResultSet::Query { relation: Relation { rows: Some(rows), .. } } => {
            assert_eq!(rows.collect::<Result<Vec<_>, _>>().unwrap(), expect)
        }
        r => panic!("Unexpected result {:?}", r),
    }
}

/// Asserts that a resultset contains the single expected row.
pub fn assert_row(result: ResultSet, expect: Row) {
    assert_rows(result, vec![expect])
}
