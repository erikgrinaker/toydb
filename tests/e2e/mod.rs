//! End-to-end tests for toyDB. These spin up toyDB clusters as separate child
//! processes using a built binary.
//!
//! TODO: these tests should be rewritten as data-driven golden master tests.

mod client;
pub mod dataset;
mod isolation;
mod recovery;
mod testcluster;

use testcluster::TestCluster;

/// Asserts that a resultset contains the expected rows.
fn assert_rows(result: toydb::ResultSet, expect: Vec<toydb::sql::types::Row>) {
    match result {
        toydb::ResultSet::Query { rows, .. } => {
            pretty_assertions::assert_eq!(rows.collect::<Result<Vec<_>, _>>().unwrap(), expect)
        }
        r => panic!("Unexpected result {:?}", r),
    }
}

/// Asserts that a resultset contains the single expected row.
fn assert_row(result: toydb::ResultSet, expect: toydb::sql::types::Row) {
    assert_rows(result, vec![expect])
}
