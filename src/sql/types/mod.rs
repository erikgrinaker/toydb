//! Type system for the SQL engine, containing:
//!
//! - Data types (e.g. integers and strings)
//! - Values of a specific type
//! - Rows of values
//! - Expressions made up of values and operators
//! - Schemas made up of tables and columns

mod datatype;
pub mod expression;
pub mod schema;

pub use datatype::{DataType, Row, Value};
