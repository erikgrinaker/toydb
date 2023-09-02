mod expression;
mod mutation;
mod query;
mod schema;

use toydb::error::Result;
use toydb::sql::engine::{Engine, KV};
use toydb::storage;

/// Sets up a basic in-memory SQL engine with an initial dataset.
fn setup(queries: Vec<&str>) -> Result<KV<storage::engine::Memory>> {
    let engine = KV::new(storage::engine::Memory::new());
    let mut session = engine.session()?;
    session.execute("BEGIN")?;
    for query in queries {
        session.execute(query)?;
    }
    session.execute("COMMIT")?;
    Ok(engine)
}
