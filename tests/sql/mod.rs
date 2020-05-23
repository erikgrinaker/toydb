mod dml;
mod expression;
mod query;
mod schema;

use toydb::error::Result;
use toydb::sql::engine::{Engine, KV};
use toydb::storage::kv;

fn setup(queries: Vec<&str>) -> Result<KV> {
    let engine = KV::new(kv::MVCC::new(Box::new(kv::Memory::new())));
    let mut session = engine.session()?;
    session.execute("BEGIN")?;
    for query in queries {
        session.execute(query)?;
    }
    session.execute("COMMIT")?;
    Ok(engine)
}
