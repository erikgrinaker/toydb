mod dml;
mod expression;
mod schema;
mod sql;

use super::Engine;
use crate::kv;
use crate::Error;

fn setup(queries: Vec<&str>) -> Result<super::engine::KV<kv::storage::Memory>, Error> {
    let engine = super::engine::KV::new(kv::MVCC::new(kv::storage::Memory::new()));
    let mut session = engine.session(None)?;
    session.execute("BEGIN")?;
    for query in queries {
        session.execute(query)?;
    }
    session.execute("COMMIT")?;
    Ok(engine)
}
