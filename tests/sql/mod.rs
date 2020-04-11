mod dml;
mod expression;
mod query;
mod schema;

use toydb::kv;
use toydb::sql::engine::{Engine, KV};
use toydb::Error;

fn setup(queries: Vec<&str>) -> Result<KV<kv::storage::Memory>, Error> {
    let engine = KV::new(kv::MVCC::new(kv::storage::Memory::new()));
    let mut session = engine.session(None)?;
    session.execute("BEGIN")?;
    for query in queries {
        session.execute(query)?;
    }
    session.execute("COMMIT")?;
    Ok(engine)
}
