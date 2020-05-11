mod dml;
mod expression;
mod query;
mod schema;

use toydb::sql::engine::{Engine, KV};
use toydb::storage::kv;
use toydb::Error;

fn setup(queries: Vec<&str>) -> Result<KV<kv::Memory>, Error> {
    let engine = KV::new(kv::MVCC::new(kv::Memory::new()));
    let mut session = engine.session()?;
    session.execute("BEGIN")?;
    for query in queries {
        session.execute(query)?;
    }
    session.execute("COMMIT")?;
    Ok(engine)
}
