mod query;

use toydb::error::Result;
use toydb::sql::engine::{Engine, Local};
use toydb::storage;

/// Sets up a basic in-memory SQL engine with an initial dataset.
fn setup(queries: Vec<&str>) -> Result<Local<storage::Memory>> {
    let engine = Local::new(storage::Memory::new());
    {
        let mut session = engine.session();
        session.execute("BEGIN")?;
        for query in queries {
            session.execute(query)?;
        }
        session.execute("COMMIT")?;
    }
    Ok(engine)
}
