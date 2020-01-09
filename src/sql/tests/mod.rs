mod expression;
mod schema;
mod sql;

use super::{Context, Engine, Parser, Plan, ResultSet, Transaction};
use crate::kv;
use crate::Error;

fn setup(queries: Vec<&str>) -> Result<super::engine::KV<kv::storage::Memory>, Error> {
    let engine = super::engine::KV::new(kv::MVCC::new(kv::storage::Memory::new()));
    let mut txn = engine.begin()?;
    for query in queries {
        let ast = Parser::new(query).parse()?;
        let plan = Plan::build(ast)?.optimize()?;
        plan.execute(Context { txn: &mut txn })?;
    }
    txn.commit()?;
    Ok(engine)
}

fn execute<E: Engine>(engine: &E, query: &str) -> Result<ResultSet, Error> {
    let mut txn = engine.begin()?;
    let ast = Parser::new(query).parse()?;
    let plan = Plan::build(ast)?.optimize()?;
    let result = plan.execute(Context { txn: &mut txn });
    if result.is_ok() {
        txn.commit()?;
    } else {
        txn.rollback()?;
    }
    result
}
