mod expression;
mod sql;

use super::{Context, Engine, Parser, Plan, Transaction};
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
