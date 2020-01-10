use super::super::types;
use super::super::types::schema;
use super::Mode;
use crate::kv;
use crate::utility::{deserialize, serialize};
use crate::Error;

pub struct KV<S: kv::storage::Storage> {
    kv: kv::MVCC<S>,
}

impl<S: kv::storage::Storage> KV<S> {
    pub fn new(kv: kv::MVCC<S>) -> Self {
        Self { kv }
    }
}

impl<S: kv::storage::Storage> super::Engine for KV<S> {
    type Transaction = Transaction<S>;

    fn begin_with_mode(&self, mode: Mode) -> Result<Self::Transaction, Error> {
        Ok(Self::Transaction::new(self.kv.begin_with_mode(mode)?))
    }

    fn resume(&self, id: u64) -> Result<Self::Transaction, Error> {
        Ok(Self::Transaction::new(self.kv.resume(id)?))
    }
}

pub struct Transaction<S: kv::storage::Storage> {
    txn: kv::Transaction<S>,
}

impl<S: kv::storage::Storage> Transaction<S> {
    fn new(txn: kv::Transaction<S>) -> Self {
        Self { txn }
    }
}

impl<S: kv::storage::Storage> super::Transaction for Transaction<S> {
    fn id(&self) -> u64 {
        self.txn.id()
    }

    fn mode(&self) -> Mode {
        self.txn.mode()
    }

    fn commit(self) -> Result<(), Error> {
        self.txn.commit()
    }

    fn rollback(self) -> Result<(), Error> {
        self.txn.rollback()
    }

    fn create(&mut self, table: &str, row: types::Row) -> Result<(), Error> {
        let table = self
            .read_table(&table)?
            .ok_or_else(|| Error::Value(format!("Table {} does not exist", table)))?;

        // Validate row
        table.validate_row(&row)?;

        // Validate primary key conflicts
        let id = table.row_key(&row)?;
        if self.read(&table.name, &id)?.is_some() {
            return Err(Error::Value(format!(
                "Primary key {} already exists for table {}",
                id, table.name
            )));
        }

        // Validate referential integrity
        for (target, pks) in table.row_references(&row)?.into_iter() {
            for pk in pks.into_iter() {
                if target == table.name && pk == table.row_key(&row)? {
                    continue;
                }
                if self.read(&target, &pk)?.is_none() {
                    return Err(Error::Value(format!(
                        "Referenced primary key {} in table {} does not exist",
                        pk, target,
                    )));
                }
            }
        }

        // Validate uniqueness
        let unique: Vec<_> = table
            .columns
            .iter()
            .zip(row.iter())
            .enumerate()
            .filter(|(_i, (c, v))| c.unique && !c.primary_key && v != &&types::Value::Null)
            .map(|(i, (c, v))| (i, c, v))
            .collect();
        if !unique.is_empty() {
            let mut scan = self.scan(&table.name)?;
            while let Some(r) = scan.next().transpose()? {
                for (i, c, v) in unique.iter() {
                    if &r.get(*i).unwrap_or_else(|| &types::Value::Null) == v {
                        return Err(Error::Value(format!(
                            "Unique value {} already exists for column {}",
                            v, c.name
                        )));
                    }
                }
            }
        }

        self.txn.set(&Key::Row(&table.name, &id).encode(), serialize(&row)?)
    }

    fn delete(&mut self, table: &str, id: &types::Value) -> Result<(), Error> {
        let table = self
            .read_table(&table)?
            .ok_or_else(|| Error::Value(format!("Table {} does not exist", table)))?;

        // FIXME We should avoid full table scans here, but let's wait until we build the
        // predicate pushdown infrastructure which we can use for this as well.
        for source in self.list_tables()? {
            let refs: Vec<_> = source
                .references()
                .into_iter()
                .filter(|c| c.references == Some(table.name.clone()))
                .collect();
            if refs.is_empty() {
                continue;
            }
            let mut scan = self.scan(&source.name)?;
            while let Some(row) = scan.next().transpose()? {
                let row = source.row_to_hashmap(row);
                for r in refs.iter() {
                    if row.get(&r.name).unwrap_or(&types::Value::Null) == id
                        && (source.name != table.name
                            || row.get(&table.primary_key()?.name).unwrap_or(&types::Value::Null)
                                != id)
                    {
                        return Err(Error::Value(format!(
                            "Primary key {} is referenced by table {} column {}",
                            id, source.name, r.name
                        )));
                    }
                }
            }
        }

        self.txn.delete(&Key::Row(&table.name, id).encode())
    }

    fn read(&self, table: &str, id: &types::Value) -> Result<Option<types::Row>, Error> {
        self.txn.get(&Key::Row(table, id).encode())?.map(|v| deserialize(&v)).transpose()
    }

    fn scan(&self, table: &str) -> Result<super::Scan, Error> {
        let from = Key::RowStart(table).encode();
        let to = Key::RowEnd(table).encode();
        // FIXME We buffer results here, to avoid dealing with trait lifetimes
        // right now
        let iter = self
            .txn
            .scan(&from..&to)?
            .map(|res| match res {
                Ok((_, v)) => deserialize(&v),
                Err(err) => Err(err),
            })
            .collect::<Vec<Result<_, Error>>>()
            .into_iter();
        Ok(Box::new(iter))
    }

    fn update(&mut self, table: &str, id: &types::Value, row: types::Row) -> Result<(), Error> {
        let table = self
            .read_table(&table)?
            .ok_or_else(|| Error::Value(format!("Table {} does not exist", table)))?;

        // If the primary key changes, we do a delete and create
        if id != &table.row_key(&row)? {
            self.delete(&table.name, id)?;
            self.create(&table.name, row)

        // Otherwise, we validate the new row and replace the existing one
        } else {
            // Validate row
            table.validate_row(&row)?;

            // Validate referential integrity
            for (target, pks) in table.row_references(&row)?.into_iter() {
                for pk in pks.into_iter() {
                    if target == table.name && &pk == id {
                        continue;
                    }
                    if self.read(&target, &pk)?.is_none() {
                        return Err(Error::Value(format!(
                            "Referenced primary key {} in table {} does not exist",
                            pk, target,
                        )));
                    }
                }
            }

            // Validate uniqueness
            let unique: Vec<_> = table
                .columns
                .iter()
                .zip(row.iter())
                .enumerate()
                .filter(|(_i, (c, v))| c.unique && !c.primary_key && v != &&types::Value::Null)
                .map(|(i, (c, v))| (i, c, v))
                .collect();
            if !unique.is_empty() {
                let mut scan = self.scan(&table.name)?;
                while let Some(r) = scan.next().transpose()? {
                    if &table.row_key(&r)? == id {
                        continue;
                    }
                    for (i, c, v) in unique.iter() {
                        if &r.get(*i).unwrap_or_else(|| &types::Value::Null) == v {
                            return Err(Error::Value(format!(
                                "Unique value {} already exists for column {}",
                                v, c.name
                            )));
                        }
                    }
                }
            }

            self.txn.set(&Key::Row(&table.name, &id).encode(), serialize(&row)?)
        }
    }

    fn create_table(&mut self, table: &schema::Table) -> Result<(), Error> {
        if self.read_table(&table.name)?.is_some() {
            return Err(Error::Value(format!("Table {} already exists", table.name)));
        }
        for column in table.references() {
            let target = match &column.references {
                Some(target) if target == &table.name => table.clone(),
                Some(target) => self.read_table(&target)?.ok_or_else(|| {
                    Error::Value(format!(
                        "Table {} referenced by column {} does not exist",
                        target, column.name
                    ))
                })?,
                None => {
                    return Err(Error::Internal(
                        "Table.references() returned non-reference column".into(),
                    ))
                }
            };
            if column.datatype != target.primary_key()?.datatype {
                return Err(Error::Value(format!(
                    "Can't reference {} primary key of table {} from {} column {}",
                    target.primary_key()?.datatype,
                    target.name,
                    column.datatype,
                    column.name
                )));
            }
        }
        self.txn.set(&Key::Table(&table.name).encode(), serialize(table)?)
    }

    fn delete_table(&mut self, table: &str) -> Result<(), Error> {
        if self.read_table(table)?.is_none() {
            return Err(Error::Value(format!("Table {} does not exist", table)));
        }
        // Check for incoming references
        // FIXME This needs to be indexed or cached
        for source in self.list_tables()? {
            for column in source.references() {
                if source.name != table && column.references.as_ref() == Some(&table.to_string()) {
                    return Err(Error::Value(format!(
                        "Table {} is referenced by table {} column {}",
                        table, source.name, column.name
                    )));
                }
            }
        }
        // FIXME Needs to delete all table data as well
        self.txn.delete(&Key::Table(table).encode())
    }

    fn list_tables(&self) -> Result<Vec<schema::Table>, Error> {
        self.txn
            .scan(&Key::TableStart.encode()..&Key::TableEnd.encode())?
            .collect::<Result<Vec<_>, Error>>()?
            .into_iter()
            .map(|(_, v)| deserialize(&v))
            .collect()
    }

    fn read_table(&self, table: &str) -> Result<Option<schema::Table>, Error> {
        self.txn.get(&Key::Table(table).encode())?.map(|v| deserialize(&v)).transpose()
    }
}

enum Key<'a> {
    TableStart,
    Table(&'a str),
    TableEnd,
    RowStart(&'a str),
    Row(&'a str, &'a types::Value),
    RowEnd(&'a str),
}

impl<'a> Key<'a> {
    fn encode(self) -> Vec<u8> {
        match self {
            Self::TableStart => vec![0x01],
            Self::Table(name) => [vec![0x01], name.as_bytes().to_vec()].concat(),
            Self::TableEnd => vec![0x02],
            Self::RowStart(table) => [vec![0x03], table.as_bytes().to_vec(), vec![0x00]].concat(),
            Self::Row(table, pk) => [
                vec![0x03],
                table.as_bytes().to_vec(),
                vec![0x00],
                match pk {
                    types::Value::Boolean(b) if *b => vec![0x01],
                    types::Value::Boolean(_) => vec![0x00],
                    types::Value::Float(f) => f.to_be_bytes().to_vec(),
                    types::Value::Integer(i) => i.to_be_bytes().to_vec(),
                    types::Value::String(s) => s.as_bytes().to_vec(),
                    types::Value::Null => panic!("Received NULL primary key"),
                },
            ]
            .concat(),
            Self::RowEnd(table) => [vec![0x03], table.as_bytes().to_vec(), vec![0xff]].concat(),
        }
    }
}
