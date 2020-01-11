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
        table.validate_row(&row, self)?;
        let id = table.row_key(&row)?;
        if self.read(&table.name, &id)?.is_some() {
            return Err(Error::Value(format!(
                "Primary key {} already exists for table {}",
                id, table.name
            )));
        }
        self.txn.set(&Key::Row(&table.name, &id).encode(), serialize(&row)?)
    }

    fn delete(&mut self, table: &str, id: &types::Value) -> Result<(), Error> {
        let table = self
            .read_table(&table)?
            .ok_or_else(|| Error::Value(format!("Table {} does not exist", table)))?;
        table.assert_pk_unreferenced(id, self)?;
        self.txn.delete(&Key::Row(&table.name, id).encode())
    }

    fn read(&self, table: &str, id: &types::Value) -> Result<Option<types::Row>, Error> {
        self.txn.get(&Key::Row(table, id).encode())?.map(|v| deserialize(&v)).transpose()
    }

    fn scan(&self, table: &str) -> Result<super::Scan, Error> {
        // FIXME We buffer results here, to avoid dealing with trait lifetimes right now
        Ok(Box::new(
            self.txn
                .scan(&Key::RowStart(table).encode()..&Key::RowEnd(table).encode())?
                .map(|r| r.and_then(|(_, v)| deserialize(&v)))
                .collect::<Vec<Result<_, Error>>>()
                .into_iter(),
        ))
    }

    fn update(&mut self, table: &str, id: &types::Value, row: types::Row) -> Result<(), Error> {
        let table = self
            .read_table(&table)?
            .ok_or_else(|| Error::Value(format!("Table {} does not exist", table)))?;
        // If the primary key changes we do a delete and create, otherwise we replace the row
        if id != &table.row_key(&row)? {
            self.delete(&table.name, id)?;
            self.create(&table.name, row)
        } else {
            table.validate_row(&row, self)?;
            self.txn.set(&Key::Row(&table.name, &id).encode(), serialize(&row)?)
        }
    }

    fn create_table(&mut self, table: &schema::Table) -> Result<(), Error> {
        if self.read_table(&table.name)?.is_some() {
            return Err(Error::Value(format!("Table {} already exists", table.name)));
        }
        table.validate(self)?;
        self.txn.set(&Key::Table(&table.name).encode(), serialize(table)?)
    }

    fn delete_table(&mut self, table: &str) -> Result<(), Error> {
        let table = self
            .read_table(table)?
            .ok_or_else(|| Error::Value(format!("Table {} does not exist", table)))?;
        table.assert_unreferenced(self)?;
        // FIXME Needs to delete all table data as well
        self.txn.delete(&Key::Table(&table.name).encode())
    }

    fn read_table(&self, table: &str) -> Result<Option<schema::Table>, Error> {
        self.txn.get(&Key::Table(table).encode())?.map(|v| deserialize(&v)).transpose()
    }

    fn scan_tables(&self) -> Result<super::TableScan, Error> {
        Ok(Box::new(
            self.txn
                .scan(&Key::TableStart.encode()..&Key::TableEnd.encode())?
                .map(|r| r.and_then(|(_, v)| deserialize(&v)))
                .collect::<Result<Vec<_>, Error>>()?
                .into_iter(),
        ))
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
