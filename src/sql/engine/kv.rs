use super::super::schema;
use super::super::types;
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
        table.validate_row(&row)?;
        let pk = row.get(table.primary_key).unwrap();
        if self.read(&table.name, &pk)?.is_some() {
            return Err(Error::Value(format!(
                "Primary key {} already exists for table {}",
                pk, table.name
            )));
        }
        self.txn.set(&Key::Row(&table.name, &pk).encode(), serialize(row)?)?;
        Ok(())
    }

    fn delete(&mut self, table: &str, id: &types::Value) -> Result<(), Error> {
        self.txn.delete(&Key::Row(table, id).encode())
    }

    fn read(&self, table: &str, id: &types::Value) -> Result<Option<types::Row>, Error> {
        self.txn.get(&Key::Row(table, id).encode())?.map(deserialize).transpose()
    }

    fn scan(&self, table: &str) -> Result<super::Scan, Error> {
        let from = Key::RowStart(table).encode();
        let to = Key::RowEnd(table).encode();
        Ok(Box::new(self.txn.scan(&from..&to)?.map(|res| match res {
            Ok((_, v)) => deserialize(v),
            Err(err) => Err(err),
        })))
    }

    fn update(&mut self, table: &str, id: &types::Value, row: types::Row) -> Result<(), Error> {
        // FIXME For now, we're lazy and just do a delete + create
        self.delete(table, id)?;
        self.create(table, row)
    }

    fn create_table(&mut self, table: &schema::Table) -> Result<(), Error> {
        if self.read_table(&table.name)?.is_some() {
            return Err(Error::Value(format!("Table {} already exists", table.name)));
        }
        self.txn.set(&Key::Table(&table.name).encode(), serialize(table)?)
    }

    fn delete_table(&mut self, table: &str) -> Result<(), Error> {
        if self.read_table(table)?.is_none() {
            return Err(Error::Value(format!("Table {} does not exist", table)));
        }
        // FIXME Needs to delete all table data as well
        self.txn.delete(&Key::Table(table).encode())
    }

    fn list_tables(&self) -> Result<Vec<schema::Table>, Error> {
        self.txn
            .scan(&Key::TableStart.encode()..&Key::TableEnd.encode())?
            .collect::<Result<Vec<_>, Error>>()?
            .into_iter()
            .map(|(_, v)| deserialize(v))
            .collect()
    }

    fn read_table(&self, table: &str) -> Result<Option<schema::Table>, Error> {
        self.txn.get(&Key::Table(table).encode())?.map(deserialize).transpose()
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
                    types::Value::Integer(i) => i.to_be_bytes().to_vec(),
                    _ => unimplemented!(),
                },
            ]
            .concat(),
            Self::RowEnd(table) => [vec![0x03], table.as_bytes().to_vec(), vec![0xff]].concat(),
        }
    }
}
