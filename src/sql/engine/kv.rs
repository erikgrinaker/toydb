use super::super::schema::Table;
use super::super::types::{Row, Value};
use crate::kv;
use crate::utility::{deserialize, serialize};
use crate::Error;

/// A SQL engine based on an underlying MVCC key/value store
pub struct KV<S: kv::storage::Storage> {
    /// The underlying key/value store
    kv: kv::MVCC<S>,
}

// FIXME Implement Clone manually due to https://github.com/rust-lang/rust/issues/26925
impl<S: kv::storage::Storage> std::clone::Clone for KV<S> {
    fn clone(&self) -> Self {
        KV::new(self.kv.clone())
    }
}

impl<S: kv::storage::Storage> KV<S> {
    /// Creates a new key/value-based SQL engine
    pub fn new(kv: kv::MVCC<S>) -> Self {
        Self { kv }
    }
}

impl<S: kv::storage::Storage> super::Engine for KV<S> {
    type Transaction = Transaction<S>;

    fn begin_with_mode(&self, mode: super::Mode) -> Result<Self::Transaction, Error> {
        Ok(Self::Transaction::new(self.kv.begin_with_mode(mode)?))
    }

    fn resume(&self, id: u64) -> Result<Self::Transaction, Error> {
        Ok(Self::Transaction::new(self.kv.resume(id)?))
    }
}

/// An SQL transaction based on an MVCC key/value transaction
pub struct Transaction<S: kv::storage::Storage> {
    txn: kv::Transaction<S>,
}

impl<S: kv::storage::Storage> Transaction<S> {
    /// Creates a new SQL transaction from an MVCC transaction
    fn new(txn: kv::Transaction<S>) -> Self {
        Self { txn }
    }
}

impl<S: kv::storage::Storage> super::Transaction for Transaction<S> {
    fn id(&self) -> u64 {
        self.txn.id()
    }

    fn mode(&self) -> super::Mode {
        self.txn.mode()
    }

    fn commit(self) -> Result<(), Error> {
        self.txn.commit()
    }

    fn rollback(self) -> Result<(), Error> {
        self.txn.rollback()
    }

    fn create(&mut self, table: &str, row: Row) -> Result<(), Error> {
        let table = self.must_read_table(&table)?;
        table.validate_row(&row, self)?;
        let id = table.get_row_key(&row)?;
        if self.read(&table.name, &id)?.is_some() {
            return Err(Error::Value(format!(
                "Primary key {} already exists for table {}",
                id, table.name
            )));
        }
        self.txn.set(&Key::Row(&table.name, &id).encode(), serialize(&row)?)
    }

    fn delete(&mut self, table: &str, id: &Value) -> Result<(), Error> {
        let table = self.must_read_table(&table)?;
        table.assert_unreferenced_key(id, self)?;
        self.txn.delete(&Key::Row(&table.name, id).encode())
    }

    fn read(&self, table: &str, id: &Value) -> Result<Option<Row>, Error> {
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

    fn update(&mut self, table: &str, id: &Value, row: Row) -> Result<(), Error> {
        let table = self.must_read_table(&table)?;
        // If the primary key changes we do a delete and create, otherwise we replace the row
        if id != &table.get_row_key(&row)? {
            self.delete(&table.name, id)?;
            self.create(&table.name, row)
        } else {
            table.validate_row(&row, self)?;
            self.txn.set(&Key::Row(&table.name, &id).encode(), serialize(&row)?)
        }
    }

    fn create_table(&mut self, table: &Table) -> Result<(), Error> {
        if self.read_table(&table.name)?.is_some() {
            return Err(Error::Value(format!("Table {} already exists", table.name)));
        }
        table.validate(self)?;
        self.txn.set(&Key::Table(&table.name).encode(), serialize(table)?)
    }

    fn delete_table(&mut self, table: &str) -> Result<(), Error> {
        let table = self.must_read_table(&table)?;
        table.assert_unreferenced(self)?;
        let mut scan = self.scan(&table.name)?;
        while let Some(row) = scan.next().transpose()? {
            self.delete(&table.name, &table.get_row_key(&row)?)?
        }
        self.txn.delete(&Key::Table(&table.name).encode())
    }

    fn read_table(&self, table: &str) -> Result<Option<Table>, Error> {
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

/// Encodes tables and rows as MVCC key/value keys
enum Key<'a> {
    /// The start of the table schema keyspace
    TableStart,
    /// A table schema key for the given table name
    Table(&'a str),
    /// The end of the table schema keyspace
    TableEnd,
    /// The start of the row keyspace of the given table name
    RowStart(&'a str),
    /// A key for a row identified by table name and row primary key
    Row(&'a str, &'a Value),
    /// The end of the row keyspace of the given table name
    RowEnd(&'a str),
}

impl<'a> Key<'a> {
    /// Encodes the key as a byte vector
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
                    Value::Boolean(b) if *b => vec![0x01],
                    Value::Boolean(_) => vec![0x00],
                    Value::Float(f) => f.to_be_bytes().to_vec(),
                    Value::Integer(i) => i.to_be_bytes().to_vec(),
                    Value::String(s) => s.as_bytes().to_vec(),
                    Value::Null => panic!("Received NULL primary key"),
                },
            ]
            .concat(),
            Self::RowEnd(table) => [vec![0x03], table.as_bytes().to_vec(), vec![0xff]].concat(),
        }
    }
}
