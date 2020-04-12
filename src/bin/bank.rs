/*
 * Simulates a bank, by setting a set of accounts and making transfers between them. Records the
 * transaction throughput and checks invariants.
 */

#![warn(clippy::all)]

#[macro_use]
extern crate clap;
extern crate crossbeam;
extern crate names;
extern crate rand;
extern crate toydb;

use rand::distributions::Distribution;
use rand::prelude::*;
use toydb::client::Client;
use toydb::sql::execution::ResultSet;
use toydb::sql::types::Value;
use toydb::Error;

fn main() -> Result<(), Error> {
    let opts = app_from_crate!()
        .arg(clap::Arg::with_name("command"))
        .arg(clap::Arg::with_name("headers").short("H").long("headers").help("Show column headers"))
        .arg(
            clap::Arg::with_name("host")
                .short("h")
                .long("host")
                .help("Host to connect to, optionally with port number")
                .takes_value(true)
                .number_of_values(1)
                .multiple(true)
                .required(true)
                .default_value("127.0.0.1:9605"),
        )
        .arg(
            clap::Arg::with_name("concurrency")
                .short("c")
                .long("concurrency")
                .help("Concurrent workers to spawn")
                .takes_value(true)
                .required(true)
                .default_value("4"),
        )
        .arg(
            clap::Arg::with_name("customers")
                .short("C")
                .long("customers")
                .help("Number of customers to create")
                .takes_value(true)
                .required(true)
                .default_value("10"),
        )
        .arg(
            clap::Arg::with_name("accounts")
                .short("a")
                .long("accounts")
                .help("Number of accounts to create per customer")
                .takes_value(true)
                .required(true)
                .default_value("10"),
        )
        .arg(
            clap::Arg::with_name("transactions")
                .short("t")
                .long("transactions")
                .help("Number of account transfers to execute")
                .takes_value(true)
                .required(true)
                .default_value("20"),
        )
        .get_matches();

    Bank::new(
        opts.values_of("host").unwrap().map(String::from).collect(),
        opts.value_of("customers").unwrap().parse()?,
        opts.value_of("accounts").unwrap().parse()?,
    )?
    .run(
        opts.value_of("concurrency").unwrap().parse()?,
        opts.value_of("transactions").unwrap().parse()?,
    )
}

struct Bank {
    hosts: Vec<(String, u16)>,
    customers: i64,
    customer_accounts: i64,
    initial_balance: i64,
}

impl Bank {
    // Creates a new bank simulation
    fn new(hosts: Vec<String>, customers: i64, accounts: i64) -> Result<Self, Error> {
        let mut h = Vec::new();
        for host in hosts {
            let parts: Vec<&str> = host.split(":").collect();
            h.push((parts[0].to_string(), if parts.len() >= 2 { parts[1].parse()? } else { 9605 }));
        }
        Ok(Self { hosts: h, customers, customer_accounts: accounts, initial_balance: 100 })
    }

    // Runs the bank simulation
    fn run(&self, concurrency: i64, transactions: i64) -> Result<(), Error> {
        println!(
            "Using {} threads with {}",
            concurrency,
            self.hosts
                .iter()
                .map(|(h, p)| format!("{}:{}", h, p))
                .collect::<Vec<String>>()
                .join(","),
        );

        let mut hosts = self.hosts.iter().cycle();
        let client = &mut {
            let (host, port) = hosts.next().unwrap();
            Client::new(host, *port)?
        };

        self.setup(client)?;
        self.check(client)?;

        let start = std::time::Instant::now();

        crossbeam::thread::scope(|scope| {
            let (tx, rx) = crossbeam::channel::unbounded();
            scope.spawn(move |_| {
                let dist = rand::distributions::Uniform::from(0..self.customers);
                let mut rng = rand::thread_rng();
                for _ in 0..transactions {
                    let from = dist.sample(&mut rng);
                    let mut to = dist.sample(&mut rng);
                    while from == to {
                        to = dist.sample(&mut rng)
                    }
                    tx.send((from, to)).unwrap();
                }
                std::mem::drop(tx)
            });

            println!();
            for i in 0..concurrency {
                let r = rx.clone();
                let (host, port) = hosts.next().unwrap();
                let mut client = Client::new(host, *port).unwrap();
                scope.spawn(move |_| self.process(&mut client, i, r).unwrap());
            }
        })
        .unwrap();

        let elapsed = start.elapsed().as_secs_f64();
        println!();
        println!(
            "Ran {} transactions in {:.3}s ({:.3}/s)",
            transactions,
            elapsed,
            transactions as f64 / elapsed
        );

        self.check(client)?;
        Ok(())
    }

    // Processes transactions for a customer
    fn process(
        &self,
        client: &mut Client,
        tid: i64,
        rx: crossbeam::channel::Receiver<(i64, i64)>,
    ) -> Result<(), Error> {
        let mut rng = rand::thread_rng();
        for (from, to) in rx {
            let mut amount = 0;
            let mut from_account = 0;
            let mut to_account = 0;
            let mut attempts = 0;
            let start = std::time::Instant::now();

            let result = client.with_txn(|t| {
                attempts += 1;
                let f = get_integers(t.query(&format!(
                    "SELECT a.id, a.balance
                    FROM account a JOIN customer c ON a.customer_id = c.id
                    WHERE c.id = {}
                    ORDER BY a.balance DESC
                    LIMIT 1",
                    from
                ))?)?;
                from_account = f[0];
                let from_balance = f[1];
                to_account = get_integers(t.query(&format!(
                    "SELECT a.id, a.balance
                    FROM account a JOIN customer c ON a.customer_id = c.id
                    WHERE c.id = {}
                    ORDER BY a.balance ASC
                    LIMIT 1",
                    to
                ))?)?[0];

                amount = rng.gen_range(0, from_balance);

                t.query(&format!(
                    "UPDATE account SET balance = balance - {} WHERE id = {}",
                    amount, from_account,
                ))?;
                t.query(&format!(
                    "UPDATE account SET balance = balance + {} WHERE id = {}",
                    amount, to_account,
                ))?;
                Ok(())
            });
            match result {
                Err(Error::Serialization) => println!(
                    "Thread {} serialization failure for {} to {}, giving up after {:.3}s ({} attempts)",
                    tid,
                    from,
                    to,
                    start.elapsed().as_secs_f64(),
                    attempts
                ),
                Ok(_) => println!(
                    "Thread {} transferred {: >4} from {: >3} ({:0>4}) to {: >3} ({:0>4}) in {:.3}s ({} attempts)",
                    tid,
                    amount,
                    from,
                    from_account,
                    to,
                    to_account,
                    start.elapsed().as_secs_f64(),
                    attempts
                ),
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    // Sets up the database
    fn setup(&self, client: &mut Client) -> Result<(), Error> {
        let mut namegen = names::Generator::default(names::Name::Plain);
        let now = std::time::Instant::now();

        client.query("BEGIN")?;
        client.query(
            "CREATE TABLE customer (
                id INTEGER PRIMARY KEY,
                name STRING NOT NULL
            )",
        )?;
        client.query(
            "CREATE TABLE account (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL REFERENCES customer,
                balance INTEGER NOT NULL
           )",
        )?;

        for i in 0..self.customers {
            client.query(&format!(
                "INSERT INTO customer VALUES ({}, '{}')",
                i,
                namegen.next().unwrap()
            ))?;
            client.query(&format!(
                "INSERT INTO account VALUES {}",
                (0..self.customer_accounts)
                    .map(|j| i * self.customer_accounts + j)
                    .map(|id| (format!("({}, {}, {})", id, i, self.initial_balance)))
                    .collect::<Vec<String>>()
                    .join(", ")
            ))?;
        }
        client.query("COMMIT")?;

        println!(
            "Created {} customers ({} accounts) in {:.3}s",
            self.customers,
            self.customers * self.customer_accounts,
            now.elapsed().as_secs_f64()
        );
        Ok(())
    }

    /// Checks that all invariants hold
    fn check(&self, client: &mut Client) -> Result<(), Error> {
        let expect_balance = self.customers * self.customer_accounts * self.initial_balance;
        let balance = get_integers(client.query("SELECT SUM(balance) FROM account")?)?[0];
        if balance != expect_balance {
            return Err(Error::Value(format!(
                "Invariant violation: expected total balance {}, found {}",
                expect_balance, balance
            )));
        }
        let neg = get_integers(client.query("SELECT COUNT(*) FROM account WHERE balance < 0")?)?[0];
        if neg > 0 {
            return Err(Error::Value(format!(
                "Invariant violation: expected 0 accounts with negative balance, got {}",
                neg
            )));
        }
        println!("Checked that total balance is {} with no negative balances", balance);
        Ok(())
    }
}

// FIXME This should be a row or result method
fn get_integers(result: ResultSet) -> Result<Vec<i64>, Error> {
    if let ResultSet::Query { mut relation } = result {
        if let Some(row) = relation.next().transpose()? {
            return row
                .into_iter()
                .map(|f| match f {
                    Value::Integer(i) => Ok(i),
                    _ => Err(Error::Internal("no value received".into())),
                })
                .collect();
        }
    }
    Err(Error::Internal("no row received".into()))
}
