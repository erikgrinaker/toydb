/*
 * Simulates a bank, by setting a set of accounts and making transfers between them. Records the
 * transaction throughput and checks invariants.
 */

#![warn(clippy::all)]

use clap::{app_from_crate, crate_authors, crate_description, crate_name, crate_version};
use futures::stream::TryStreamExt as _;
use rand::distributions::Distribution;
use rand::Rng as _;
use std::cell::Cell;
use std::rc::Rc;
use tokio::net::ToSocketAddrs;
use toydb::client::Pool;
use toydb::error::{Error, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let opts = app_from_crate!()
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
        opts.value_of("concurrency").unwrap().parse()?,
        opts.value_of("customers").unwrap().parse()?,
        opts.value_of("accounts").unwrap().parse()?,
    )
    .await?
    .run(opts.value_of("transactions").unwrap().parse()?)
    .await
}

struct Bank {
    clients: Pool,
    customers: i64,
    customer_accounts: i64,
}

impl Bank {
    const INITIAL_BALANCE: u64 = 100;

    // Creates a new bank simulation
    async fn new<A: ToSocketAddrs + Clone>(
        addrs: Vec<A>,
        concurrency: u64,
        customers: i64,
        accounts: i64,
    ) -> Result<Self> {
        Ok(Self {
            clients: Pool::new(addrs, concurrency).await?,
            customers,
            customer_accounts: accounts,
        })
    }

    // Runs the bank simulation
    async fn run(&self, transactions: u64) -> Result<()> {
        self.setup().await?;
        self.verify().await?;
        println!();

        let mut rng = rand::thread_rng();
        let custs = rand::distributions::Uniform::from(1..=self.customers as i64);
        let transfers = futures::stream::iter(
            std::iter::from_fn(|| Some((custs.sample(&mut rng), custs.sample(&mut rng))))
                .filter(|(from, to)| from != to)
                .map(Ok)
                .take(transactions as usize),
        );

        let start = std::time::Instant::now();
        transfers
            .try_for_each_concurrent(self.clients.size(), |(from, to)| self.transfer(from, to))
            .await?;
        let elapsed = start.elapsed().as_secs_f64();

        println!();
        println!(
            "Ran {} transactions in {:.3}s ({:.3}/s)",
            transactions,
            elapsed,
            transactions as f64 / elapsed
        );

        self.verify().await?;
        Ok(())
    }

    // Sets up the database
    async fn setup(&self) -> Result<()> {
        let client = self.clients.get().await;
        let start = std::time::Instant::now();
        client.execute("BEGIN").await?;
        client
            .execute(
                "CREATE TABLE customer (
                    id INTEGER PRIMARY KEY,
                    name STRING NOT NULL
                )",
            )
            .await?;
        client
            .execute(
                "CREATE TABLE account (
                    id INTEGER PRIMARY KEY,
                    customer_id INTEGER NOT NULL INDEX REFERENCES customer,
                    balance INTEGER NOT NULL
                )",
            )
            .await?;
        client
            .execute(&format!(
                "INSERT INTO customer VALUES {}",
                (1..=self.customers)
                    .zip(names::Generator::with_naming(names::Name::Plain))
                    .map(|(id, name)| format!("({}, '{}')", id, name))
                    .collect::<Vec<String>>()
                    .join(", ")
            ))
            .await?;
        client
            .execute(&format!(
                "INSERT INTO account VALUES {}",
                (1..=self.customers)
                    .flat_map(|c| (1..=self.customer_accounts).map(move |a| (c, a)))
                    .map(|(c, a)| (c, (c - 1) * self.customer_accounts + a))
                    .map(|(c, a)| (format!("({}, {}, {})", a, c, Self::INITIAL_BALANCE)))
                    .collect::<Vec<String>>()
                    .join(", ")
            ))
            .await?;
        client.execute("COMMIT").await?;

        println!(
            "Created {} customers ({} accounts) in {:.3}s",
            self.customers,
            self.customers * self.customer_accounts,
            start.elapsed().as_secs_f64()
        );
        Ok(())
    }

    /// Verifies that all invariants hold
    async fn verify(&self) -> Result<()> {
        let client = self.clients.get().await;
        let expect = self.customers * self.customer_accounts * Self::INITIAL_BALANCE as i64;
        let balance =
            client.execute("SELECT SUM(balance) FROM account").await?.into_value()?.integer()?;
        if balance != expect {
            return Err(Error::Value(format!(
                "Expected total balance {}, found {}",
                expect, balance
            )));
        }
        let negative = client
            .execute("SELECT COUNT(*) FROM account WHERE balance < 0")
            .await?
            .into_value()?
            .integer()?;
        if negative > 0 {
            return Err(Error::Value(format!("Found {} accounts with negative balance", negative)));
        }
        println!("Verified that total balance is {} with no negative balances", balance);
        Ok(())
    }

    /// Transfers a random amount between two customers.
    async fn transfer(&self, from: i64, to: i64) -> Result<()> {
        let client = self.clients.get().await;
        let attempts = Rc::new(Cell::new(0_u8));
        let start = std::time::Instant::now();

        let (from_account, to_account, amount) = client
            .with_txn(|txn| {
                let attempts = attempts.clone();
                async move {
                    attempts.set(attempts.get() + 1);
                    let mut row = txn
                        .execute(&format!(
                            "SELECT a.id, a.balance
                            FROM account a JOIN customer c ON a.customer_id = c.id
                            WHERE c.id = {}
                            ORDER BY a.balance DESC
                            LIMIT 1",
                            from
                        ))
                        .await?
                        .into_row()?;
                    let from_account = row.remove(0).integer()?;
                    let from_balance = row.remove(0).integer()?;

                    let to_account = txn
                        .execute(&format!(
                            "SELECT a.id, a.balance
                            FROM account a JOIN customer c ON a.customer_id = c.id
                            WHERE c.id = {}
                            ORDER BY a.balance ASC
                            LIMIT 1",
                            to
                        ))
                        .await?
                        .into_value()?
                        .integer()?;

                    let amount = rand::thread_rng().gen_range(0, from_balance);
                    txn.execute(&format!(
                        "UPDATE account SET balance = balance - {} WHERE id = {}",
                        amount, from_account,
                    ))
                    .await?;
                    txn.execute(&format!(
                        "UPDATE account SET balance = balance + {} WHERE id = {}",
                        amount, to_account,
                    ))
                    .await?;
                    Ok((from_account, to_account, amount))
                }
            })
            .await?;

        println!(
            "Thread {} transferred {: >4} from {: >3} ({:0>4}) to {: >3} ({:0>4}) in {:.3}s ({} attempts)",
            client.id(),
            amount,
            from,
            from_account,
            to,
            to_account,
            start.elapsed().as_secs_f64(),
            attempts.get());
        Ok(())
    }
}
