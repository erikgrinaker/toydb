/*
 * Simulates a bank, by setting a set of accounts and making transfers between them. Records the
 * transaction throughput and checks invariants.
 */

#![warn(clippy::all)]

use clap::{app_from_crate, crate_authors, crate_description, crate_name, crate_version};
use futures::stream::TryStreamExt as _;
use rand::distributions::Distribution;
use rand::Rng as _;
use tokio::net::ToSocketAddrs;
use tokio::sync::MutexGuard;
use toydb::client::{Client, Pool};
use toydb::Error;

#[tokio::main]
async fn main() -> Result<(), Error> {
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
    const TXN_RETRIES: u64 = 8;

    // Creates a new bank simulation
    async fn new<A: ToSocketAddrs + Clone>(
        addrs: Vec<A>,
        concurrency: u64,
        customers: i64,
        accounts: i64,
    ) -> Result<Self, Error> {
        Ok(Self {
            clients: Pool::new(addrs, concurrency).await?,
            customers,
            customer_accounts: accounts,
        })
    }

    // Runs the bank simulation
    async fn run(&self, transactions: u64) -> Result<(), Error> {
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
    async fn setup(&self) -> Result<(), Error> {
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
    async fn verify(&self) -> Result<(), Error> {
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

    /// Transfers a random amount between two customers, retrying serialization failures
    /// FIXME The serialization rety with exponential backoff should be a helper on Client
    async fn transfer(&self, from: i64, to: i64) -> Result<(), Error> {
        let client = self.clients.get().await;
        let mut attempts = 0;
        let start = std::time::Instant::now();

        for i in 0..Self::TXN_RETRIES {
            attempts += 1;
            if i > 0 {
                tokio::time::delay_for(std::time::Duration::from_millis(
                    2_u64.pow(i as u32 - 1) * rand::thread_rng().gen_range(75, 125),
                ))
                .await;
            }
            let result = async {
                client.execute("BEGIN").await?;
                let result = self.try_transfer(&client, from, to).await?;
                client.execute("COMMIT").await?;
                Ok(result)
            }
            .await;
            if result.is_err() {
                client.execute("ROLLBACK").await?;
                if let Err(Error::Serialization) = result {
                    continue;
                }
            }
            let (from_account, to_account, amount) = result?;
            println!(
                "Thread {} transferred {: >4} from {: >3} ({:0>4}) to {: >3} ({:0>4}) in {:.3}s ({} attempts)",
                client.id(),
                amount,
                from,
                from_account,
                to,
                to_account,
                start.elapsed().as_secs_f64(),
                attempts
            );
            return Ok(());
        }
        Err(Error::Serialization)
    }

    /// Attempts to transfer a random amount
    async fn try_transfer(
        &self,
        client: &MutexGuard<'_, Client>,
        from: i64,
        to: i64,
    ) -> Result<(i64, i64, i64), Error> {
        let mut row = client
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

        let to_account = client
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
        client
            .execute(&format!(
                "UPDATE account SET balance = balance - {} WHERE id = {}",
                amount, from_account,
            ))
            .await?;
        client
            .execute(&format!(
                "UPDATE account SET balance = balance + {} WHERE id = {}",
                amount, to_account,
            ))
            .await?;
        Ok((from_account, to_account, amount))
    }
}
