//! Runs toyDB workload benchmarks. For example, a read-only
//! workload can be run as:
//!
//! cargo run --bin workload --
//!     --hosts localhost:9605,localhost:9604,localhost:9603
//!     --concurrency 16 --count 100000
//!     read --rows 1000 --size 65536 --batch 10
//!
//! See --help for a list of available workloads and arguments.

#![warn(clippy::all)]

use clap::Parser;
use itertools::Itertools;
use rand::distributions::Distribution;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashSet;
use std::io::Write as _;
use std::time::Duration;
use toydb::error::{Error, Result};
use toydb::{Client, ResultSet};

#[tokio::main]
async fn main() -> Result<()> {
    let Command { runner, subcommand } = Command::parse();
    match subcommand {
        Subcommand::Read(read) => runner.run(read).await,
        Subcommand::Write(write) => runner.run(write).await,
        Subcommand::Bank(bank) => runner.run(bank).await,
    }
}

/// Handles command-line parsing.
#[derive(clap::Parser)]
#[command(about = "Runs toyDB workload benchmarks.", version, propagate_version = true)]
struct Command {
    #[command(flatten)]
    runner: Runner,

    #[command(subcommand)]
    subcommand: Subcommand,
}

#[derive(clap::Subcommand)]
enum Subcommand {
    Read(Read),
    Write(Write),
    Bank(Bank),
}

/// Runs a workload benchmark.
#[derive(clap::Args)]
struct Runner {
    /// Hosts to connect to (optionally with port number).
    #[arg(
        short = 'H',
        long,
        value_delimiter = ',',
        default_value = "localhost:9605,localhost:9604,localhost:9603,localhost:9602,localhost:9601"
    )]
    hosts: Vec<String>,

    /// Number of concurrent workers to spawn.
    #[arg(short, long, default_value = "16")]
    concurrency: usize,

    /// Number of transactions to execute.
    #[arg(short = 'n', long, default_value = "100000")]
    count: usize,

    /// Seed to use for random number generation.
    #[arg(short, long, default_value = "16791084677885396490")]
    seed: u64,
}

impl Runner {
    /// Runs the specified workload.
    async fn run<W: Workload>(self, workload: W) -> Result<()> {
        let mut rng = rand::rngs::StdRng::seed_from_u64(self.seed);
        let mut client = Client::new(&self.hosts[0]).await?;

        // Set up a histogram recording txn latencies as nanoseconds. The
        // buckets range from 0.001s to 10s.
        let mut hist =
            hdrhistogram::Histogram::<u32>::new_with_bounds(1_000, 10_000_000_000, 3)?.into_sync();

        // Prepare the dataset.
        print!("Preparing initial dataset... ");
        std::io::stdout().flush()?;
        let start = std::time::Instant::now();
        workload.prepare(&mut client, &mut rng).await?;
        println!("done ({:.3}s)", start.elapsed().as_secs_f64());

        // Spawn workers, round robin across hosts.
        print!("Spawning {} workers... ", self.concurrency);
        std::io::stdout().flush()?;
        let start = std::time::Instant::now();

        let mut js = tokio::task::JoinSet::<Result<()>>::new();
        let (work_tx, work_rx) = async_channel::bounded(self.concurrency);

        for addr in self.hosts.iter().cycle().take(self.concurrency) {
            let mut client = Client::new(addr).await?;
            let work_rx = work_rx.clone();
            let mut recorder = hist.recorder();
            js.spawn(async move {
                while let Ok(item) = work_rx.recv().await {
                    let start = std::time::Instant::now();
                    Self::execute_with_retry::<W>(&mut client, item).await?;
                    recorder.record(start.elapsed().as_nanos() as u64)?;
                }
                Ok(())
            });
        }

        println!("done ({:.3}s)", start.elapsed().as_secs_f64());

        // Spawn work generator.
        {
            println!("Running workload {}...", workload);
            let generator = workload.generate(rng).take(self.count);
            js.spawn(async move {
                for item in generator {
                    work_tx.send(item).await?;
                }
                work_tx.close();
                Ok(())
            });
        }

        // Wait for workers to complete, and periodically print stats.
        let start = std::time::Instant::now();
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        ticker.tick().await; // skip first tick

        println!();
        println!("Time   Progress     Txns      Rate       p50       p90       p99      pMax");

        let mut print_stats = || {
            let duration = start.elapsed().as_secs_f64();
            hist.refresh_timeout(Duration::from_secs(1));
            println!(
                "{:<8} {:>5.1}%  {:>7}  {:>6.0}/s  {:>6.1}ms  {:>6.1}ms  {:>6.1}ms  {:>6.1}ms",
                format!("{:.1}s", duration),
                hist.len() as f64 / self.count as f64 * 100.0,
                hist.len(),
                hist.len() as f64 / duration,
                Duration::from_nanos(hist.value_at_quantile(0.5)).as_secs_f64() * 1000.0,
                Duration::from_nanos(hist.value_at_quantile(0.9)).as_secs_f64() * 1000.0,
                Duration::from_nanos(hist.value_at_quantile(0.99)).as_secs_f64() * 1000.0,
                Duration::from_nanos(hist.max()).as_secs_f64() * 1000.0,
            );
        };

        loop {
            tokio::select! {
                // Print stats every second.
                _ = ticker.tick() => print_stats(),

                // Check if tasks are done.
                result = js.join_next() => match result {
                    Some(result) => result??,
                    None => break,
                },
            }
        }
        print_stats();
        println!();

        // Verify the final dataset.
        print!("Verifying dataset... ");
        std::io::stdout().flush()?;
        let start = std::time::Instant::now();
        workload.verify(&mut client, self.count).await?;
        println!("done ({:.3}s)", start.elapsed().as_secs_f64());

        Ok(())
    }

    /// Executes a workload item, automatically retrying serialization errors.
    /// Due to async trait/lifetime hassles, this is on the runner rather than
    /// the client or workload trait.
    ///
    /// TODO: move this to a Client.with_txn() helper once async is removed.
    async fn execute_with_retry<W: Workload>(client: &mut Client, item: W::Item) -> Result<()> {
        const MAX_RETRIES: u32 = 10;
        const MIN_WAIT: u64 = 10;
        const MAX_WAIT: u64 = 2_000;

        let mut retries: u32 = 0;
        loop {
            match W::execute(client, &item).await {
                Ok(()) => return Ok(()),
                Err(Error::Serialization | Error::Abort) if retries < MAX_RETRIES => {
                    if client.txn().is_some() {
                        client.execute("ROLLBACK").await?;
                    }

                    // Use exponential backoff starting at MIN_WAIT doubling up
                    // to MAX_WAIT, but randomize the wait time in this interval
                    // to reduce the chance of collisions.
                    let mut wait = std::cmp::min(MIN_WAIT * 2_u64.pow(retries), MAX_WAIT);
                    wait = rand::thread_rng().gen_range(MIN_WAIT..=wait);
                    tokio::time::sleep(std::time::Duration::from_millis(wait)).await;
                    retries += 1;
                }
                Err(e) => {
                    if client.txn().is_some() {
                        client.execute("ROLLBACK").await.ok(); // ignore rollback error
                    }
                    return Err(e);
                }
            }
        }
    }
}

/// A workload.
trait Workload: std::fmt::Display + 'static {
    /// A work item.
    type Item: Send;

    /// Prepares the workload by creating initial tables and data.
    async fn prepare(&self, client: &mut Client, rng: &mut StdRng) -> Result<()>;

    /// Generates work items as an iterator.
    fn generate(&self, rng: StdRng) -> impl Iterator<Item = Self::Item> + Send + 'static;

    /// Executes a single work item. This will automatically be retried on
    /// certain errors, and must use a transaction where appropriate.
    fn execute(
        client: &mut Client,
        item: &Self::Item,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Verifies the dataset after the workload has completed.
    async fn verify(&self, _client: &mut Client, _txns: usize) -> Result<()> {
        Ok(())
    }
}

/// A read-only workload. Creates an id,value table and populates it with the
/// given row count and value size. Then runs batches of random primary key
/// lookups (SELECT * FROM read WHERE id = 1 OR id = 2 ...).
#[derive(clap::Args, Clone)]
#[command(about = "A read-only workload using primary key lookups")]
struct Read {
    /// Total number of rows in data set.
    #[arg(short, long, default_value = "1000")]
    rows: u64,

    /// Row value size (excluding primary key).
    #[arg(short, long, default_value = "64")]
    size: usize,

    /// Number of rows to fetch in a single select.
    #[arg(short, long, default_value = "1")]
    batch: usize,
}

impl std::fmt::Display for Read {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "read (rows={} size={} batch={})", self.rows, self.size, self.batch)
    }
}

impl Workload for Read {
    type Item = HashSet<u64>;

    async fn prepare(&self, client: &mut Client, rng: &mut StdRng) -> Result<()> {
        client.execute("BEGIN").await?;
        client.execute(r#"DROP TABLE IF EXISTS "read""#).await?;
        client
            .execute(r#"CREATE TABLE "read" (id INT PRIMARY KEY, value STRING NOT NULL)"#)
            .await?;

        let chars = &mut rand::distributions::Alphanumeric.sample_iter(rng).map(|b| b as char);
        let rows = (1..=self.rows).map(|id| (id, chars.take(self.size).collect::<String>()));
        let chunks = rows.chunks(100);
        let queries = chunks.into_iter().map(|chunk| {
            format!(
                r#"INSERT INTO "read" (id, value) VALUES ({})"#,
                chunk.map(|(id, value)| format!("{}, '{}'", id, value)).join("), (")
            )
        });
        for query in queries {
            client.execute(&query).await?;
        }
        client.execute("COMMIT").await?;
        Ok(())
    }

    fn generate(&self, rng: StdRng) -> impl Iterator<Item = Self::Item> + 'static {
        ReadGenerator {
            batch: self.batch,
            dist: rand::distributions::Uniform::new(1, self.rows + 1),
            rng,
        }
    }

    async fn execute(client: &mut Client, item: &Self::Item) -> Result<()> {
        let batch_size = item.len();
        let query = format!(
            r#"SELECT * FROM "read" WHERE {}"#,
            item.iter().map(|id| format!("id = {}", id)).join(" OR ")
        );
        let rows = client.execute(&query).await?.into_rows()?;
        assert_eq!(rows.count(), batch_size, "Unexpected row count");
        Ok(())
    }

    async fn verify(&self, client: &mut Client, _: usize) -> Result<()> {
        let count =
            client.execute(r#"SELECT COUNT(*) FROM "read""#).await?.into_value()?.integer()?;
        assert_eq!(count as u64, self.rows, "Unexpected row count");
        Ok(())
    }
}

/// A Read workload generator, yielding batches of random, unique primary keys.
struct ReadGenerator {
    batch: usize,
    rng: StdRng,
    dist: rand::distributions::Uniform<u64>,
}

impl Iterator for ReadGenerator {
    type Item = <Read as Workload>::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let mut ids = HashSet::new();
        for id in self.dist.sample_iter(&mut self.rng) {
            ids.insert(id);
            if ids.len() >= self.batch {
                break;
            }
        }
        Some(ids)
    }
}

/// A write-only workload. Creates an id,value table, and writes rows with
/// sequential primary keys and the given value size, in the given batch size
/// (INSERT INTO write (id, value) VALUES ...). The number of rows written
/// is given by Runner.count * Write.batch.
#[derive(clap::Args, Clone)]
#[command(about = "A write-only workload writing sequential rows")]
struct Write {
    /// Row value size (excluding primary key).
    #[arg(short, long, default_value = "64")]
    size: usize,

    /// Number of rows to write in a single insert query.
    #[arg(short, long, default_value = "1")]
    batch: usize,
}

impl std::fmt::Display for Write {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "write (size={} batch={})", self.size, self.batch)
    }
}

impl Workload for Write {
    type Item = Vec<(u64, String)>;

    async fn prepare(&self, client: &mut Client, _: &mut StdRng) -> Result<()> {
        client.execute("BEGIN").await?;
        client.execute(r#"DROP TABLE IF EXISTS "write""#).await?;
        client
            .execute(r#"CREATE TABLE "write" (id INT PRIMARY KEY, value STRING NOT NULL)"#)
            .await?;
        client.execute("COMMIT").await?;
        Ok(())
    }

    fn generate(&self, rng: StdRng) -> impl Iterator<Item = Self::Item> + 'static {
        WriteGenerator { next_id: 1, size: self.size, batch: self.batch, rng }
    }

    async fn execute(client: &mut Client, item: &Self::Item) -> Result<()> {
        let batch_size = item.len();
        let query = format!(
            r#"INSERT INTO "write" (id, value) VALUES {}"#,
            item.iter().map(|(id, value)| format!("({}, '{}')", id, value)).join(", ")
        );
        if let ResultSet::Create { count } = client.execute(&query).await? {
            assert_eq!(count as usize, batch_size, "Unexpected row count");
        } else {
            panic!("Unexpected result")
        }
        Ok(())
    }

    async fn verify(&self, client: &mut Client, txns: usize) -> Result<()> {
        let count =
            client.execute(r#"SELECT COUNT(*) FROM "write""#).await?.into_value()?.integer()?;
        assert_eq!(count as usize, txns * self.batch, "Unexpected row count");
        Ok(())
    }
}

/// A Write workload generator, yielding batches of sequential primary keys and
/// random rows.
struct WriteGenerator {
    next_id: u64,
    size: usize,
    batch: usize,
    rng: StdRng,
}

impl Iterator for WriteGenerator {
    type Item = <Write as Workload>::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let chars =
            &mut rand::distributions::Alphanumeric.sample_iter(&mut self.rng).map(|b| b as char);
        let mut rows = Vec::with_capacity(self.batch);
        while rows.len() < self.batch {
            rows.push((self.next_id, chars.take(self.size).collect()));
            self.next_id += 1;
        }
        Some(rows)
    }
}

/// A bank workload. Creates a set of customers and accounts, and makes random
/// transfers between them. Specifically, it picks two random customers A and B,
/// and then finds A's highest-balance account and B's lowest-balance account,
/// and transfers a random amount without overdrawing the account. This
/// somewhat convoluted scheme is used to make the workload slightly less
/// trivial, including joins, ordering, and secondary indexes.
#[derive(clap::Args, Clone)]
#[command(about = "A bank workload, making transfers between customer accounts")]
struct Bank {
    /// Number of customers.
    #[arg(short, long, default_value = "100")]
    customers: u64,

    /// Number of accounts per customer.
    #[arg(short, long, default_value = "10")]
    accounts: u64,

    /// Initial account balance.
    #[arg(short, long, default_value = "100")]
    balance: u64,

    /// Max amount to transfer.
    #[arg(short, long, default_value = "50")]
    max_transfer: u64,
}

impl std::fmt::Display for Bank {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bank (customers={} accounts={})", self.customers, self.accounts)
    }
}

impl Workload for Bank {
    type Item = (u64, u64, u64); // from,to,amount

    async fn prepare(&self, client: &mut Client, rng: &mut StdRng) -> Result<()> {
        let petnames = petname::Petnames::default();
        client.execute("BEGIN").await?;
        client.execute("DROP TABLE IF EXISTS account").await?;
        client.execute("DROP TABLE IF EXISTS customer").await?;
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
                    .zip(petnames.iter(rng, 3, " "))
                    .map(|(id, name)| format!("({}, '{}')", id, name))
                    .join(", ")
            ))
            .await?;
        client
            .execute(&format!(
                "INSERT INTO account VALUES {}",
                (1..=self.customers)
                    .flat_map(|c| (1..=self.accounts).map(move |a| (c, (c-1)*self.accounts + a)))
                    .map(|(c, a)| (format!("({}, {}, {})", a, c, self.balance)))
                    .join(", ")
            ))
            .await?;
        client.execute("COMMIT").await?;
        Ok(())
    }

    fn generate(&self, rng: StdRng) -> impl Iterator<Item = Self::Item> + 'static {
        let customers = self.customers;
        let max_transfer = self.max_transfer;
        // Generate random u64s, then pick random from,to,amount as the
        // remainder of the max customer and amount.
        rand::distributions::Uniform::new_inclusive(0, u64::MAX)
            .sample_iter(rng)
            .tuples()
            .map(move |(a, b, c)| (a % customers + 1, b % customers + 1, c % max_transfer + 1))
            .filter(|(from, to, _)| from != to)
    }

    async fn execute(client: &mut Client, item: &Self::Item) -> Result<()> {
        let (from, to, mut amount) = item;

        client.execute("BEGIN").await?;

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
        let from_balance = row.pop().unwrap().integer()?;
        let from_account = row.pop().unwrap().integer()?;
        amount = std::cmp::min(amount, from_balance as u64);

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

        client.execute("COMMIT").await?;

        Ok(())
    }

    async fn verify(&self, client: &mut Client, _: usize) -> Result<()> {
        let balance =
            client.execute("SELECT SUM(balance) FROM account").await?.into_value()?.integer()?;
        assert_eq!(balance as u64, self.customers * self.accounts * self.balance);
        let negative = client
            .execute("SELECT COUNT(*) FROM account WHERE balance < 0")
            .await?
            .into_value()?
            .integer()?;
        assert_eq!(negative, 0);
        Ok(())
    }
}
