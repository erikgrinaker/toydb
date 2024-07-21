//! Runs toyDB workload benchmarks. By default, it assumes a running 5-node
//! cluster as launched via cluster/run.sh, but this can be modified via -H.
//! For example, a read-only workload can be run as:
//!
//! cargo run --release --bin workload -- read
//!
//! See --help for a list of available workloads and arguments.

#![warn(clippy::all)]

use hdrhistogram::Histogram;
use toydb::error::Result;
use toydb::sql::types::{Row, Rows};
use toydb::{Client, StatementResult};

use clap::Parser;
use itertools::Itertools as _;
use petname::Generator as _;
use rand::distributions::Distribution as _;
use rand::rngs::StdRng;
use rand::SeedableRng as _;
use std::collections::HashSet;
use std::io::Write as _;
use std::time::Duration;

fn main() {
    let Command { runner, subcommand } = Command::parse();
    let result = match subcommand {
        Subcommand::Read(read) => runner.run(read),
        Subcommand::Write(write) => runner.run(write),
        Subcommand::Bank(bank) => runner.run(bank),
    };
    if let Err(error) = result {
        eprintln!("Error: {error}")
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
    fn run<W: Workload>(self, workload: W) -> Result<()> {
        let mut rng = StdRng::seed_from_u64(self.seed);
        let mut client = Client::connect(&self.hosts[0])?;

        // Set up a histogram recording txn latencies as nanoseconds. The
        // buckets range from 0.001s to 10s.
        let mut hist = Histogram::<u32>::new_with_bounds(1_000, 10_000_000_000, 3)?.into_sync();

        // Prepare the dataset.
        print!("Preparing initial dataset... ");
        std::io::stdout().flush()?;
        let start = std::time::Instant::now();
        workload.prepare(&mut client, &mut rng)?;
        println!("done ({:.3}s)", start.elapsed().as_secs_f64());

        // Spawn workers, round robin across hosts.
        std::thread::scope(|s| -> Result<()> {
            print!("Spawning {} workers... ", self.concurrency);
            std::io::stdout().flush()?;
            let start = std::time::Instant::now();

            let (work_tx, work_rx) = crossbeam::channel::bounded(self.concurrency);
            let (done_tx, done_rx) = crossbeam::channel::bounded::<()>(0);

            for addr in self.hosts.iter().cycle().take(self.concurrency) {
                let mut client = Client::connect(addr)?;
                let mut recorder = hist.recorder();
                let work_rx = work_rx.clone();
                let done_tx = done_tx.clone();
                s.spawn(move || -> Result<()> {
                    while let Ok(item) = work_rx.recv() {
                        let start = std::time::Instant::now();
                        client.with_retry(|client| W::execute(client, &item))?;
                        recorder.record(start.elapsed().as_nanos() as u64)?;
                    }
                    drop(done_tx); // disconnects done_rx once all workers exit
                    Ok(())
                });
            }
            drop(done_tx); // drop local copy

            println!("done ({:.3}s)", start.elapsed().as_secs_f64());

            // Spawn work generator.
            {
                println!("Running workload {}...", workload);
                let generator = workload.generate(rng).take(self.count);
                s.spawn(move || -> Result<()> {
                    for item in generator {
                        work_tx.send(item)?;
                    }
                    Ok(())
                });
            }

            // Periodically print stats until all workers are done.
            let start = std::time::Instant::now();
            let ticker = crossbeam::channel::tick(Duration::from_secs(1));

            println!();
            println!("Time   Progress     Txns      Rate       p50       p90       p99      pMax");

            while let Err(crossbeam::channel::TryRecvError::Empty) = done_rx.try_recv() {
                crossbeam::select! {
                    recv(ticker) -> _ => {},
                    recv(done_rx) -> _ => {},
                }

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
            }
            Ok(())
        })?;

        // Verify the final dataset.
        println!();
        print!("Verifying dataset... ");
        std::io::stdout().flush()?;
        let start = std::time::Instant::now();
        workload.verify(&mut client, self.count)?;
        println!("done ({:.3}s)", start.elapsed().as_secs_f64());

        Ok(())
    }
}

/// A workload.
trait Workload: std::fmt::Display {
    /// A work item.
    type Item: Send;

    /// Prepares the workload by creating initial tables and data.
    fn prepare(&self, client: &mut Client, rng: &mut StdRng) -> Result<()>;

    /// Generates work items as an iterator.
    fn generate(&self, rng: StdRng) -> impl Iterator<Item = Self::Item> + Send + 'static;

    /// Executes a single work item. This will automatically be retried on
    /// certain errors, and must use a transaction where appropriate.
    fn execute(client: &mut Client, item: &Self::Item) -> Result<()>;

    /// Verifies the dataset after the workload has completed.
    fn verify(&self, _client: &mut Client, _txns: usize) -> Result<()> {
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

    fn prepare(&self, client: &mut Client, rng: &mut StdRng) -> Result<()> {
        client.execute("BEGIN")?;
        client.execute(r#"DROP TABLE IF EXISTS "read""#)?;
        client.execute(r#"CREATE TABLE "read" (id INT PRIMARY KEY, value STRING NOT NULL)"#)?;

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
            client.execute(&query)?;
        }
        client.execute("COMMIT")?;
        Ok(())
    }

    fn generate(&self, rng: StdRng) -> impl Iterator<Item = Self::Item> + 'static {
        ReadGenerator {
            batch: self.batch,
            dist: rand::distributions::Uniform::new(1, self.rows + 1),
            rng,
        }
    }

    fn execute(client: &mut Client, item: &Self::Item) -> Result<()> {
        let batch_size = item.len();
        let query = format!(
            r#"SELECT * FROM "read" WHERE {}"#,
            item.iter().map(|id| format!("id = {}", id)).join(" OR ")
        );
        let rows: Rows = client.execute(&query)?.try_into()?;
        assert_eq!(rows.count(), batch_size, "Unexpected row count");
        Ok(())
    }

    fn verify(&self, client: &mut Client, _: usize) -> Result<()> {
        let count: i64 = client.execute(r#"SELECT COUNT(*) FROM "read""#)?.try_into()?;
        assert_eq!(count, self.rows as i64, "Unexpected row count");
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

    fn prepare(&self, client: &mut Client, _: &mut StdRng) -> Result<()> {
        client.execute("BEGIN")?;
        client.execute(r#"DROP TABLE IF EXISTS "write""#)?;
        client.execute(r#"CREATE TABLE "write" (id INT PRIMARY KEY, value STRING NOT NULL)"#)?;
        client.execute("COMMIT")?;
        Ok(())
    }

    fn generate(&self, rng: StdRng) -> impl Iterator<Item = Self::Item> + 'static {
        WriteGenerator { next_id: 1, size: self.size, batch: self.batch, rng }
    }

    fn execute(client: &mut Client, item: &Self::Item) -> Result<()> {
        let batch_size = item.len();
        let query = format!(
            r#"INSERT INTO "write" (id, value) VALUES {}"#,
            item.iter().map(|(id, value)| format!("({}, '{}')", id, value)).join(", ")
        );
        if let StatementResult::Insert { count } = client.execute(&query)? {
            assert_eq!(count as usize, batch_size, "Unexpected row count");
        } else {
            panic!("Unexpected result")
        }
        Ok(())
    }

    fn verify(&self, client: &mut Client, txns: usize) -> Result<()> {
        let count: i64 = client.execute(r#"SELECT COUNT(*) FROM "write""#)?.try_into()?;
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

    fn prepare(&self, client: &mut Client, rng: &mut StdRng) -> Result<()> {
        let petnames = petname::Petnames::default();
        client.execute("BEGIN")?;
        client.execute("DROP TABLE IF EXISTS account")?;
        client.execute("DROP TABLE IF EXISTS customer")?;
        client.execute(
            "CREATE TABLE customer (
                    id INTEGER PRIMARY KEY,
                    name STRING NOT NULL
                )",
        )?;
        client.execute(
            "CREATE TABLE account (
                    id INTEGER PRIMARY KEY,
                    customer_id INTEGER NOT NULL INDEX REFERENCES customer,
                    balance INTEGER NOT NULL
                )",
        )?;
        client.execute(&format!(
            "INSERT INTO customer VALUES {}",
            (1..=self.customers)
                .zip(petnames.iter(rng, 3, " "))
                .map(|(id, name)| format!("({}, '{}')", id, name))
                .join(", ")
        ))?;
        client.execute(&format!(
            "INSERT INTO account VALUES {}",
            (1..=self.customers)
                .flat_map(|c| (1..=self.accounts).map(move |a| (c, (c - 1) * self.accounts + a)))
                .map(|(c, a)| (format!("({}, {}, {})", a, c, self.balance)))
                .join(", ")
        ))?;
        client.execute("COMMIT")?;
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

    fn execute(client: &mut Client, item: &Self::Item) -> Result<()> {
        let (from, to, mut amount) = item;

        client.execute("BEGIN")?;

        let row: Row = client
            .execute(&format!(
                "SELECT a.id, a.balance
                        FROM account a JOIN customer c ON a.customer_id = c.id
                        WHERE c.id = {}
                        ORDER BY a.balance DESC
                        LIMIT 1",
                from
            ))?
            .try_into()?;
        let mut row = row.into_iter();
        let from_account: i64 = row.next().unwrap().try_into()?;
        let from_balance: i64 = row.next().unwrap().try_into()?;
        amount = std::cmp::min(amount, from_balance as u64);

        let to_account: i64 = client
            .execute(&format!(
                "SELECT a.id, a.balance
                        FROM account a JOIN customer c ON a.customer_id = c.id
                        WHERE c.id = {}
                        ORDER BY a.balance ASC
                        LIMIT 1",
                to
            ))?
            .try_into()?;

        client.execute(&format!(
            "UPDATE account SET balance = balance - {} WHERE id = {}",
            amount, from_account,
        ))?;
        client.execute(&format!(
            "UPDATE account SET balance = balance + {} WHERE id = {}",
            amount, to_account,
        ))?;

        client.execute("COMMIT")?;

        Ok(())
    }

    fn verify(&self, client: &mut Client, _: usize) -> Result<()> {
        let balance: i64 = client.execute("SELECT SUM(balance) FROM account")?.try_into()?;
        assert_eq!(balance as u64, self.customers * self.accounts * self.balance);
        let negative: i64 =
            client.execute("SELECT COUNT(*) FROM account WHERE balance < 0")?.try_into()?;
        assert_eq!(negative, 0);
        Ok(())
    }
}
