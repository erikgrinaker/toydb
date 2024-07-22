use rand::Rng;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::Write as _;
use std::path::Path;
use std::time::Duration;
use toydb::raft::NodeID;
use toydb::Client;

/// Timeout for node readiness.
const TIMEOUT: Duration = Duration::from_secs(5);

/// The base SQL port (+id).
const SQL_BASE_PORT: u16 = 19600;

/// The base Raft port (+id).
const RAFT_BASE_PORT: u16 = 19700;

/// Runs a toyDB cluster using the built binary in a temporary directory. The
/// cluster will be killed and removed when dropped.
///
/// This runs the cluster as child processes using the built binary instead of
/// spawning in-memory threads for a couple of reasons: it avoids having to
/// gracefully shut down the server (which is complicated by e.g.
/// TcpListener::accept() not being interruptable), and it tests the entire
/// server (and eventually the toySQL client) end-to-end.
pub struct TestCluster {
    servers: BTreeMap<NodeID, TestServer>,
    #[allow(dead_code)]
    dir: tempfile::TempDir, // deleted when dropped
}

type NodePorts = BTreeMap<NodeID, (u16, u16)>; // raft,sql on localhost

impl TestCluster {
    /// Runs and returns a test cluster. It keeps running until dropped.
    pub fn run(nodes: u8) -> Result<Self, Box<dyn Error>> {
        // Create temporary directory.
        let dir = tempfile::TempDir::with_prefix("toydb")?;

        // Allocate port numbers for nodes.
        let ports: NodePorts = (1..=nodes)
            .map(|id| (id, (RAFT_BASE_PORT + id as u16, SQL_BASE_PORT + id as u16)))
            .collect();

        // Start nodes.
        let mut servers = BTreeMap::new();
        for id in 1..=nodes {
            let dir = dir.path().join(format!("toydb{id}"));
            servers.insert(id, TestServer::run(id, &dir, &ports)?);
        }

        // Wait for the nodes to be ready, by fetching the server status.
        let started = std::time::Instant::now();
        for server in servers.values_mut() {
            while let Err(error) = server.connect().and_then(|mut c| Ok(c.status()?)) {
                server.assert_alive();
                if started.elapsed() >= TIMEOUT {
                    return Err(error);
                }
                std::thread::sleep(Duration::from_millis(200));
            }
        }

        Ok(Self { servers, dir })
    }

    /// Connects to a random cluster node using a Rust client. Testing with
    /// toysql is too annoying, since we have to deal with rustyline, PTYs,
    /// echoing, multiline editing, etc.
    pub fn connect(&self) -> Result<Client, Box<dyn Error>> {
        let id = rand::thread_rng().gen_range(1..=self.servers.len()) as NodeID;
        self.servers.get(&id).unwrap().connect()
    }
}

/// A toyDB server.
pub struct TestServer {
    id: NodeID,
    child: std::process::Child,
    sql_port: u16,
}

impl TestServer {
    /// Runs a toyDB server.
    fn run(id: NodeID, dir: &Path, ports: &NodePorts) -> Result<Self, Box<dyn Error>> {
        // Build and write the configuration file.
        let configfile = dir.join("toydb.yaml");
        std::fs::create_dir_all(dir)?;
        std::fs::write(&configfile, Self::build_config(id, dir, ports)?)?;

        // Build the binary.
        //
        // TODO: this may contribute to slow tests, consider building once.
        let build = escargot::CargoBuild::new().bin("toydb").run()?;

        // Spawn process. Discard output.
        let child = build
            .command()
            .args(["-c", &configfile.to_string_lossy()])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()?;

        let (_, sql_port) = ports.get(&id).copied().expect("node not in ports");
        Ok(Self { id, child, sql_port })
    }

    /// Generates a config file for the given node.
    fn build_config(id: NodeID, dir: &Path, ports: &NodePorts) -> Result<String, Box<dyn Error>> {
        let (raft_port, sql_port) = ports.get(&id).expect("node not in ports");
        let mut cfg = String::new();
        writeln!(cfg, "id: {id}")?;
        writeln!(cfg, "data_dir: {}", dir.to_string_lossy())?;
        writeln!(cfg, "listen_raft: localhost:{raft_port}")?;
        writeln!(cfg, "listen_sql: localhost:{sql_port}")?;
        writeln!(cfg, "peers: {{")?;
        for (peer_id, (peer_raft_port, _)) in ports.iter().filter(|(peer, _)| **peer != id) {
            writeln!(cfg, "  '{peer_id}': localhost:{peer_raft_port},")?;
        }
        writeln!(cfg, "}}")?;
        Ok(cfg)
    }

    /// Asserts that the server is still running.
    fn assert_alive(&mut self) {
        if let Some(status) = self.child.try_wait().expect("failed to check exit status") {
            panic!("node {id} exited with status {status}", id = self.id)
        }
    }

    /// Connects to the server using a regular client.
    fn connect(&self) -> Result<Client, Box<dyn Error>> {
        Ok(Client::connect(("localhost", self.sql_port))?)
    }
}

impl Drop for TestServer {
    // Kills the child process when dropped.
    fn drop(&mut self) {
        self.child.kill().expect("failed to kill node");
        self.child.wait().expect("failed to wait for node to terminate");
    }
}
