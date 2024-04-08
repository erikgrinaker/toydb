use rand::Rng;
use toydb::error::Result;
use toydb::raft::NodeID;
use toydb::Client;

/// Runs a toyDB cluster using the built binary in a temporary directory. The
/// cluster will be killed and removed when dropped.
///
/// This runs the cluster as child processes using the built binary instead of
/// spawning in-memory threads for a couple of reasons: it avoids having to
/// gracefully shut down the server (which is complicated by e.g.
/// TcpListener::accept() not being interruptable), and it tests the entire
/// server (and eventually the toySQL client) end-to-end.
pub struct TestCluster {
    nodes: u8,
    dir: tempdir::TempDir,
    children: std::collections::HashMap<NodeID, std::process::Child>,
}

impl TestCluster {
    const SQL_BASE_PORT: u16 = 19600;
    const RAFT_BASE_PORT: u16 = 19700;

    /// Creates a new test cluster.
    pub fn new(nodes: u8) -> Result<Self> {
        Ok(Self {
            nodes,
            dir: tempdir::TempDir::new("toydb")?,
            children: std::collections::HashMap::new(),
        })
    }

    /// Creates a new test cluster and starts it.
    pub async fn run(nodes: u8) -> Result<Self> {
        let mut tc = Self::new(nodes)?;
        tc.start().await?;
        Ok(tc)
    }

    /// Creates a new test cluster, starts it, and imports an initial dataset.
    pub async fn run_with(nodes: u8, init: &str) -> Result<Self> {
        let tc = Self::run(nodes).await?;

        let mut c = tc.connect_any().await?;
        c.execute("BEGIN").await?;
        for stmt in init.split(';') {
            c.execute(stmt).await?;
        }
        c.execute("COMMIT").await?;

        Ok(tc)
    }

    /// Returns an iterator over the cluster node IDs.
    fn ids(&self) -> impl Iterator<Item = NodeID> {
        1..=self.nodes
    }

    /// Asserts that the given node ID exists.
    fn assert_id(&self, id: NodeID) {
        assert!(id > 0 && id <= self.nodes, "invalid node ID {}", id)
    }

    /// Asserts that all children are still alive.
    fn assert_alive(&mut self) {
        for (id, child) in self.children.iter_mut() {
            if let Some(s) = child.try_wait().expect("Failed to check child exit status") {
                panic!("Node {id} exited with status {s}")
            }
        }
    }

    /// Returns the path to the given node's directory.
    fn node_path(&self, id: NodeID) -> std::path::PathBuf {
        self.assert_id(id);
        self.dir.path().join(format!("toydb{}", id))
    }

    /// Generates a config file for the given node.
    fn node_config(&self, id: NodeID) -> String {
        self.assert_id(id);
        let mut cfg = String::new();
        cfg.push_str(&format!("id: {}\n", id));
        cfg.push_str(&format!("data_dir: {}\n", self.node_path(id).to_string_lossy()));
        cfg.push_str(&format!("listen_sql: {}\n", self.node_address_sql(id)));
        cfg.push_str(&format!("listen_raft: {}\n", self.node_address_raft(id)));
        cfg.push_str("peers: {\n");
        for peer in self.ids().filter(|p| p != &id) {
            cfg.push_str(&format!("  '{}': {},\n", peer, self.node_address_raft(peer)))
        }
        cfg.push_str("}\n");
        cfg
    }

    /// Returns the given node's Raft TCP address.
    fn node_address_raft(&self, id: NodeID) -> String {
        self.assert_id(id);
        format!("localhost:{}", Self::RAFT_BASE_PORT + id as u16)
    }

    /// Returns the given node's SQL TCP address.
    fn node_address_sql(&self, id: NodeID) -> String {
        self.assert_id(id);
        format!("localhost:{}", Self::SQL_BASE_PORT + id as u16)
    }

    /// Starts the test cluster. It keeps running until the cluster is dropped.
    ///
    /// TODO: this only uses async because Client is still async. Remove it.
    pub async fn start(&mut self) -> Result<()> {
        // Build the binary.
        let build = escargot::CargoBuild::new().bin("toydb").run().expect("Failed to build binary");

        // Spawn nodes.
        for id in self.ids() {
            // Create node directory and config file.
            std::fs::create_dir_all(&self.node_path(id))?;
            std::fs::write(&self.node_path(id).join("toydb.yaml"), self.node_config(id))?;

            // Spawn node. Silence output by default, since there doesn't appear
            // to be a way to pass the output to the "cargo test" output capture
            // without a thread piping it through println!.
            //
            // TODO: see if there's a way to send this to "cargo test" and have
            // it capture it like println!.
            let child = build
                .command()
                .args(vec!["-c", &self.node_path(id).join("toydb.yaml").to_string_lossy()])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()?;
            self.children.insert(id, child);
        }
        self.assert_alive();

        // Wait for all nodes to be ready, by connecting to them and fetching
        // the cluster status.
        const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
        const COOLDOWN: std::time::Duration = std::time::Duration::from_millis(200);

        let deadline = std::time::Instant::now().checked_add(TIMEOUT).unwrap();
        for id in self.ids() {
            while let Err(e) = async { self.connect(id).await?.status().await }.await {
                self.assert_alive();
                if std::time::Instant::now() >= deadline {
                    return Err(e);
                }
                tokio::time::sleep(COOLDOWN).await;
            }
        }

        Ok(())
    }

    /// Connects to the given cluster node.
    pub async fn connect(&self, id: NodeID) -> Result<Client> {
        self.assert_id(id);
        Client::new(self.node_address_sql(id)).await
    }

    /// Connects to a random cluster node.
    pub async fn connect_any(&self) -> Result<Client> {
        self.connect(rand::thread_rng().gen_range(1..=self.nodes)).await
    }
}

impl Drop for TestCluster {
    /// Kills the child processes when the cluster is dropped. The temp dir is
    /// removed by TempDir::drop().
    ///
    /// Note that cargo will itself kill all child processes if the tests are
    /// aborted via e.g. Ctrl-C: https://github.com/rust-lang/cargo/issues/5598
    fn drop(&mut self) {
        for (_, mut child) in self.children.drain() {
            child.kill().expect("Failed to kill node");
            child.wait().expect("Failed to wait for node to terminate");
        }
    }
}
