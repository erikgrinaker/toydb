use super::*;
use std::collections::{HashMap, HashSet};

// A leader serves requests and replicates the log to followers.
#[derive(Debug)]
pub struct Leader {
    /// Number of ticks since last heartbeat.
    heartbeat_ticks: u64,
    /// The next index to replicate to a peer.
    peer_next_index: HashMap<String, u64>,
    /// The last index known to be replicated on a peer.
    peer_last_index: HashMap<String, u64>,
    /// Any client calls being processed.
    calls: Calls,
}

impl Leader {
    /// Creates a new leader role.
    pub fn new(peers: Vec<String>, last_index: u64) -> Self {
        let mut leader = Self {
            heartbeat_ticks: 0,
            peer_next_index: HashMap::new(),
            peer_last_index: HashMap::new(),
            calls: Calls::new(),
        };
        for peer in peers {
            leader.peer_next_index.insert(peer.clone(), last_index + 1);
            leader.peer_last_index.insert(peer.clone(), 0);
        }
        leader
    }
}

impl<L: kv::storage::Storage, S: State> RoleNode<Leader, L, S> {
    /// Transforms the leader into a follower
    fn become_follower(
        mut self,
        term: u64,
        leader: &str,
    ) -> Result<RoleNode<Follower, L, S>, Error> {
        info!("Discovered new leader {} for term {}, following", leader, term);
        self.save_term(term, None)?;
        self.become_role(Follower::new(Some(leader), None))
    }

    /// Appends an entry to the log and replicates it to peers.
    pub fn append(&mut self, command: Option<Vec<u8>>) -> Result<u64, Error> {
        let index = self.log.append(Entry { term: self.term, command })?;
        for peer in self.peers.iter() {
            self.replicate(peer)?;
        }
        Ok(index)
    }

    /// Applies any pending log entries.
    fn apply(&mut self) -> Result<u64, Error> {
        let (mut index, _) = self.log.get_applied();
        while let Some((i, output)) = self.log.apply(&mut self.state)? {
            index = i;
            if let Some(call) = self.role.calls.log_applied(index) {
                self.send(
                    call.from.as_deref(),
                    Event::RespondState { call_id: call.id, response: output },
                )?
            }
        }
        Ok(index)
    }

    /// Commits any pending log entries.
    fn commit(&mut self) -> Result<u64, Error> {
        let (last_index, _) = self.log.get_last();
        let (commit_index, _) = self.log.get_committed();
        let mut last_indexes = vec![last_index];
        last_indexes.extend(self.role.peer_last_index.values());
        last_indexes.sort();
        last_indexes.reverse();
        let quorum_index = last_indexes[self.quorum() as usize - 1];
        if quorum_index < commit_index {
            return Ok(commit_index);
        }
        // We can only safely commit up to an entry from our own term, see
        // figure 8 in Raft paper for background.
        match self.log.get(quorum_index)? {
            Some(ref entry) if entry.term == self.term => self.log.commit(quorum_index),
            _ => Ok(commit_index),
        }
    }

    /// Replicates the log to a peer.
    fn replicate(&self, peer: &str) -> Result<(), Error> {
        let peer_next = self
            .role
            .peer_next_index
            .get(peer)
            .cloned()
            .ok_or_else(|| Error::Internal(format!("Unknown peer {}", peer)))?;
        let base_index = if peer_next > 0 { peer_next - 1 } else { 0 };
        let base_term = match self.log.get(base_index)? {
            Some(base) => base.term,
            None if base_index == 0 => 0,
            None => return Err(Error::Internal(format!("Missing base entry {}", base_index))),
        };
        let entries = self.log.range(peer_next..)?;
        debug!("Replicating {} entries at base {} to {}", entries.len(), base_index, peer);
        self.send(Some(peer), Event::ReplicateEntries { base_index, base_term, entries })?;
        Ok(())
    }

    /// Registers a vote for quorum-based client calls.
    fn vote_call(&mut self, from: &str, commit_index: u64) -> Result<(), Error> {
        for call in self.role.calls.quorum_vote(from, commit_index) {
            match call.operation {
                Operation::ReadState { command, .. } => self.send(
                    call.from.as_deref(),
                    Event::RespondState { call_id: call.id, response: self.state.query(command)? },
                )?,
                _ => return Err(Error::Internal(format!("Unsupported call {:?}", call))),
            }
        }
        Ok(())
    }

    /// Processes a message.
    pub fn step(mut self, mut msg: Message) -> Result<Node<L, S>, Error> {
        if !self.normalize_message(&mut msg) {
            return Ok(self.into());
        }
        if msg.term > self.term {
            if let Some(from) = &msg.from {
                return self.become_follower(msg.term, from)?.step(msg);
            }
        }

        match msg.event {
            Event::ConfirmLeader { commit_index, has_committed } => {
                if let Some(from) = &msg.from {
                    self.vote_call(from, commit_index)?;
                    if !has_committed {
                        self.replicate(from)?;
                    }
                }
            }
            Event::AcceptEntries { last_index } => {
                if let Some(from) = msg.from {
                    self.role.peer_last_index.insert(from.clone(), last_index);
                    self.role.peer_next_index.insert(from, last_index + 1);
                }
                self.commit()?;
                self.apply()?;
            }
            Event::RejectEntries => {
                if let Some(from) = msg.from {
                    self.role.peer_next_index.entry(from.clone()).and_modify(|i| {
                        if *i > 1 {
                            *i -= 1
                        }
                    });
                    self.replicate(&from)?;
                }
            }
            Event::QueryState { call_id, command } => {
                let (commit_index, commit_term) = self.log.get_committed();
                self.role.calls.register(Call {
                    id: call_id,
                    from: msg.from,
                    operation: Operation::ReadState {
                        command,
                        commit_index,
                        quorum: self.quorum(),
                        votes: HashSet::new(),
                    },
                });
                self.vote_call(self.id.clone().as_ref(), commit_index)?;
                // Send heartbeats immediately, so we don't have to wait for the next tick
                self.broadcast(Event::Heartbeat { commit_index, commit_term })?;
            }
            Event::MutateState { call_id, command } => {
                let index = self.append(Some(command))?;
                self.role.calls.register(Call {
                    id: call_id,
                    from: msg.from,
                    operation: Operation::MutateState { log_index: index },
                });
                if self.peers.is_empty() {
                    self.commit()?;
                    self.apply()?;
                }
            }
            Event::Heartbeat { .. } => {}
            Event::SolicitVote { .. } => {}
            Event::GrantVote => {}
            Event::ReplicateEntries { .. } => {}
            // FIXME We may want to handle these
            Event::RespondState { .. } => {}
            Event::RespondError { .. } => {}
        }

        Ok(self.into())
    }

    /// Processes a logical clock tick.
    pub fn tick(mut self) -> Result<Node<L, S>, Error> {
        self.apply()?;
        self.role.heartbeat_ticks += 1;
        if self.role.heartbeat_ticks >= HEARTBEAT_INTERVAL {
            self.role.heartbeat_ticks = 0;
            let (commit_index, commit_term) = self.log.get_committed();
            self.broadcast(Event::Heartbeat { commit_index, commit_term })?;
        }
        Ok(self.into())
    }
}

/// A client call processed by the leader.
#[derive(Clone, Debug, PartialEq)]
struct Call {
    id: Vec<u8>,
    from: Option<String>,
    operation: Operation,
}

/// An operation performed by a call.
#[derive(Clone, Debug, PartialEq)]
enum Operation {
    /// A mutation submitted to the Raft log.
    MutateState { log_index: u64 },
    /// A state machine read requiring a quorum.
    ReadState { command: Vec<u8>, commit_index: u64, quorum: u64, votes: HashSet<String> },
}

/// A set of calls
#[derive(Clone, Debug)]
struct Calls {
    calls: Vec<Call>,
}

impl Calls {
    /// Creates a new Calls
    fn new() -> Self {
        Self { calls: Vec::new() }
    }

    /// Registers a call
    fn register(&mut self, call: Call) {
        self.calls.push(call);
    }

    /// Signals application of the log entry with the given index, removes and
    /// returns the call tracking the entry (if any).
    fn log_applied(&mut self, index: u64) -> Option<Call> {
        self.calls
            .iter()
            .position(|call| match call.operation {
                Operation::MutateState { log_index } => log_index == index,
                Operation::ReadState { .. } => false,
            })
            .map(|i| self.calls.remove(i))
    }

    /// Signals a leadership vote by a peer for a given commit index, removes and
    /// returns any calls which have received votes from a quorum.
    fn quorum_vote(&mut self, voter: &str, index: u64) -> Vec<Call> {
        let mut indexes = Vec::new();
        for (i, call) in self.calls.iter_mut().enumerate() {
            match call.operation {
                Operation::MutateState { .. } => {}
                Operation::ReadState { commit_index, ref mut votes, quorum, .. } => {
                    if index >= commit_index {
                        votes.insert(voter.to_owned());
                        if votes.len() as u64 >= quorum {
                            indexes.push(i)
                        }
                    }
                }
            }
        }
        indexes.into_iter().enumerate().map(|(j, i)| self.calls.remove(i - j)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{assert_messages, assert_node, TestState};
    use super::*;
    use crossbeam_channel::Receiver;

    fn setup() -> (RoleNode<Leader, kv::storage::Memory, TestState>, Receiver<Message>) {
        let (sender, receiver) = crossbeam_channel::unbounded();
        let mut state = TestState::new();
        let mut log = Log::new(kv::Simple::new(kv::storage::Memory::new())).unwrap();
        log.append(Entry { term: 1, command: Some(vec![0x01]) }).unwrap();
        log.append(Entry { term: 1, command: Some(vec![0x02]) }).unwrap();
        log.append(Entry { term: 2, command: Some(vec![0x03]) }).unwrap();
        log.append(Entry { term: 3, command: Some(vec![0x04]) }).unwrap();
        log.append(Entry { term: 3, command: Some(vec![0x05]) }).unwrap();
        log.commit(2).unwrap();
        log.apply(&mut state).unwrap();

        let peers = vec!["b".into(), "c".into(), "d".into(), "e".into()];
        let (last_index, _) = log.get_last();

        let mut node = RoleNode {
            id: "a".into(),
            peers: peers.clone(),
            term: 3,
            log,
            state,
            sender,
            role: Leader::new(peers, last_index),
        };
        node.save_term(3, None).unwrap();
        (node, receiver)
    }

    #[test]
    // ConfirmLeader with has_committed has no effect without any calls
    fn step_confirmleader() {
        let (leader, rx) = setup();
        let mut node: Node<_, _> = leader.into();

        node = node
            .step(Message {
                from: Some("b".into()),
                to: Some("a".into()),
                term: 3,
                event: Event::ConfirmLeader { commit_index: 2, has_committed: true },
            })
            .unwrap();
        assert_node(&node).is_leader().term(3).committed(2).applied(1);
        assert_messages(&rx, vec![]);
    }

    #[test]
    // ConfirmLeader without has_committed triggers replication
    fn step_confirmleader_without_has_committed() {
        let (leader, rx) = setup();
        let mut node: Node<_, _> = leader.into();

        node = node
            .step(Message {
                from: Some("b".into()),
                to: Some("a".into()),
                term: 3,
                event: Event::ConfirmLeader { commit_index: 2, has_committed: false },
            })
            .unwrap();
        assert_node(&node).is_leader().term(3).committed(2).applied(1);
        assert_messages(
            &rx,
            vec![Message {
                from: Some("a".into()),
                to: Some("b".into()),
                term: 3,
                event: Event::ReplicateEntries { base_index: 5, base_term: 3, entries: vec![] },
            }],
        );
    }

    #[test]
    // Heartbeats from other leaders in current term are ignored.
    fn step_heartbeat_current_term() {
        let (leader, rx) = setup();
        let mut node: Node<_, _> = leader.into();

        node = node
            .step(Message {
                from: Some("b".into()),
                to: Some("a".into()),
                term: 3,
                event: Event::Heartbeat { commit_index: 5, commit_term: 3 },
            })
            .unwrap();
        assert_node(&node).is_leader().term(3).committed(2).applied(1);
        assert_messages(&rx, vec![]);
    }

    #[test]
    // Heartbeats from other leaders in future term converts to follower and steps.
    fn step_heartbeat_future_term() {
        let (leader, rx) = setup();
        let mut node: Node<_, _> = leader.into();

        node = node
            .step(Message {
                from: Some("b".into()),
                to: Some("a".into()),
                term: 4,
                event: Event::Heartbeat { commit_index: 7, commit_term: 4 },
            })
            .unwrap();
        assert_node(&node).is_follower().term(4).leader(Some("b")).committed(2).applied(1);
        assert_messages(
            &rx,
            vec![Message {
                from: Some("a".into()),
                to: Some("b".into()),
                term: 4,
                event: Event::ConfirmLeader { commit_index: 7, has_committed: false },
            }],
        );
    }

    #[test]
    // Heartbeats from other leaders in past terms are ignored.
    fn step_heartbeat_past_term() {
        let (leader, rx) = setup();
        let mut node: Node<_, _> = leader.into();

        node = node
            .step(Message {
                from: Some("b".into()),
                to: Some("a".into()),
                term: 2,
                event: Event::Heartbeat { commit_index: 3, commit_term: 2 },
            })
            .unwrap();
        assert_node(&node).is_leader().term(3).committed(2).applied(1);
        assert_messages(&rx, vec![]);
    }

    #[test]
    fn step_acceptentries() {
        let (leader, rx) = setup();
        let mut node: Node<_, _> = leader.into();

        node = node
            .step(Message {
                from: Some("b".into()),
                to: Some("a".into()),
                term: 3,
                event: Event::AcceptEntries { last_index: 4 },
            })
            .unwrap();
        assert_node(&node).committed(2).applied(2);
        assert_messages(&rx, vec![]);

        node = node
            .step(Message {
                from: Some("c".into()),
                to: Some("a".into()),
                term: 3,
                event: Event::AcceptEntries { last_index: 5 },
            })
            .unwrap();
        assert_node(&node).committed(4).applied(4);
        assert_messages(&rx, vec![]);

        node = node
            .step(Message {
                from: Some("d".into()),
                to: Some("a".into()),
                term: 3,
                event: Event::AcceptEntries { last_index: 5 },
            })
            .unwrap();
        assert_node(&node).committed(5).applied(5);
        assert_messages(&rx, vec![]);

        assert_node(&node).is_leader().term(3);
    }

    #[test]
    // Duplicate AcceptEntries from single node should not trigger commit.
    fn step_acceptentries_duplicate() {
        let (leader, rx) = setup();
        let mut node: Node<_, _> = leader.into();

        for _ in 0..5 {
            node = node
                .step(Message {
                    from: Some("b".into()),
                    to: Some("a".into()),
                    term: 3,
                    event: Event::AcceptEntries { last_index: 5 },
                })
                .unwrap();
            assert_node(&node).is_leader().term(3).committed(2).applied(2);
            assert_messages(&rx, vec![]);
        }
    }

    #[test]
    // AcceptEntries quorum for entry in past term should not trigger commit
    fn step_acceptentries_past_term() {
        let (leader, rx) = setup();
        let peers = leader.peers.clone();
        let mut node: Node<_, _> = leader.into();

        for peer in peers.into_iter() {
            node = node
                .step(Message {
                    from: Some(peer),
                    to: Some("a".into()),
                    term: 3,
                    event: Event::AcceptEntries { last_index: 3 },
                })
                .unwrap();
            assert_node(&node).is_leader().term(3).committed(2).applied(2);
            assert_messages(&rx, vec![]);
        }
    }

    #[test]
    // AcceptEntries quorum for missing future entry
    fn step_acceptentries_future_index() {
        let (leader, rx) = setup();
        let peers = leader.peers.clone();
        let mut node: Node<_, _> = leader.into();

        for (i, peer) in peers.into_iter().enumerate() {
            node = node
                .step(Message {
                    from: Some(peer),
                    to: Some("a".into()),
                    term: 3,
                    event: Event::AcceptEntries { last_index: 7 },
                })
                .unwrap();
            // The local leader will cast a vote to commit 5, thus
            // when we have votes 2x7, 1x5, 2x0 we will commit index 5.
            // However, we will correctly ignore the following votes for 7.
            let c = if i == 0 { 2 } else { 5 };
            assert_node(&node).is_leader().term(3).committed(c).applied(c).last(5);
            assert_messages(&rx, vec![]);
        }
    }

    #[test]
    fn step_rejectentries() {
        let (leader, rx) = setup();
        let entries = leader.log.range(0..).unwrap();
        let mut node: Node<_, _> = leader.into();

        for i in 0..(entries.len() + 3) {
            node = node
                .step(Message {
                    from: Some("b".into()),
                    to: Some("a".into()),
                    term: 3,
                    event: Event::RejectEntries,
                })
                .unwrap();
            assert_node(&node).is_leader().term(3).committed(2).applied(1);
            let index = if i >= entries.len() { 0 } else { entries.len() - i - 1 };
            let replicate = entries.get(index..).unwrap().to_vec();
            assert_messages(
                &rx,
                vec![Message {
                    from: Some("a".into()),
                    to: Some("b".into()),
                    term: 3,
                    event: Event::ReplicateEntries {
                        base_index: index as u64,
                        base_term: if index > 0 {
                            entries.get(index - 1).map(|e| e.term).unwrap()
                        } else {
                            0
                        },
                        entries: replicate,
                    },
                }],
            );
        }
    }

    #[test]
    fn step_mutatestate_readstate() {
        let (leader, rx) = setup();
        let peers = leader.peers.clone();
        let quorum = leader.quorum();
        let mut node: Node<_, _> = leader.into();

        // Submit the mutate call from local sender, and observe it being
        // appended to log and replicated to peers. The mutate command will be
        // appended to the internal commands list of TestState and returned with
        // a 0xff prefix, and a subsequent read command can read back the
        // command at the index given by the command with the result prefixed
        // with 0xbb.
        node = node
            .step(Message {
                from: None,
                to: None,
                term: 0,
                event: Event::MutateState { call_id: vec![0x01], command: vec![0xaf] },
            })
            .unwrap();
        assert_node(&node)
            .is_leader()
            .term(3)
            .committed(2)
            .applied(1)
            .last(6)
            .entry(6, Entry { term: 3, command: Some(vec![0xaf]) });
        for peer in peers.iter().cloned() {
            assert!(!rx.is_empty());
            assert_eq!(
                rx.recv().unwrap(),
                Message {
                    from: Some("a".into()),
                    to: Some(peer),
                    term: 3,
                    event: Event::ReplicateEntries {
                        base_index: 5,
                        base_term: 3,
                        entries: vec![Entry { term: 3, command: Some(vec![0xaf]) },]
                    },
                }
            )
        }

        // Receive some ConfirmLeader messages from peers, to make sure
        // they do not affect mutation calls at all.
        for peer in peers.iter().cloned() {
            node = node
                .step(Message {
                    from: Some(peer),
                    to: Some("a".into()),
                    term: 3,
                    event: Event::ConfirmLeader { commit_index: 6, has_committed: true },
                })
                .unwrap();
            assert_node(&node).committed(2).applied(1).last(6);
        }
        assert_messages(&rx, vec![]);

        // Receive AcceptEntries calls from peers, which after a quorum
        // will commit and apply the entries and return a call response.
        for (i, peer) in peers.iter().cloned().enumerate() {
            node = node
                .step(Message {
                    from: Some(peer),
                    to: Some("a".into()),
                    term: 3,
                    event: Event::AcceptEntries { last_index: 6 },
                })
                .unwrap();
            if (i as u64 + 2) < quorum {
                assert_node(&node).committed(2).applied(2).last(6);
                assert_messages(&rx, vec![]);
            } else {
                assert_node(&node).committed(6).applied(6).last(6);
                assert!(!rx.is_empty());
            }
        }
        assert_messages(
            &rx,
            vec![Message {
                from: Some("a".into()),
                to: None,
                term: 3,
                event: Event::RespondState { call_id: vec![0x01], response: vec![0xff, 0xaf] },
            }],
        );

        // Submit a read call to read back our entry, which will immediately
        // send heartbeats to all peers to confirm that we're still the leader.
        node = node
            .step(Message {
                from: None,
                to: None,
                term: 0,
                event: Event::QueryState { call_id: vec![0x02], command: vec![0x06] },
            })
            .unwrap();
        assert_node(&node).is_leader().term(3);
        for peer in peers.iter().cloned() {
            assert!(!rx.is_empty());
            assert_eq!(
                rx.recv().unwrap(),
                Message {
                    from: Some("a".into()),
                    to: Some(peer),
                    term: 3,
                    event: Event::Heartbeat { commit_index: 6, commit_term: 3 },
                }
            )
        }
        assert_messages(&rx, vec![]);

        // Check that ConfirmLeader calls for an old commit_index as well as
        // AcceptEntries calls for the current last_index do not trigger a
        // read response.
        for peer in peers.iter().cloned() {
            node = node
                .step(Message {
                    from: Some(peer.clone()),
                    to: Some("a".into()),
                    term: 3,
                    event: Event::ConfirmLeader { commit_index: 5, has_committed: true },
                })
                .unwrap();
            node = node
                .step(Message {
                    from: Some(peer),
                    to: Some("a".into()),
                    term: 3,
                    event: Event::AcceptEntries { last_index: 6 },
                })
                .unwrap();
            assert_messages(&rx, vec![]);
        }

        // After we receive leadership confirmation from a quorum of
        // votes, the read response is returned.
        for (i, peer) in peers.iter().cloned().enumerate() {
            node = node
                .step(Message {
                    from: Some(peer.clone()),
                    to: Some("a".into()),
                    term: 3,
                    event: Event::ConfirmLeader { commit_index: 6, has_committed: true },
                })
                .unwrap();
            assert_node(&node).is_leader().term(3).committed(6).applied(6).last(6);
            if (i as u64 + 2) < quorum {
                assert_messages(&rx, vec![]);
            } else {
                assert!(!rx.is_empty());
            }
        }
        assert_messages(
            &rx,
            vec![Message {
                from: Some("a".into()),
                to: None,
                term: 3,
                event: Event::RespondState { call_id: vec![0x02], response: vec![0xbb, 0xaf] },
            }],
        );

        // Further leadership confirmation messages should not trigger messages.
        for peer in peers.iter().cloned() {
            node = node
                .step(Message {
                    from: Some(peer.clone()),
                    to: Some("a".into()),
                    term: 3,
                    event: Event::ConfirmLeader { commit_index: 6, has_committed: true },
                })
                .unwrap();
            assert_messages(&rx, vec![]);
        }
        assert_node(&node).is_leader().term(3).committed(6).applied(6).last(6);
    }

    #[test]
    fn tick() {
        let (leader, rx) = setup();
        let peers = leader.peers.clone();
        let mut node: Node<_, _> = leader.into();
        for _ in 0..5 {
            for _ in 0..HEARTBEAT_INTERVAL {
                assert_messages(&rx, vec![]);
                node = node.tick().unwrap();
                assert_node(&node).is_leader().term(3).committed(2).applied(2);
            }
            for peer in peers.iter() {
                assert!(!rx.is_empty());
                assert_eq!(
                    rx.recv().unwrap(),
                    Message {
                        from: Some("a".into()),
                        to: Some(peer.into()),
                        term: 3,
                        event: Event::Heartbeat { commit_index: 2, commit_term: 1 },
                    }
                );
            }
        }
    }

    fn setup_calls() -> Calls {
        let mut calls = Calls::new();
        calls.register(Call {
            id: vec![0xa0],
            from: None,
            operation: Operation::MutateState { log_index: 1 },
        });
        calls.register(Call {
            id: vec![0xa2],
            from: None,
            operation: Operation::MutateState { log_index: 2 },
        });
        calls.register(Call {
            id: vec![0xa3],
            from: None,
            operation: Operation::MutateState { log_index: 3 },
        });

        calls.register(Call {
            id: vec![0xb0],
            from: None,
            operation: Operation::ReadState {
                command: vec![0x01],
                commit_index: 1,
                quorum: 3,
                votes: HashSet::new(),
            },
        });
        calls.register(Call {
            id: vec![0xb1],
            from: None,
            operation: Operation::ReadState {
                command: vec![0x02],
                commit_index: 2,
                quorum: 3,
                votes: HashSet::new(),
            },
        });
        calls.register(Call {
            id: vec![0xb2],
            from: None,
            operation: Operation::ReadState {
                command: vec![0x02],
                commit_index: 3,
                quorum: 3,
                votes: HashSet::new(),
            },
        });
        calls
    }

    #[test]
    fn calls_log_applied() {
        let mut calls = setup_calls();
        assert_eq!(calls.log_applied(2).unwrap().id, vec![0xa2]);
        assert_eq!(calls.log_applied(2), None);
        assert_eq!(calls.log_applied(9), None);
    }

    #[test]
    fn calls_quorum_vote() {
        let mut calls = setup_calls();
        // 0xb0=1 0xb1=1 0xb2=1
        assert_eq!(calls.quorum_vote("a", 3), vec![]);
        // 0xb0=2 0xb1=1 0xb2=1
        assert_eq!(calls.quorum_vote("b", 1), vec![]);
        // 0xb0=3 0xb1=2 0xb2=1
        assert_eq!(
            calls.quorum_vote("c", 2).into_iter().map(|c| c.id).collect::<Vec<Vec<u8>>>(),
            vec![vec![0xb0_u8]]
        );
        // 0xb1=3 0xb2=2
        assert_eq!(
            calls.quorum_vote("d", 4).into_iter().map(|c| c.id).collect::<Vec<Vec<u8>>>(),
            vec![vec![0xb1_u8]]
        );
        // 0xb2=3
        assert_eq!(
            calls.quorum_vote("e", 3).into_iter().map(|c| c.id).collect::<Vec<Vec<u8>>>(),
            vec![vec![0xb2_u8]]
        );
    }

    #[test]
    fn calls_quorum_vote_multiple() {
        let mut calls = setup_calls();
        assert_eq!(calls.quorum_vote("a", 3), vec![]);
        assert_eq!(calls.quorum_vote("b", 3), vec![]);
        assert_eq!(
            calls.quorum_vote("c", 3).into_iter().map(|c| c.id).collect::<Vec<Vec<u8>>>(),
            vec![vec![0xb0_u8], vec![0xb1_u8], vec![0xb2_u8]]
        );
    }

    #[test]
    fn calls_quorum_vote_same_voter_ignored() {
        let mut calls = setup_calls();
        assert_eq!(calls.quorum_vote("a", 1), vec![]);
        assert_eq!(calls.quorum_vote("a", 1), vec![]);
        assert_eq!(calls.quorum_vote("a", 1), vec![]);
        assert_eq!(calls.quorum_vote("b", 1), vec![]);
        assert_eq!(calls.quorum_vote("b", 1), vec![]);
        assert_eq!(calls.quorum_vote("b", 1), vec![]);
        assert_eq!(
            calls.quorum_vote("c", 1).into_iter().map(|c| c.id).collect::<Vec<Vec<u8>>>(),
            vec![vec![0xb0_u8]]
        );
    }
}
