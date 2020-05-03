use super::super::{Address, Event, Instruction, Message, Response};
use super::{Candidate, Node, RoleNode, ELECTION_TIMEOUT_MAX, ELECTION_TIMEOUT_MIN};
use crate::kv::storage::Storage;
use crate::Error;

use log::{debug, info, warn};
use rand::Rng as _;

// A follower replicates state from a leader.
#[derive(Debug)]
pub struct Follower {
    /// The leader, or None if just initialized.
    leader: Option<String>,
    /// The number of ticks since the last message from the leader.
    leader_seen_ticks: u64,
    /// The timeout before triggering an election.
    leader_seen_timeout: u64,
    /// The node we voted for in the current term, if any.
    voted_for: Option<String>,
}

impl Follower {
    /// Creates a new follower role.
    pub fn new(leader: Option<&str>, voted_for: Option<&str>) -> Self {
        Self {
            leader: leader.map(String::from),
            voted_for: voted_for.map(String::from),
            leader_seen_ticks: 0,
            leader_seen_timeout: rand::thread_rng()
                .gen_range(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX),
        }
    }
}

impl<L: Storage> RoleNode<Follower, L> {
    /// Transforms the node into a candidate.
    fn become_candidate(self) -> Result<RoleNode<Candidate, L>, Error> {
        info!("Starting election for term {}", self.term + 1);
        let mut node = self.become_role(Candidate::new())?;
        node.save_term(node.term + 1, None)?;
        let (last_index, last_term) = node.log.get_last();
        node.send(Address::Peers, Event::SolicitVote { last_index, last_term })?;
        Ok(node)
    }

    /// Transforms the node into a follower for a new leader.
    fn become_follower(mut self, leader: &str, term: u64) -> Result<RoleNode<Follower, L>, Error> {
        let mut voted_for = None;
        if term > self.term {
            info!("Discovered new term {}, following leader {}", term, leader);
            self.save_term(term, None)?;
        } else {
            info!("Discovered leader {}, following", leader);
            voted_for = self.role.voted_for;
        };
        self.role = Follower::new(Some(leader), voted_for.as_deref());
        self.abort_proxied()?;
        self.forward_queued(Address::Peer(leader.to_string()))?;
        Ok(self)
    }

    /// Checks if an address is the current leader
    fn is_leader(&self, from: &Address) -> bool {
        match (&self.role.leader, from) {
            (Some(leader), Address::Peer(from)) if leader == from => true,
            _ => false,
        }
    }

    /// Processes a message.
    pub fn step(mut self, msg: Message) -> Result<Node<L>, Error> {
        if let Err(err) = self.validate(&msg) {
            warn!("Ignoring invalid message: {}", err);
            return Ok(self.into());
        }
        if let Address::Peer(from) = &msg.from {
            if msg.term > self.term || self.role.leader.is_none() {
                return self.become_follower(from, msg.term)?.step(msg);
            }
        }
        if self.is_leader(&msg.from) {
            self.role.leader_seen_ticks = 0
        }

        match msg.event {
            Event::Heartbeat { commit_index, commit_term } => {
                if self.is_leader(&msg.from) {
                    let (prev_commit_index, _) = self.log.get_committed();
                    let has_committed = self.log.has(commit_index, commit_term)?;
                    if has_committed {
                        self.log.commit(commit_index)?;
                        // FIXME This should use a range scan
                        for index in (prev_commit_index + 1)..=commit_index {
                            if let Some(entry) = self.log.get(index)? {
                                self.state_tx.send(Instruction::Apply { entry })?
                            }
                        }
                    }
                    self.send(msg.from, Event::ConfirmLeader { commit_index, has_committed })?;
                }
            }

            Event::SolicitVote { last_index, last_term } => {
                if let Some(voted_for) = &self.role.voted_for {
                    if msg.from != Address::Peer(voted_for.clone()) {
                        return Ok(self.into());
                    }
                }
                let (local_last_index, local_last_term) = self.log.get_last();
                if last_term < local_last_term {
                    return Ok(self.into());
                }
                if last_term == local_last_term && last_index < local_last_index {
                    return Ok(self.into());
                }
                if let Address::Peer(from) = msg.from {
                    info!("Voting for {} in term {} election", from, self.term);
                    self.send(Address::Peer(from.clone()), Event::GrantVote)?;
                    self.save_term(self.term, Some(&from))?;
                    self.role.voted_for = Some(from);
                }
            }

            Event::ReplicateEntries { base_index, base_term, entries } => {
                if self.is_leader(&msg.from) {
                    if base_index > 0 && !self.log.has(base_index, base_term)? {
                        debug!("Rejecting log entries at base {}", base_index);
                        self.send(msg.from, Event::RejectEntries)?
                    } else {
                        let last_index = self.log.splice(entries)?;
                        self.send(msg.from, Event::AcceptEntries { last_index })?
                    }
                }
            }

            Event::ClientRequest { ref id, .. } => {
                if let Some(leader) = self.role.leader.as_deref() {
                    self.proxied_reqs.insert(id.clone(), msg.from);
                    self.send(Address::Peer(leader.to_string()), msg.event)?
                } else {
                    self.queued_reqs.push((msg.from, msg.event));
                }
            }

            Event::ClientResponse { id, mut response } => {
                if let Ok(Response::Status(ref mut status)) = response {
                    status.id = self.id.clone();
                    status.role = "follower".into();
                }
                self.proxied_reqs.remove(&id);
                self.send(Address::Client, Event::ClientResponse { id, response })?;
            }

            // Ignore votes which are usually strays from the previous election that we lost.
            Event::GrantVote => {}

            Event::ConfirmLeader { .. }
            | Event::AcceptEntries { .. }
            | Event::RejectEntries { .. } => warn!("Received unexpected message {:?}", msg),
        };
        Ok(self.into())
    }

    /// Processes a logical clock tick.
    pub fn tick(mut self) -> Result<Node<L>, Error> {
        self.role.leader_seen_ticks += 1;
        if self.role.leader_seen_ticks >= self.role.leader_seen_timeout {
            Ok(self.become_candidate()?.into())
        } else {
            Ok(self.into())
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::super::super::{Entry, Log, Request};
    use super::super::tests::{assert_messages, assert_node};
    use super::*;
    use crate::kv;
    use std::collections::HashMap;
    use tokio::sync::mpsc;

    pub fn follower_leader<L: Storage>(node: &RoleNode<Follower, L>) -> Option<String> {
        node.role.leader.clone()
    }

    pub fn follower_voted_for<L: Storage>(node: &RoleNode<Follower, L>) -> Option<String> {
        node.role.voted_for.clone()
    }

    #[allow(clippy::type_complexity)]
    fn setup() -> Result<
        (
            RoleNode<Follower, kv::storage::Test>,
            mpsc::UnboundedReceiver<Message>,
            mpsc::UnboundedReceiver<Instruction>,
        ),
        Error,
    > {
        let (node_tx, node_rx) = mpsc::unbounded_channel();
        let (state_tx, state_rx) = mpsc::unbounded_channel();
        let mut log = Log::new(kv::Simple::new(kv::storage::Test::new()))?;
        log.append(1, Some(vec![0x01]))?;
        log.append(1, Some(vec![0x02]))?;
        log.append(2, Some(vec![0x03]))?;
        log.commit(2)?;

        let mut node = RoleNode {
            id: "a".into(),
            peers: vec!["b".into(), "c".into(), "d".into(), "e".into()],
            term: 3,
            log,
            node_tx,
            state_tx,
            proxied_reqs: HashMap::new(),
            queued_reqs: Vec::new(),
            role: Follower::new(Some("b"), None),
        };
        node.save_term(3, None)?;
        Ok((node, node_rx, state_rx))
    }

    #[test]
    // Heartbeat from current leader should commit and apply
    fn step_heartbeat() -> Result<(), Error> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let node = follower.step(Message {
            from: Address::Peer("b".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::Heartbeat { commit_index: 3, commit_term: 2 },
        })?;
        assert_node(&node).is_follower().term(3).leader(Some("b")).voted_for(None).committed(3);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Peer("b".into()),
                term: 3,
                event: Event::ConfirmLeader { commit_index: 3, has_committed: true },
            }],
        );
        assert_messages(
            &mut state_rx,
            vec![Instruction::Apply {
                entry: Entry { index: 3, term: 2, command: Some(vec![0x03]) },
            }],
        );
        Ok(())
    }

    #[test]
    // Heartbeat from current leader with conflicting commit_term
    fn step_heartbeat_conflict_commit_term() -> Result<(), Error> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let node = follower.step(Message {
            from: Address::Peer("b".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::Heartbeat { commit_index: 3, commit_term: 3 },
        })?;
        assert_node(&node).is_follower().term(3).leader(Some("b")).voted_for(None).committed(2);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Peer("b".into()),
                term: 3,
                event: Event::ConfirmLeader { commit_index: 3, has_committed: false },
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // Heartbeat from current leader with a missing commit_index
    fn step_heartbeat_missing_commit_entry() -> Result<(), Error> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let node = follower.step(Message {
            from: Address::Peer("b".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::Heartbeat { commit_index: 5, commit_term: 3 },
        })?;
        assert_node(&node).is_follower().term(3).leader(Some("b")).voted_for(None).committed(2);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Peer("b".into()),
                term: 3,
                event: Event::ConfirmLeader { commit_index: 5, has_committed: false },
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // Heartbeat from fake leader
    fn step_heartbeat_fake_leader() -> Result<(), Error> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let node = follower.step(Message {
            from: Address::Peer("c".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::Heartbeat { commit_index: 5, commit_term: 3 },
        })?;
        assert_node(&node).is_follower().term(3).leader(Some("b")).voted_for(None).committed(2);
        assert_messages(&mut node_rx, vec![]);
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // Heartbeat when no current leader makes us follow the leader
    fn step_heartbeat_no_leader() -> Result<(), Error> {
        let (mut follower, mut node_rx, mut state_rx) = setup()?;
        follower.role = Follower::new(None, None);
        let node = follower.step(Message {
            from: Address::Peer("c".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::Heartbeat { commit_index: 3, commit_term: 2 },
        })?;
        assert_node(&node).is_follower().term(3).leader(Some("c")).voted_for(None).committed(3);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Peer("c".into()),
                term: 3,
                event: Event::ConfirmLeader { commit_index: 3, has_committed: true },
            }],
        );
        assert_messages(
            &mut state_rx,
            vec![Instruction::Apply {
                entry: Entry { index: 3, term: 2, command: Some(vec![0x03]) },
            }],
        );
        Ok(())
    }

    #[test]
    // Heartbeat from current leader with old commit_index
    fn step_heartbeat_old_commit_index() -> Result<(), Error> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let node = follower.step(Message {
            from: Address::Peer("b".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::Heartbeat { commit_index: 1, commit_term: 1 },
        })?;
        assert_node(&node).is_follower().term(3).leader(Some("b")).voted_for(None).committed(2);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Peer("b".into()),
                term: 3,
                event: Event::ConfirmLeader { commit_index: 1, has_committed: true },
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // Heartbeat for future term with other leader changes leader
    fn step_heartbeat_future_term() -> Result<(), Error> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let node = follower.step(Message {
            from: Address::Peer("c".into()),
            to: Address::Peer("a".into()),
            term: 4,
            event: Event::Heartbeat { commit_index: 3, commit_term: 2 },
        })?;
        assert_node(&node).is_follower().term(4).leader(Some("c")).voted_for(None);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Peer("c".into()),
                term: 4,
                event: Event::ConfirmLeader { commit_index: 3, has_committed: true },
            }],
        );
        assert_messages(
            &mut state_rx,
            vec![Instruction::Apply {
                entry: Entry { index: 3, term: 2, command: Some(vec![0x03]) },
            }],
        );
        Ok(())
    }

    #[test]
    // Heartbeat from past term
    fn step_heartbeat_past_term() -> Result<(), Error> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let node = follower.step(Message {
            from: Address::Peer("b".into()),
            to: Address::Peer("a".into()),
            term: 2,
            event: Event::Heartbeat { commit_index: 3, commit_term: 2 },
        })?;
        assert_node(&node).is_follower().term(3).leader(Some("b")).voted_for(None).committed(2);
        assert_messages(&mut node_rx, vec![]);
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // SolicitVote is granted for the first solicitor, otherwise ignored.
    fn step_solicitvote() -> Result<(), Error> {
        let (follower, mut node_rx, mut state_rx) = setup()?;

        // The first vote request in this term yields a vote response.
        let mut node = follower.step(Message {
            from: Address::Peer("c".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::SolicitVote { last_index: 3, last_term: 2 },
        })?;
        assert_node(&node).is_follower().term(3).leader(Some("b")).voted_for(Some("c"));
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Peer("c".into()),
                term: 3,
                event: Event::GrantVote,
            }],
        );
        assert_messages(&mut state_rx, vec![]);

        // Another vote request from the same sender is granted.
        node = node.step(Message {
            from: Address::Peer("c".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::SolicitVote { last_index: 3, last_term: 2 },
        })?;
        assert_node(&node).is_follower().term(3).leader(Some("b")).voted_for(Some("c"));
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Peer("c".into()),
                term: 3,
                event: Event::GrantVote,
            }],
        );
        assert_messages(&mut state_rx, vec![]);

        // But a vote request from a different node is ignored.
        node = node.step(Message {
            from: Address::Peer("d".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::SolicitVote { last_index: 3, last_term: 2 },
        })?;
        assert_node(&node).is_follower().term(3).leader(Some("b")).voted_for(Some("c"));
        assert_messages(&mut node_rx, vec![]);
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // GrantVote messages are ignored
    fn step_grantvote_noop() -> Result<(), Error> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let node = follower.step(Message {
            from: Address::Peer("b".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::GrantVote,
        })?;
        assert_node(&node).is_follower().term(3).leader(Some("b"));
        assert_messages(&mut node_rx, vec![]);
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // SolicitVote is rejected if last_term is outdated.
    fn step_solicitvote_last_index_outdated() -> Result<(), Error> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let node = follower.step(Message {
            from: Address::Peer("c".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::SolicitVote { last_index: 2, last_term: 2 },
        })?;
        assert_node(&node).is_follower().term(3).leader(Some("b")).voted_for(None);
        assert_messages(&mut node_rx, vec![]);
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // SolicitVote is rejected if last_term is outdated.
    fn step_solicitvote_last_term_outdated() -> Result<(), Error> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let node = follower.step(Message {
            from: Address::Peer("c".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::SolicitVote { last_index: 3, last_term: 1 },
        })?;
        assert_node(&node).is_follower().term(3).leader(Some("b")).voted_for(None);
        assert_messages(&mut node_rx, vec![]);
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // ReplicateEntries accepts some entries at base 0 without changes
    fn step_replicateentries_base0() -> Result<(), Error> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let node = follower.step(Message {
            from: Address::Peer("b".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::ReplicateEntries {
                base_index: 0,
                base_term: 0,
                entries: vec![
                    Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                    Entry { index: 2, term: 1, command: Some(vec![0x02]) },
                ],
            },
        })?;
        assert_node(&node).is_follower().term(3).entries(vec![
            Entry { index: 1, term: 1, command: Some(vec![0x01]) },
            Entry { index: 2, term: 1, command: Some(vec![0x02]) },
            Entry { index: 3, term: 2, command: Some(vec![0x03]) },
        ]);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Peer("b".into()),
                term: 3,
                event: Event::AcceptEntries { last_index: 3 },
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // ReplicateEntries appends entries but does not commit them
    fn step_replicateentries_append() -> Result<(), Error> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let node = follower.step(Message {
            from: Address::Peer("b".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::ReplicateEntries {
                base_index: 3,
                base_term: 2,
                entries: vec![
                    Entry { index: 4, term: 3, command: Some(vec![0x04]) },
                    Entry { index: 5, term: 3, command: Some(vec![0x05]) },
                ],
            },
        })?;
        assert_node(&node).is_follower().term(3).entries(vec![
            Entry { index: 1, term: 1, command: Some(vec![0x01]) },
            Entry { index: 2, term: 1, command: Some(vec![0x02]) },
            Entry { index: 3, term: 2, command: Some(vec![0x03]) },
            Entry { index: 4, term: 3, command: Some(vec![0x04]) },
            Entry { index: 5, term: 3, command: Some(vec![0x05]) },
        ]);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Peer("b".into()),
                term: 3,
                event: Event::AcceptEntries { last_index: 5 },
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // ReplicateEntries accepts partially overlapping entries
    fn step_replicateentries_partial_overlap() -> Result<(), Error> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let node = follower.step(Message {
            from: Address::Peer("b".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::ReplicateEntries {
                base_index: 1,
                base_term: 1,
                entries: vec![
                    Entry { index: 2, term: 1, command: Some(vec![0x02]) },
                    Entry { index: 3, term: 2, command: Some(vec![0x03]) },
                    Entry { index: 4, term: 3, command: Some(vec![0x04]) },
                ],
            },
        })?;
        assert_node(&node).is_follower().term(3).entries(vec![
            Entry { index: 1, term: 1, command: Some(vec![0x01]) },
            Entry { index: 2, term: 1, command: Some(vec![0x02]) },
            Entry { index: 3, term: 2, command: Some(vec![0x03]) },
            Entry { index: 4, term: 3, command: Some(vec![0x04]) },
        ]);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Peer("b".into()),
                term: 3,
                event: Event::AcceptEntries { last_index: 4 },
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // ReplicateEntries replaces conflicting entries
    fn step_replicateentries_replace() -> Result<(), Error> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let node = follower.step(Message {
            from: Address::Peer("b".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::ReplicateEntries {
                base_index: 2,
                base_term: 1,
                entries: vec![
                    Entry { index: 3, term: 3, command: Some(vec![0x04]) },
                    Entry { index: 4, term: 3, command: Some(vec![0x05]) },
                ],
            },
        })?;
        assert_node(&node).is_follower().term(3).entries(vec![
            Entry { index: 1, term: 1, command: Some(vec![0x01]) },
            Entry { index: 2, term: 1, command: Some(vec![0x02]) },
            Entry { index: 3, term: 3, command: Some(vec![0x04]) },
            Entry { index: 4, term: 3, command: Some(vec![0x05]) },
        ]);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Peer("b".into()),
                term: 3,
                event: Event::AcceptEntries { last_index: 4 },
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // ReplicateEntries replaces partially conflicting entries
    fn step_replicateentries_replace_partial() -> Result<(), Error> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let node = follower.step(Message {
            from: Address::Peer("b".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::ReplicateEntries {
                base_index: 2,
                base_term: 1,
                entries: vec![
                    Entry { index: 3, term: 2, command: Some(vec![0x03]) },
                    Entry { index: 4, term: 3, command: Some(vec![0x04]) },
                ],
            },
        })?;
        assert_node(&node).is_follower().term(3).entries(vec![
            Entry { index: 1, term: 1, command: Some(vec![0x01]) },
            Entry { index: 2, term: 1, command: Some(vec![0x02]) },
            Entry { index: 3, term: 2, command: Some(vec![0x03]) },
            Entry { index: 4, term: 3, command: Some(vec![0x04]) },
        ]);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Peer("b".into()),
                term: 3,
                event: Event::AcceptEntries { last_index: 4 },
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // ReplicateEntries rejects missing base index
    fn step_replicateentries_reject_missing_base_index() -> Result<(), Error> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let node = follower.step(Message {
            from: Address::Peer("b".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::ReplicateEntries {
                base_index: 5,
                base_term: 2,
                entries: vec![Entry { index: 6, term: 3, command: Some(vec![0x04]) }],
            },
        })?;
        assert_node(&node).is_follower().term(3).entries(vec![
            Entry { index: 1, term: 1, command: Some(vec![0x01]) },
            Entry { index: 2, term: 1, command: Some(vec![0x02]) },
            Entry { index: 3, term: 2, command: Some(vec![0x03]) },
        ]);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Peer("b".into()),
                term: 3,
                event: Event::RejectEntries,
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // ReplicateEntries rejects conflicting base term
    fn step_replicateentries_reject_missing_base_term() -> Result<(), Error> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let node = follower.step(Message {
            from: Address::Peer("b".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::ReplicateEntries {
                base_index: 1,
                base_term: 2,
                entries: vec![Entry { index: 2, term: 3, command: Some(vec![0x04]) }],
            },
        })?;
        assert_node(&node).is_follower().term(3).entries(vec![
            Entry { index: 1, term: 1, command: Some(vec![0x01]) },
            Entry { index: 2, term: 1, command: Some(vec![0x02]) },
            Entry { index: 3, term: 2, command: Some(vec![0x03]) },
        ]);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Peer("b".into()),
                term: 3,
                event: Event::RejectEntries,
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // ClientRequest is proxied, as is the response.
    fn step_clientrequest_clientresponse() -> Result<(), Error> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let mut node = Node::Follower(follower);

        node = node.step(Message {
            from: Address::Client,
            to: Address::Local,
            term: 0,
            event: Event::ClientRequest { id: vec![0x01], request: Request::Mutate(vec![0xaf]) },
        })?;
        assert_node(&node)
            .is_follower()
            .term(3)
            .leader(Some("b"))
            .proxied(vec![(vec![0x01], Address::Client)])
            .queued(vec![]);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Peer("b".into()),
                term: 3,
                event: Event::ClientRequest {
                    id: vec![0x01],
                    request: Request::Mutate(vec![0xaf]),
                },
            }],
        );
        assert_messages(&mut state_rx, vec![]);

        node = node.step(Message {
            from: Address::Peer("b".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::ClientResponse {
                id: vec![0x01],
                response: Ok(Response::State(vec![0xaf])),
            },
        })?;
        assert_node(&node).is_follower().term(3).leader(Some("b")).proxied(vec![]).queued(vec![]);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Client,
                term: 3,
                event: Event::ClientResponse {
                    id: vec![0x01],
                    response: Ok(Response::State(vec![0xaf])),
                },
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // ClientRequest is queued when there is no leader, and forwarded when a leader appears.
    fn step_clientrequest_queued() -> Result<(), Error> {
        let (mut follower, mut node_rx, mut state_rx) = setup()?;
        follower.role = Follower::new(None, None);
        let mut node = Node::Follower(follower);

        node = node.step(Message {
            from: Address::Client,
            to: Address::Local,
            term: 0,
            event: Event::ClientRequest { id: vec![0x01], request: Request::Mutate(vec![0xaf]) },
        })?;
        assert_node(&node).is_follower().term(3).leader(None).proxied(vec![]).queued(vec![(
            Address::Client,
            Event::ClientRequest { id: vec![0x01], request: Request::Mutate(vec![0xaf]) },
        )]);
        assert_messages(&mut node_rx, vec![]);
        assert_messages(&mut state_rx, vec![]);

        // When a leader appears, we will proxy the queued request to them.
        node = node.step(Message {
            from: Address::Peer("c".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::Heartbeat { commit_index: 3, commit_term: 2 },
        })?;
        assert_node(&node)
            .is_follower()
            .term(3)
            .leader(Some("c"))
            .proxied(vec![(vec![0x01], Address::Client)])
            .queued(vec![]);
        assert_messages(
            &mut node_rx,
            vec![
                Message {
                    from: Address::Local,
                    to: Address::Peer("c".into()),
                    term: 0,
                    event: Event::ClientRequest {
                        id: vec![0x01],
                        request: Request::Mutate(vec![0xaf]),
                    },
                },
                Message {
                    from: Address::Local,
                    to: Address::Peer("c".into()),
                    term: 3,
                    event: Event::ConfirmLeader { commit_index: 3, has_committed: true },
                },
            ],
        );
        assert_messages(
            &mut state_rx,
            vec![Instruction::Apply {
                entry: Entry { index: 3, term: 2, command: Some(vec![0x03]) },
            }],
        );
        Ok(())
    }

    // ClientRequest is proxied, but aborted when a new leader appears.
    #[test]
    fn step_clientrequest_aborted() -> Result<(), Error> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let mut node = Node::Follower(follower);

        node = node.step(Message {
            from: Address::Client,
            to: Address::Local,
            term: 0,
            event: Event::ClientRequest { id: vec![0x01], request: Request::Mutate(vec![0xaf]) },
        })?;
        assert_node(&node)
            .is_follower()
            .term(3)
            .leader(Some("b"))
            .proxied(vec![(vec![0x01], Address::Client)])
            .queued(vec![]);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Peer("b".into()),
                term: 3,
                event: Event::ClientRequest {
                    id: vec![0x01],
                    request: Request::Mutate(vec![0xaf]),
                },
            }],
        );
        assert_messages(&mut state_rx, vec![]);

        // When a new leader appears, the proxied request is aborted.
        node = node.step(Message {
            from: Address::Peer("c".into()),
            to: Address::Peer("a".into()),
            term: 4,
            event: Event::Heartbeat { commit_index: 3, commit_term: 2 },
        })?;
        assert_node(&node).is_follower().term(4).leader(Some("c")).proxied(vec![]).queued(vec![]);
        assert_messages(
            &mut node_rx,
            vec![
                Message {
                    from: Address::Local,
                    to: Address::Client,
                    term: 4,
                    event: Event::ClientResponse { id: vec![0x01], response: Err(Error::Abort) },
                },
                Message {
                    from: Address::Local,
                    to: Address::Peer("c".into()),
                    term: 4,
                    event: Event::ConfirmLeader { commit_index: 3, has_committed: true },
                },
            ],
        );
        assert_messages(
            &mut state_rx,
            vec![Instruction::Apply {
                entry: Entry { index: 3, term: 2, command: Some(vec![0x03]) },
            }],
        );
        Ok(())
    }

    #[test]
    fn tick() -> Result<(), Error> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let timeout = follower.role.leader_seen_timeout;
        let mut node = Node::Follower(follower);

        // Make sure heartbeats reset election timeout
        assert!(timeout > 0);
        for _ in 0..(3 * timeout) {
            assert_node(&node).is_follower().term(3).leader(Some("b"));
            node = node.tick()?;
            node = node.step(Message {
                from: Address::Peer("b".into()),
                to: Address::Peer("a".into()),
                term: 3,
                event: Event::Heartbeat { commit_index: 2, commit_term: 1 },
            })?;
            assert_messages(
                &mut node_rx,
                vec![Message {
                    from: Address::Local,
                    to: Address::Peer("b".into()),
                    term: 3,
                    event: Event::ConfirmLeader { commit_index: 2, has_committed: true },
                }],
            )
        }

        for _ in 0..timeout {
            assert_node(&node).is_follower().term(3).leader(Some("b"));
            node = node.tick()?;
        }
        assert_node(&node).is_candidate().term(4);

        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Peers,
                term: 4,
                event: Event::SolicitVote { last_index: 3, last_term: 2 },
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }
}
