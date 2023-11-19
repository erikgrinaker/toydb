use super::super::{Address, Event, Instruction, Message, Response};
use super::{Candidate, Node, NodeID, RoleNode, Term, ELECTION_TIMEOUT_MAX, ELECTION_TIMEOUT_MIN};
use crate::error::{Error, Result};

use ::log::{debug, error, info, warn};
use rand::Rng as _;

// A follower replicates state from a leader.
#[derive(Debug)]
pub struct Follower {
    /// The leader, or None if just initialized.
    leader: Option<NodeID>,
    /// The number of ticks since the last message from the leader.
    leader_seen_ticks: u64,
    /// The timeout before triggering an election.
    leader_seen_timeout: u64,
    /// The node we voted for in the current term, if any.
    voted_for: Option<NodeID>,
}

impl Follower {
    /// Creates a new follower role.
    pub fn new(leader: Option<NodeID>, voted_for: Option<NodeID>) -> Self {
        Self {
            leader,
            voted_for,
            leader_seen_ticks: 0,
            leader_seen_timeout: rand::thread_rng()
                .gen_range(ELECTION_TIMEOUT_MIN..=ELECTION_TIMEOUT_MAX),
        }
    }
}

impl RoleNode<Follower> {
    /// Transforms the node into a candidate.
    fn become_candidate(self) -> Result<RoleNode<Candidate>> {
        info!("Starting election for term {}", self.term + 1);
        let (last_index, last_term) = self.log.get_last_index();
        let mut node = self.become_role(Candidate::new());
        node.term += 1;
        node.log.set_term(node.term, Some(node.id))?;
        node.send(Address::Broadcast, Event::SolicitVote { last_index, last_term })?;
        Ok(node)
    }

    /// Transforms the node into a follower, either a leaderless follower in a
    /// new term or following a leader in the current term.
    fn become_follower(mut self, leader: Option<NodeID>, term: Term) -> Result<RoleNode<Follower>> {
        assert!(term >= self.term, "Term regression {} -> {}", self.term, term);

        if let Some(leader) = leader {
            // We found a leader in the current term.
            assert_eq!(self.role.leader, None, "Already have leader in term");
            assert_eq!(term, self.term, "Can't follow leader in different term");
            info!("Following leader {} in term {}", leader, term);
            self.role = Follower::new(Some(leader), self.role.voted_for);
        } else {
            // We found a new term, but we don't necessarily know who the leader
            // is yet. We'll find out when we step a message from it.
            assert_ne!(term, self.term, "Can't become leaderless follower in current term");
            info!("Discovered new term {}", term);
            self.term = term;
            self.log.set_term(term, None)?;
            self.role = Follower::new(None, None);
        }
        // Abort any proxied requests.
        //
        // TODO: Move this into the new term branch, and assert that there are
        // no proxied requests in the new leader branch.
        self.abort_proxied()?;
        Ok(self)
    }

    /// Checks if an address is the current leader.
    fn is_leader(&self, from: &Address) -> bool {
        if let Some(leader) = &self.role.leader {
            if let Address::Node(from) = from {
                return leader == from;
            }
        }
        false
    }

    /// Processes a message.
    pub fn step(mut self, msg: Message) -> Result<Node> {
        // Drop invalid messages and messages from past terms.
        if let Err(err) = self.validate(&msg) {
            error!("Invalid message: {} ({:?})", err, msg);
            return Ok(self.into());
        }
        if msg.term < self.term && msg.term > 0 {
            debug!("Dropping message from past term ({:?})", msg);
            return Ok(self.into());
        }

        // If we receive a message for a future term, become a leaderless
        // follower in it and step the message. If the message is a Heartbeat or
        // AppendEntries from the leader, stepping it will follow the leader.
        if msg.term > self.term {
            return self.become_follower(None, msg.term)?.step(msg);
        }

        // Record when we last saw a message from the leader (if any).
        if self.is_leader(&msg.from) {
            self.role.leader_seen_ticks = 0
        }

        match msg.event {
            // The leader will send periodic heartbeats. If we don't have a
            // leader in this term yet, follow it. If the commit_index advances,
            // apply state transitions.
            Event::Heartbeat { commit_index, commit_term } => {
                // Check that the heartbeat is from our leader.
                let from = msg.from.unwrap();
                match self.role.leader {
                    Some(leader) => assert_eq!(from, leader, "Multiple leaders in term"),
                    None => self = self.become_follower(Some(from), msg.term)?,
                }

                // Advance commit index and apply entries if possible.
                let has_committed = self.log.has(commit_index, commit_term)?;
                let (old_commit_index, _) = self.log.get_commit_index();
                if has_committed && commit_index > old_commit_index {
                    self.log.commit(commit_index)?;
                    let mut scan = self.log.scan((old_commit_index + 1)..=commit_index)?;
                    while let Some(entry) = scan.next().transpose()? {
                        self.state_tx.send(Instruction::Apply { entry })?;
                    }
                }
                self.send(msg.from, Event::ConfirmLeader { commit_index, has_committed })?;
            }

            // Replicate entries from the leader. If we don't have a leader in
            // this term yet, follow it.
            Event::AppendEntries { base_index, base_term, entries } => {
                // Check that the entries are from our leader.
                let from = msg.from.unwrap();
                match self.role.leader {
                    Some(leader) => assert_eq!(from, leader, "Multiple leaders in term"),
                    None => self = self.become_follower(Some(from), msg.term)?,
                }

                // Append the entries, if possible.
                if base_index > 0 && !self.log.has(base_index, base_term)? {
                    debug!("Rejecting log entries at base {}", base_index);
                    self.send(msg.from, Event::RejectEntries)?
                } else {
                    let last_index = self.log.splice(entries)?;
                    self.send(msg.from, Event::AcceptEntries { last_index })?
                }
            }

            Event::SolicitVote { last_index, last_term } => {
                if let Some(voted_for) = self.role.voted_for {
                    if msg.from != Address::Node(voted_for) {
                        return Ok(self.into());
                    }
                }
                let (log_last_index, log_last_term) = self.log.get_last_index();
                if last_term < log_last_term {
                    return Ok(self.into());
                }
                if last_term == log_last_term && last_index < log_last_index {
                    return Ok(self.into());
                }
                if let Address::Node(from) = msg.from {
                    info!("Voting for {} in term {} election", from, self.term);
                    self.send(Address::Node(from), Event::GrantVote)?;
                    self.log.set_term(self.term, Some(from))?;
                    self.role.voted_for = Some(from);
                }
            }

            // Forward requests to the leader, or abort them if there is none.
            Event::ClientRequest { ref id, .. } => {
                let id = id.clone();
                if let Some(leader) = self.role.leader {
                    self.proxied_reqs.insert(id, msg.from);
                    self.send(Address::Node(leader), msg.event)?
                } else {
                    self.send(msg.from, Event::ClientResponse { id, response: Err(Error::Abort) })?
                }
            }

            Event::ClientResponse { id, mut response } => {
                if let Ok(Response::Status(ref mut status)) = response {
                    status.server = self.id;
                }
                self.proxied_reqs.remove(&id);
                self.send(Address::Client, Event::ClientResponse { id, response })?;
            }

            // Ignore votes which are usually strays from the previous election that we lost.
            Event::GrantVote => {}

            // We're not a leader in this term, so we shoudn't see these.
            Event::ConfirmLeader { .. }
            | Event::AcceptEntries { .. }
            | Event::RejectEntries { .. } => warn!("Received unexpected message {:?}", msg),
        };
        Ok(self.into())
    }

    /// Processes a logical clock tick.
    pub fn tick(mut self) -> Result<Node> {
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
    use crate::error::Error;
    use crate::storage;
    use std::collections::HashMap;
    use tokio::sync::mpsc;

    pub fn follower_leader(node: &RoleNode<Follower>) -> Option<NodeID> {
        node.role.leader
    }

    pub fn follower_voted_for(node: &RoleNode<Follower>) -> Option<NodeID> {
        node.role.voted_for
    }

    #[allow(clippy::type_complexity)]
    fn setup() -> Result<(
        RoleNode<Follower>,
        mpsc::UnboundedReceiver<Message>,
        mpsc::UnboundedReceiver<Instruction>,
    )> {
        let (node_tx, node_rx) = mpsc::unbounded_channel();
        let (state_tx, state_rx) = mpsc::unbounded_channel();
        let mut log = Log::new(Box::new(storage::engine::Memory::new()), false)?;
        log.append(1, Some(vec![0x01]))?;
        log.append(1, Some(vec![0x02]))?;
        log.append(2, Some(vec![0x03]))?;
        log.commit(2)?;
        log.set_term(3, None)?;

        let node = RoleNode {
            id: 1,
            peers: vec![2, 3, 4, 5],
            term: 3,
            log,
            node_tx,
            state_tx,
            proxied_reqs: HashMap::new(),
            role: Follower::new(Some(2), None),
        };
        Ok((node, node_rx, state_rx))
    }

    #[test]
    // Heartbeat from current leader should commit and apply
    fn step_heartbeat() -> Result<()> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let mut node = follower.step(Message {
            from: Address::Node(2),
            to: Address::Node(1),
            term: 3,
            event: Event::Heartbeat { commit_index: 3, commit_term: 2 },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(2)).voted_for(None).committed(3);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Node(1),
                to: Address::Node(2),
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
    fn step_heartbeat_conflict_commit_term() -> Result<()> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let mut node = follower.step(Message {
            from: Address::Node(2),
            to: Address::Node(1),
            term: 3,
            event: Event::Heartbeat { commit_index: 3, commit_term: 3 },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(2)).voted_for(None).committed(2);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Node(1),
                to: Address::Node(2),
                term: 3,
                event: Event::ConfirmLeader { commit_index: 3, has_committed: false },
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // Heartbeat from current leader with a missing commit_index
    fn step_heartbeat_missing_commit_entry() -> Result<()> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let mut node = follower.step(Message {
            from: Address::Node(2),
            to: Address::Node(1),
            term: 3,
            event: Event::Heartbeat { commit_index: 5, commit_term: 3 },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(2)).voted_for(None).committed(2);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Node(1),
                to: Address::Node(2),
                term: 3,
                event: Event::ConfirmLeader { commit_index: 5, has_committed: false },
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    #[should_panic(expected = "Multiple leaders in term")]
    // Heartbeat from other leader should panic.
    fn step_heartbeat_fake_leader() {
        let (follower, _, _) = setup().unwrap();
        follower
            .step(Message {
                from: Address::Node(3),
                to: Address::Node(1),
                term: 3,
                event: Event::Heartbeat { commit_index: 5, commit_term: 3 },
            })
            .unwrap();
    }

    #[test]
    // Heartbeat when no current leader makes us follow the leader
    fn step_heartbeat_no_leader() -> Result<()> {
        let (mut follower, mut node_rx, mut state_rx) = setup()?;
        follower.role = Follower::new(None, None);
        let mut node = follower.step(Message {
            from: Address::Node(3),
            to: Address::Node(1),
            term: 3,
            event: Event::Heartbeat { commit_index: 3, commit_term: 2 },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(3)).voted_for(None).committed(3);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Node(1),
                to: Address::Node(3),
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
    fn step_heartbeat_old_commit_index() -> Result<()> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let mut node = follower.step(Message {
            from: Address::Node(2),
            to: Address::Node(1),
            term: 3,
            event: Event::Heartbeat { commit_index: 1, commit_term: 1 },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(2)).voted_for(None).committed(2);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Node(1),
                to: Address::Node(2),
                term: 3,
                event: Event::ConfirmLeader { commit_index: 1, has_committed: true },
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // Heartbeat for future term with other leader changes leader
    fn step_heartbeat_future_term() -> Result<()> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let mut node = follower.step(Message {
            from: Address::Node(3),
            to: Address::Node(1),
            term: 4,
            event: Event::Heartbeat { commit_index: 3, commit_term: 2 },
        })?;
        assert_node(&mut node).is_follower().term(4).leader(Some(3)).voted_for(None);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Node(1),
                to: Address::Node(3),
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
    fn step_heartbeat_past_term() -> Result<()> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let mut node = follower.step(Message {
            from: Address::Node(2),
            to: Address::Node(1),
            term: 2,
            event: Event::Heartbeat { commit_index: 3, commit_term: 2 },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(2)).voted_for(None).committed(2);
        assert_messages(&mut node_rx, vec![]);
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // SolicitVote is granted for the first solicitor, otherwise ignored.
    fn step_solicitvote() -> Result<()> {
        let (follower, mut node_rx, mut state_rx) = setup()?;

        // The first vote request in this term yields a vote response.
        let mut node = follower.step(Message {
            from: Address::Node(3),
            to: Address::Node(1),
            term: 3,
            event: Event::SolicitVote { last_index: 3, last_term: 2 },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(2)).voted_for(Some(3));
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Node(1),
                to: Address::Node(3),
                term: 3,
                event: Event::GrantVote,
            }],
        );
        assert_messages(&mut state_rx, vec![]);

        // Another vote request from the same sender is granted.
        node = node.step(Message {
            from: Address::Node(3),
            to: Address::Node(1),
            term: 3,
            event: Event::SolicitVote { last_index: 3, last_term: 2 },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(2)).voted_for(Some(3));
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Node(1),
                to: Address::Node(3),
                term: 3,
                event: Event::GrantVote,
            }],
        );
        assert_messages(&mut state_rx, vec![]);

        // But a vote request from a different node is ignored.
        node = node.step(Message {
            from: Address::Node(4),
            to: Address::Node(1),
            term: 3,
            event: Event::SolicitVote { last_index: 3, last_term: 2 },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(2)).voted_for(Some(3));
        assert_messages(&mut node_rx, vec![]);
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // GrantVote messages are ignored
    fn step_grantvote_noop() -> Result<()> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let mut node = follower.step(Message {
            from: Address::Node(2),
            to: Address::Node(1),
            term: 3,
            event: Event::GrantVote,
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(2));
        assert_messages(&mut node_rx, vec![]);
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // SolicitVote is rejected if last_term is outdated.
    fn step_solicitvote_last_index_outdated() -> Result<()> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let mut node = follower.step(Message {
            from: Address::Node(3),
            to: Address::Node(1),
            term: 3,
            event: Event::SolicitVote { last_index: 2, last_term: 2 },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(2)).voted_for(None);
        assert_messages(&mut node_rx, vec![]);
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // SolicitVote is rejected if last_term is outdated.
    fn step_solicitvote_last_term_outdated() -> Result<()> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let mut node = follower.step(Message {
            from: Address::Node(3),
            to: Address::Node(1),
            term: 3,
            event: Event::SolicitVote { last_index: 3, last_term: 1 },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(2)).voted_for(None);
        assert_messages(&mut node_rx, vec![]);
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // AppendEntries accepts some entries at base 0 without changes
    fn step_appendentries_base0() -> Result<()> {
        // TODO: Move this into a setup function.
        let (node_tx, mut node_rx) = mpsc::unbounded_channel();
        let (state_tx, mut state_rx) = mpsc::unbounded_channel();
        let mut log = Log::new(Box::new(storage::engine::Memory::new()), false)?;
        log.append(1, Some(vec![0x01]))?;
        log.append(1, Some(vec![0x02]))?;
        log.append(2, Some(vec![0x03]))?;

        let follower = RoleNode {
            id: 1,
            peers: vec![2, 3, 4, 5],
            term: 0,
            log,
            node_tx,
            state_tx,
            proxied_reqs: HashMap::new(),
            role: Follower::new(Some(2), None),
        };

        let mut node = follower.step(Message {
            from: Address::Node(2),
            to: Address::Node(1),
            term: 3,
            event: Event::AppendEntries {
                base_index: 0,
                base_term: 0,
                entries: vec![
                    Entry { index: 1, term: 1, command: Some(vec![0x01]) },
                    Entry { index: 2, term: 1, command: Some(vec![0x02]) },
                ],
            },
        })?;
        assert_node(&mut node).is_follower().term(3).entries(vec![
            Entry { index: 1, term: 1, command: Some(vec![0x01]) },
            Entry { index: 2, term: 1, command: Some(vec![0x02]) },
        ]);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Node(1),
                to: Address::Node(2),
                term: 3,
                event: Event::AcceptEntries { last_index: 2 },
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // AppendEntries appends entries but does not commit them
    fn step_appendentries_append() -> Result<()> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let mut node = follower.step(Message {
            from: Address::Node(2),
            to: Address::Node(1),
            term: 3,
            event: Event::AppendEntries {
                base_index: 3,
                base_term: 2,
                entries: vec![
                    Entry { index: 4, term: 3, command: Some(vec![0x04]) },
                    Entry { index: 5, term: 3, command: Some(vec![0x05]) },
                ],
            },
        })?;
        assert_node(&mut node).is_follower().term(3).entries(vec![
            Entry { index: 1, term: 1, command: Some(vec![0x01]) },
            Entry { index: 2, term: 1, command: Some(vec![0x02]) },
            Entry { index: 3, term: 2, command: Some(vec![0x03]) },
            Entry { index: 4, term: 3, command: Some(vec![0x04]) },
            Entry { index: 5, term: 3, command: Some(vec![0x05]) },
        ]);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Node(1),
                to: Address::Node(2),
                term: 3,
                event: Event::AcceptEntries { last_index: 5 },
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // AppendEntries accepts partially overlapping entries
    fn step_appendentries_partial_overlap() -> Result<()> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let mut node = follower.step(Message {
            from: Address::Node(2),
            to: Address::Node(1),
            term: 3,
            event: Event::AppendEntries {
                base_index: 1,
                base_term: 1,
                entries: vec![
                    Entry { index: 3, term: 2, command: Some(vec![0x03]) },
                    Entry { index: 4, term: 3, command: Some(vec![0x04]) },
                ],
            },
        })?;
        assert_node(&mut node).is_follower().term(3).entries(vec![
            Entry { index: 1, term: 1, command: Some(vec![0x01]) },
            Entry { index: 2, term: 1, command: Some(vec![0x02]) },
            Entry { index: 3, term: 2, command: Some(vec![0x03]) },
            Entry { index: 4, term: 3, command: Some(vec![0x04]) },
        ]);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Node(1),
                to: Address::Node(2),
                term: 3,
                event: Event::AcceptEntries { last_index: 4 },
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // AppendEntries replaces conflicting entries
    fn step_appendentries_replace() -> Result<()> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let mut node = follower.step(Message {
            from: Address::Node(2),
            to: Address::Node(1),
            term: 3,
            event: Event::AppendEntries {
                base_index: 2,
                base_term: 1,
                entries: vec![
                    Entry { index: 3, term: 3, command: Some(vec![0x04]) },
                    Entry { index: 4, term: 3, command: Some(vec![0x05]) },
                ],
            },
        })?;
        assert_node(&mut node).is_follower().term(3).entries(vec![
            Entry { index: 1, term: 1, command: Some(vec![0x01]) },
            Entry { index: 2, term: 1, command: Some(vec![0x02]) },
            Entry { index: 3, term: 3, command: Some(vec![0x04]) },
            Entry { index: 4, term: 3, command: Some(vec![0x05]) },
        ]);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Node(1),
                to: Address::Node(2),
                term: 3,
                event: Event::AcceptEntries { last_index: 4 },
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // AppendEntries replaces partially conflicting entries
    fn step_appendentries_replace_partial() -> Result<()> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let mut node = follower.step(Message {
            from: Address::Node(2),
            to: Address::Node(1),
            term: 3,
            event: Event::AppendEntries {
                base_index: 2,
                base_term: 1,
                entries: vec![
                    Entry { index: 3, term: 2, command: Some(vec![0x03]) },
                    Entry { index: 4, term: 3, command: Some(vec![0x04]) },
                ],
            },
        })?;
        assert_node(&mut node).is_follower().term(3).entries(vec![
            Entry { index: 1, term: 1, command: Some(vec![0x01]) },
            Entry { index: 2, term: 1, command: Some(vec![0x02]) },
            Entry { index: 3, term: 2, command: Some(vec![0x03]) },
            Entry { index: 4, term: 3, command: Some(vec![0x04]) },
        ]);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Node(1),
                to: Address::Node(2),
                term: 3,
                event: Event::AcceptEntries { last_index: 4 },
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // AppendEntries rejects missing base index
    fn step_appendentries_reject_missing_base_index() -> Result<()> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let mut node = follower.step(Message {
            from: Address::Node(2),
            to: Address::Node(1),
            term: 3,
            event: Event::AppendEntries {
                base_index: 5,
                base_term: 2,
                entries: vec![Entry { index: 6, term: 3, command: Some(vec![0x04]) }],
            },
        })?;
        assert_node(&mut node).is_follower().term(3).entries(vec![
            Entry { index: 1, term: 1, command: Some(vec![0x01]) },
            Entry { index: 2, term: 1, command: Some(vec![0x02]) },
            Entry { index: 3, term: 2, command: Some(vec![0x03]) },
        ]);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Node(1),
                to: Address::Node(2),
                term: 3,
                event: Event::RejectEntries,
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // AppendEntries rejects conflicting base term
    fn step_appendentries_reject_missing_base_term() -> Result<()> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let mut node = follower.step(Message {
            from: Address::Node(2),
            to: Address::Node(1),
            term: 3,
            event: Event::AppendEntries {
                base_index: 1,
                base_term: 2,
                entries: vec![Entry { index: 2, term: 3, command: Some(vec![0x04]) }],
            },
        })?;
        assert_node(&mut node).is_follower().term(3).entries(vec![
            Entry { index: 1, term: 1, command: Some(vec![0x01]) },
            Entry { index: 2, term: 1, command: Some(vec![0x02]) },
            Entry { index: 3, term: 2, command: Some(vec![0x03]) },
        ]);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Node(1),
                to: Address::Node(2),
                term: 3,
                event: Event::RejectEntries,
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // ClientRequest is proxied, as is the response.
    fn step_clientrequest_clientresponse() -> Result<()> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let mut node = Node::Follower(follower);

        node = node.step(Message {
            from: Address::Client,
            to: Address::Node(1),
            term: 0,
            event: Event::ClientRequest { id: vec![0x01], request: Request::Mutate(vec![0xaf]) },
        })?;
        assert_node(&mut node)
            .is_follower()
            .term(3)
            .leader(Some(2))
            .proxied(vec![(vec![0x01], Address::Client)]);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Node(1),
                to: Address::Node(2),
                term: 3,
                event: Event::ClientRequest {
                    id: vec![0x01],
                    request: Request::Mutate(vec![0xaf]),
                },
            }],
        );
        assert_messages(&mut state_rx, vec![]);

        node = node.step(Message {
            from: Address::Node(2),
            to: Address::Node(1),
            term: 3,
            event: Event::ClientResponse {
                id: vec![0x01],
                response: Ok(Response::Mutate(vec![0xaf])),
            },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(2)).proxied(vec![]);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Node(1),
                to: Address::Client,
                term: 3,
                event: Event::ClientResponse {
                    id: vec![0x01],
                    response: Ok(Response::Mutate(vec![0xaf])),
                },
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // ClientRequest returns Error::Abort when there is no leader.
    fn step_clientrequest_no_leader() -> Result<()> {
        let (mut follower, mut node_rx, mut state_rx) = setup()?;
        follower.role = Follower::new(None, None);
        let mut node = Node::Follower(follower);

        node = node.step(Message {
            from: Address::Client,
            to: Address::Node(1),
            term: 0,
            event: Event::ClientRequest { id: vec![0x01], request: Request::Mutate(vec![0xaf]) },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(None).proxied(vec![]);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Node(1),
                to: Address::Client,
                term: 3,
                event: Event::ClientResponse { id: vec![0x01], response: Err(Error::Abort) },
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    // ClientRequest is proxied, but aborted when a new leader appears.
    #[test]
    fn step_clientrequest_aborted() -> Result<()> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let mut node = Node::Follower(follower);

        node = node.step(Message {
            from: Address::Client,
            to: Address::Node(1),
            term: 0,
            event: Event::ClientRequest { id: vec![0x01], request: Request::Mutate(vec![0xaf]) },
        })?;
        assert_node(&mut node)
            .is_follower()
            .term(3)
            .leader(Some(2))
            .proxied(vec![(vec![0x01], Address::Client)]);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Node(1),
                to: Address::Node(2),
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
            from: Address::Node(3),
            to: Address::Node(1),
            term: 4,
            event: Event::Heartbeat { commit_index: 3, commit_term: 2 },
        })?;
        assert_node(&mut node).is_follower().term(4).leader(Some(3)).proxied(vec![]);
        assert_messages(
            &mut node_rx,
            vec![
                Message {
                    from: Address::Node(1),
                    to: Address::Client,
                    term: 4,
                    event: Event::ClientResponse { id: vec![0x01], response: Err(Error::Abort) },
                },
                Message {
                    from: Address::Node(1),
                    to: Address::Node(3),
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
    fn tick() -> Result<()> {
        let (follower, mut node_rx, mut state_rx) = setup()?;
        let timeout = follower.role.leader_seen_timeout;
        let mut node = Node::Follower(follower);

        // Make sure heartbeats reset election timeout
        assert!(timeout > 0);
        for _ in 0..(3 * timeout) {
            assert_node(&mut node).is_follower().term(3).leader(Some(2));
            node = node.tick()?;
            node = node.step(Message {
                from: Address::Node(2),
                to: Address::Node(1),
                term: 3,
                event: Event::Heartbeat { commit_index: 2, commit_term: 1 },
            })?;
            assert_messages(
                &mut node_rx,
                vec![Message {
                    from: Address::Node(1),
                    to: Address::Node(2),
                    term: 3,
                    event: Event::ConfirmLeader { commit_index: 2, has_committed: true },
                }],
            )
        }

        for _ in 0..timeout {
            assert_node(&mut node).is_follower().term(3).leader(Some(2));
            node = node.tick()?;
        }
        assert_node(&mut node).is_candidate().term(4);

        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Node(1),
                to: Address::Broadcast,
                term: 4,
                event: Event::SolicitVote { last_index: 3, last_term: 2 },
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }
}
