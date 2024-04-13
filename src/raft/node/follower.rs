use super::super::{Envelope, Log, Message, RequestID, State};
use super::{rand_election_timeout, Candidate, Node, NodeID, RawNode, Role, Term, Ticks};
use crate::error::{Error, Result};

use ::log::{debug, info};
use std::collections::HashSet;

// A follower replicates state from a leader.
#[derive(Clone, Debug, PartialEq)]
pub struct Follower {
    /// The leader, or None if just initialized.
    pub(super) leader: Option<NodeID>,
    /// The number of ticks since the last message from the leader.
    leader_seen: Ticks,
    /// The leader_seen timeout before triggering an election.
    election_timeout: Ticks,
    /// The node we voted for in the current term, if any.
    pub(super) voted_for: Option<NodeID>,
    // Local client requests that have been forwarded to the leader. These are
    // aborted on leader/term changes.
    pub(super) forwarded: HashSet<RequestID>,
}

impl Follower {
    /// Creates a new follower role.
    pub fn new(leader: Option<NodeID>, voted_for: Option<NodeID>) -> Self {
        Self {
            leader,
            voted_for,
            leader_seen: 0,
            election_timeout: rand_election_timeout(),
            forwarded: HashSet::new(),
        }
    }
}

impl Role for Follower {}

impl RawNode<Follower> {
    /// Creates a new node as a leaderless follower.
    pub fn new(
        id: NodeID,
        peers: HashSet<NodeID>,
        mut log: Log,
        state: Box<dyn State>,
        node_tx: crossbeam::channel::Sender<Envelope>,
    ) -> Result<Self> {
        let (term, voted_for) = log.get_term()?;
        let role = Follower::new(None, voted_for);
        Ok(Self { id, peers, term, log, state, node_tx, role })
    }

    /// Asserts internal invariants.
    fn assert(&mut self) -> Result<()> {
        self.assert_node()?;

        if let Some(leader) = self.role.leader {
            assert_ne!(leader, self.id, "Can't follow self");
            assert!(self.peers.contains(&leader), "Leader not in peers");
            assert_ne!(self.term, 0, "Followers with leaders can't have term 0");
        } else {
            assert!(self.role.forwarded.is_empty(), "Leaderless follower has forwarded requests");
        }

        // NB: We allow voted_for not in peers, since this can happen when
        // removing nodes from the cluster via a cold restart. We also allow
        // voted_for self, which can happen if we lose an election.

        debug_assert_eq!(self.role.voted_for, self.log.get_term()?.1, "Vote does not match log");
        assert!(self.role.leader_seen < self.role.election_timeout, "Election timeout passed");

        Ok(())
    }

    /// Transitions the follower into a candidate, by campaigning for
    /// leadership in a new term.
    pub(super) fn into_candidate(mut self) -> Result<RawNode<Candidate>> {
        // Abort any forwarded requests. These must be retried with new leader.
        self.abort_forwarded()?;

        // Apply any pending log entries, so that we're caught up if we win.
        self.maybe_apply()?;

        let mut node = self.into_role(Candidate::new());
        node.campaign()?;
        Ok(node)
    }

    /// Transitions the candidate into a follower, either a leaderless follower
    /// in a new term (e.g. if someone holds a new election) or following a
    /// leader in the current term once someone wins the election.
    pub(super) fn into_follower(
        mut self,
        leader: Option<NodeID>,
        term: Term,
    ) -> Result<RawNode<Follower>> {
        assert!(term >= self.term, "Term regression {} -> {}", self.term, term);

        // Abort any forwarded requests. These must be retried with new leader.
        self.abort_forwarded()?;

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
        Ok(self)
    }

    /// Processes a message.
    pub fn step(mut self, msg: Envelope) -> Result<Node> {
        self.assert()?;
        self.assert_step(&msg);

        // Drop messages from past terms.
        if msg.term < self.term {
            debug!("Dropping message from past term ({:?})", msg);
            return Ok(self.into());
        }

        // If we receive a message for a future term, become a leaderless
        // follower in it and step the message. If the message is a Heartbeat or
        // AppendEntries from the leader, stepping it will follow the leader.
        if msg.term > self.term {
            return self.into_follower(None, msg.term)?.step(msg);
        }

        // Record when we last saw a message from the leader (if any).
        if self.is_leader(msg.from) {
            self.role.leader_seen = 0
        }

        match msg.message {
            // The leader will send periodic heartbeats. If we don't have a
            // leader in this term yet, follow it. If the commit_index advances,
            // apply state transitions.
            Message::Heartbeat { commit_index, commit_term, read_seq } => {
                // Check that the heartbeat is from our leader.
                match self.role.leader {
                    Some(leader) => assert_eq!(msg.from, leader, "Multiple leaders in term"),
                    None => self = self.into_follower(Some(msg.from), msg.term)?,
                }

                // Respond to the heartbeat.
                let (last_index, last_term) = self.log.get_last_index();
                self.send(
                    msg.from,
                    Message::HeartbeatResponse { last_index, last_term, read_seq },
                )?;

                // Advance commit index and apply entries.
                if self.log.has(commit_index, commit_term)?
                    && commit_index > self.log.get_commit_index().0
                {
                    self.log.commit(commit_index)?;
                    self.maybe_apply()?;
                }
            }

            // Replicate entries from the leader. If we don't have a leader in
            // this term yet, follow it.
            Message::Append { base_index, base_term, entries } => {
                // Check that the entries are from our leader.
                let from = msg.from;
                match self.role.leader {
                    Some(leader) => assert_eq!(from, leader, "Multiple leaders in term"),
                    None => self = self.into_follower(Some(from), msg.term)?,
                }

                // Append the entries, if possible.
                let reject = base_index > 0 && !self.log.has(base_index, base_term)?;
                if !reject {
                    self.log.splice(entries)?;
                }
                let (last_index, last_term) = self.log.get_last_index();
                self.send(msg.from, Message::AppendResponse { reject, last_index, last_term })?;
            }

            // A candidate in this term is requesting our vote.
            Message::Campaign { last_index, last_term } => {
                // Don't vote if we already voted for someone else in this term.
                if let Some(voted_for) = self.role.voted_for {
                    if msg.from != voted_for {
                        self.send(msg.from, Message::CampaignResponse { vote: false })?;
                        return Ok(self.into());
                    }
                }

                // Don't vote if our log is newer than the candidate's log.
                let (log_index, log_term) = self.log.get_last_index();
                if log_term > last_term || log_term == last_term && log_index > last_index {
                    self.send(msg.from, Message::CampaignResponse { vote: false })?;
                    return Ok(self.into());
                }

                // Grant the vote.
                info!("Voting for {} in term {} election", msg.from, self.term);
                self.send(msg.from, Message::CampaignResponse { vote: true })?;
                self.log.set_term(self.term, Some(msg.from))?;
                self.role.voted_for = Some(msg.from);
            }

            // We may receive a vote after we lost an election and followed a
            // different leader. Ignore it.
            Message::CampaignResponse { .. } => {}

            // Forward client requests to the leader, or abort them if there is
            // none (the client must retry).
            Message::ClientRequest { ref id, .. } => {
                assert_eq!(msg.from, self.id, "Client request from other node");

                let id = id.clone();
                if let Some(leader) = self.role.leader {
                    debug!("Forwarding request to leader {}: {:?}", leader, msg);
                    self.role.forwarded.insert(id);
                    self.send(leader, msg.message)?
                } else {
                    self.send(
                        msg.from,
                        Message::ClientResponse { id, response: Err(Error::Abort) },
                    )?
                }
            }

            // Returns client responses for forwarded requests.
            Message::ClientResponse { id, response } => {
                assert!(self.is_leader(msg.from), "Client response from non-leader");

                if self.role.forwarded.remove(&id) {
                    self.send(self.id, Message::ClientResponse { id, response })?;
                }
            }

            // We're not a leader nor candidate in this term, so we shoudn't see these.
            Message::HeartbeatResponse { .. } | Message::AppendResponse { .. } => {
                panic!("Received unexpected message {msg:?}")
            }
        };
        Ok(self.into())
    }

    /// Processes a logical clock tick.
    pub fn tick(mut self) -> Result<Node> {
        self.assert()?;

        self.role.leader_seen += 1;
        if self.role.leader_seen >= self.role.election_timeout {
            return Ok(self.into_candidate()?.into());
        }
        Ok(self.into())
    }

    /// Aborts all forwarded requests.
    fn abort_forwarded(&mut self) -> Result<()> {
        for id in std::mem::take(&mut self.role.forwarded) {
            debug!("Aborting forwarded request {:x?}", id);
            self.send(self.id, Message::ClientResponse { id, response: Err(Error::Abort) })?;
        }
        Ok(())
    }

    /// Checks if an address is the current leader.
    fn is_leader(&self, from: NodeID) -> bool {
        self.role.leader == Some(from)
    }
}

#[cfg(test)]
pub mod tests {
    use super::super::super::state::tests::TestState;
    use super::super::super::{Entry, Log, Request, Response};
    use super::super::tests::{assert_messages, assert_node};
    use super::*;
    use crate::error::Error;
    use crate::storage;
    use itertools::Itertools as _;

    #[allow(clippy::type_complexity)]
    fn setup() -> Result<(RawNode<Follower>, crossbeam::channel::Receiver<Envelope>)> {
        let (node_tx, node_rx) = crossbeam::channel::unbounded();
        let state = Box::new(TestState::new(0));
        let mut log = Log::new(storage::Memory::new(), false)?;
        log.append(1, Some(vec![0x01]))?;
        log.append(1, Some(vec![0x02]))?;
        log.append(2, Some(vec![0x03]))?;
        log.commit(2)?;
        log.set_term(3, None)?;

        let node = RawNode {
            id: 1,
            peers: HashSet::from([2, 3, 4, 5]),
            term: 3,
            log,
            state,
            node_tx,
            role: Follower::new(Some(2), None),
        };
        Ok((node, node_rx))
    }

    #[test]
    // Heartbeat from current leader should commit and apply
    fn step_heartbeat() -> Result<()> {
        let (follower, mut node_rx) = setup()?;
        let mut node = follower.step(Envelope {
            from: 2,
            to: 1,
            term: 3,
            message: Message::Heartbeat { commit_index: 3, commit_term: 2, read_seq: 7 },
        })?;
        assert_node(&mut node)
            .is_follower()
            .term(3)
            .leader(Some(2))
            .voted_for(None)
            .committed(3)
            .applied(3);
        assert_messages(
            &mut node_rx,
            vec![Envelope {
                from: 1,
                to: 2,
                term: 3,
                message: Message::HeartbeatResponse { last_index: 3, last_term: 2, read_seq: 7 },
            }],
        );
        Ok(())
    }

    #[test]
    // Heartbeat from current leader with conflicting commit_term
    fn step_heartbeat_conflict_commit_term() -> Result<()> {
        let (follower, mut node_rx) = setup()?;
        let mut node = follower.step(Envelope {
            from: 2,
            to: 1,
            term: 3,
            message: Message::Heartbeat { commit_index: 3, commit_term: 3, read_seq: 7 },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(2)).voted_for(None).committed(2);
        assert_messages(
            &mut node_rx,
            vec![Envelope {
                from: 1,
                to: 2,
                term: 3,
                message: Message::HeartbeatResponse { last_index: 3, last_term: 2, read_seq: 7 },
            }],
        );
        Ok(())
    }

    #[test]
    // Heartbeat from current leader with a missing commit_index
    fn step_heartbeat_missing_commit_entry() -> Result<()> {
        let (follower, mut node_rx) = setup()?;
        let mut node = follower.step(Envelope {
            from: 2,
            to: 1,
            term: 3,
            message: Message::Heartbeat { commit_index: 5, commit_term: 3, read_seq: 7 },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(2)).voted_for(None).committed(2);
        assert_messages(
            &mut node_rx,
            vec![Envelope {
                from: 1,
                to: 2,
                term: 3,
                message: Message::HeartbeatResponse { last_index: 3, last_term: 2, read_seq: 7 },
            }],
        );
        Ok(())
    }

    #[test]
    #[should_panic(expected = "Multiple leaders in term")]
    // Heartbeat from other leader should panic.
    fn step_heartbeat_fake_leader() {
        let (follower, _) = setup().unwrap();
        follower
            .step(Envelope {
                from: 3,
                to: 1,
                term: 3,
                message: Message::Heartbeat { commit_index: 5, commit_term: 3, read_seq: 7 },
            })
            .unwrap();
    }

    #[test]
    // Heartbeat when no current leader makes us follow the leader
    fn step_heartbeat_no_leader() -> Result<()> {
        let (mut follower, mut node_rx) = setup()?;
        follower.role = Follower::new(None, None);
        let mut node = follower.step(Envelope {
            from: 3,
            to: 1,
            term: 3,
            message: Message::Heartbeat { commit_index: 3, commit_term: 2, read_seq: 7 },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(3)).voted_for(None).committed(3);
        assert_messages(
            &mut node_rx,
            vec![Envelope {
                from: 1,
                to: 3,
                term: 3,
                message: Message::HeartbeatResponse { last_index: 3, last_term: 2, read_seq: 7 },
            }],
        );
        Ok(())
    }

    #[test]
    // Heartbeat from current leader with old commit_index
    fn step_heartbeat_old_commit_index() -> Result<()> {
        let (follower, mut node_rx) = setup()?;
        let mut node = follower.step(Envelope {
            from: 2,
            to: 1,
            term: 3,
            message: Message::Heartbeat { commit_index: 1, commit_term: 1, read_seq: 7 },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(2)).voted_for(None).committed(2);
        assert_messages(
            &mut node_rx,
            vec![Envelope {
                from: 1,
                to: 2,
                term: 3,
                message: Message::HeartbeatResponse { last_index: 3, last_term: 2, read_seq: 7 },
            }],
        );
        Ok(())
    }

    #[test]
    // Heartbeat for future term with other leader changes leader
    fn step_heartbeat_future_term() -> Result<()> {
        let (follower, mut node_rx) = setup()?;
        let mut node = follower.step(Envelope {
            from: 3,
            to: 1,
            term: 4,
            message: Message::Heartbeat { commit_index: 3, commit_term: 2, read_seq: 7 },
        })?;
        assert_node(&mut node).is_follower().term(4).leader(Some(3)).voted_for(None);
        assert_messages(
            &mut node_rx,
            vec![Envelope {
                from: 1,
                to: 3,
                term: 4,
                message: Message::HeartbeatResponse { last_index: 3, last_term: 2, read_seq: 7 },
            }],
        );
        Ok(())
    }

    #[test]
    // Heartbeat from past term
    fn step_heartbeat_past_term() -> Result<()> {
        let (follower, mut node_rx) = setup()?;
        let mut node = follower.step(Envelope {
            from: 2,
            to: 1,
            term: 2,
            message: Message::Heartbeat { commit_index: 3, commit_term: 2, read_seq: 7 },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(2)).voted_for(None).committed(2);
        assert_messages(&mut node_rx, vec![]);
        Ok(())
    }

    #[test]
    // SolicitVote is granted for the first solicitor, otherwise ignored.
    fn step_solicitvote() -> Result<()> {
        let (follower, mut node_rx) = setup()?;

        // The first vote request in this term yields a vote response.
        let mut node = follower.step(Envelope {
            from: 3,
            to: 1,
            term: 3,
            message: Message::Campaign { last_index: 3, last_term: 2 },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(2)).voted_for(Some(3));
        assert_messages(
            &mut node_rx,
            vec![Envelope {
                from: 1,
                to: 3,
                term: 3,
                message: Message::CampaignResponse { vote: true },
            }],
        );

        // Another vote request from the same sender is granted.
        node = node.step(Envelope {
            from: 3,
            to: 1,
            term: 3,
            message: Message::Campaign { last_index: 3, last_term: 2 },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(2)).voted_for(Some(3));
        assert_messages(
            &mut node_rx,
            vec![Envelope {
                from: 1,
                to: 3,
                term: 3,
                message: Message::CampaignResponse { vote: true },
            }],
        );

        // But a vote request from a different node is ignored.
        node = node.step(Envelope {
            from: 4,
            to: 1,
            term: 3,
            message: Message::Campaign { last_index: 3, last_term: 2 },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(2)).voted_for(Some(3));
        assert_messages(
            &mut node_rx,
            vec![Envelope {
                from: 1,
                to: 4,
                term: 3,
                message: Message::CampaignResponse { vote: false },
            }],
        );
        Ok(())
    }

    #[test]
    // Vote messages are ignored.
    fn step_grantvote_noop() -> Result<()> {
        let (follower, mut node_rx) = setup()?;
        let mut node = follower.step(Envelope {
            from: 2,
            to: 1,
            term: 3,
            message: Message::CampaignResponse { vote: true },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(2));
        assert_messages(&mut node_rx, vec![]);
        Ok(())
    }

    #[test]
    // SolicitVote is rejected if last_term is outdated.
    fn step_solicitvote_last_index_outdated() -> Result<()> {
        let (follower, mut node_rx) = setup()?;
        let mut node = follower.step(Envelope {
            from: 3,
            to: 1,
            term: 3,
            message: Message::Campaign { last_index: 2, last_term: 2 },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(2)).voted_for(None);
        assert_messages(
            &mut node_rx,
            vec![Envelope {
                from: 1,
                to: 3,
                term: 3,
                message: Message::CampaignResponse { vote: false },
            }],
        );
        Ok(())
    }

    #[test]
    // SolicitVote is rejected if last_term is outdated.
    fn step_solicitvote_last_term_outdated() -> Result<()> {
        let (follower, mut node_rx) = setup()?;
        let mut node = follower.step(Envelope {
            from: 3,
            to: 1,
            term: 3,
            message: Message::Campaign { last_index: 3, last_term: 1 },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(2)).voted_for(None);
        assert_messages(
            &mut node_rx,
            vec![Envelope {
                from: 1,
                to: 3,
                term: 3,
                message: Message::CampaignResponse { vote: false },
            }],
        );
        Ok(())
    }

    #[test]
    // AppendEntries accepts some entries at base 0 without changes
    fn step_appendentries_base0() -> Result<()> {
        // TODO: Move this into a setup function.
        let (node_tx, mut node_rx) = crossbeam::channel::unbounded();
        let mut log = Log::new(storage::Memory::new(), false)?;
        log.append(1, Some(vec![0x01]))?;
        log.append(1, Some(vec![0x02]))?;
        log.append(2, Some(vec![0x03]))?;
        log.set_term(1, None)?;

        let follower = RawNode {
            id: 1,
            peers: HashSet::from([2, 3, 4, 5]),
            term: 1,
            log,
            state: Box::new(TestState::new(0)),
            node_tx,
            role: Follower::new(Some(2), None),
        };

        let mut node = follower.step(Envelope {
            from: 2,
            to: 1,
            term: 3,
            message: Message::Append {
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
            vec![Envelope {
                from: 1,
                to: 2,
                term: 3,
                message: Message::AppendResponse { reject: false, last_index: 2, last_term: 1 },
            }],
        );
        Ok(())
    }

    #[test]
    // AppendEntries appends entries but does not commit them
    fn step_appendentries_append() -> Result<()> {
        let (follower, mut node_rx) = setup()?;
        let mut node = follower.step(Envelope {
            from: 2,
            to: 1,
            term: 3,
            message: Message::Append {
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
            vec![Envelope {
                from: 1,
                to: 2,
                term: 3,
                message: Message::AppendResponse { reject: false, last_index: 5, last_term: 3 },
            }],
        );
        Ok(())
    }

    #[test]
    // AppendEntries accepts partially overlapping entries
    fn step_appendentries_partial_overlap() -> Result<()> {
        let (follower, mut node_rx) = setup()?;
        let mut node = follower.step(Envelope {
            from: 2,
            to: 1,
            term: 3,
            message: Message::Append {
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
            vec![Envelope {
                from: 1,
                to: 2,
                term: 3,
                message: Message::AppendResponse { reject: false, last_index: 4, last_term: 3 },
            }],
        );
        Ok(())
    }

    #[test]
    // AppendEntries replaces conflicting entries
    fn step_appendentries_replace() -> Result<()> {
        let (follower, mut node_rx) = setup()?;
        let mut node = follower.step(Envelope {
            from: 2,
            to: 1,
            term: 3,
            message: Message::Append {
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
            vec![Envelope {
                from: 1,
                to: 2,
                term: 3,
                message: Message::AppendResponse { reject: false, last_index: 4, last_term: 3 },
            }],
        );
        Ok(())
    }

    #[test]
    // AppendEntries replaces partially conflicting entries
    fn step_appendentries_replace_partial() -> Result<()> {
        let (follower, mut node_rx) = setup()?;
        let mut node = follower.step(Envelope {
            from: 2,
            to: 1,
            term: 3,
            message: Message::Append {
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
            vec![Envelope {
                from: 1,
                to: 2,
                term: 3,
                message: Message::AppendResponse { reject: false, last_index: 4, last_term: 3 },
            }],
        );
        Ok(())
    }

    #[test]
    // AppendEntries rejects missing base index
    fn step_appendentries_reject_missing_base_index() -> Result<()> {
        let (follower, mut node_rx) = setup()?;
        let mut node = follower.step(Envelope {
            from: 2,
            to: 1,
            term: 3,
            message: Message::Append {
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
            vec![Envelope {
                from: 1,
                to: 2,
                term: 3,
                message: Message::AppendResponse { reject: true, last_index: 3, last_term: 2 },
            }],
        );
        Ok(())
    }

    #[test]
    // AppendEntries rejects conflicting base term
    fn step_appendentries_reject_missing_base_term() -> Result<()> {
        let (follower, mut node_rx) = setup()?;
        let mut node = follower.step(Envelope {
            from: 2,
            to: 1,
            term: 3,
            message: Message::Append {
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
            vec![Envelope {
                from: 1,
                to: 2,
                term: 3,
                message: Message::AppendResponse { reject: true, last_index: 3, last_term: 2 },
            }],
        );
        Ok(())
    }

    #[test]
    // ClientRequest is forwarded, as is the response.
    fn step_clientrequest_clientresponse() -> Result<()> {
        let (follower, mut node_rx) = setup()?;
        let mut node = Node::Follower(follower);

        node = node.step(Envelope {
            from: 1,
            to: 1,
            term: 3,
            message: Message::ClientRequest { id: vec![0x01], request: Request::Write(vec![0xaf]) },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(2)).forwarded(vec![vec![0x01]]);
        assert_messages(
            &mut node_rx,
            vec![Envelope {
                from: 1,
                to: 2,
                term: 3,
                message: Message::ClientRequest {
                    id: vec![0x01],
                    request: Request::Write(vec![0xaf]),
                },
            }],
        );

        node = node.step(Envelope {
            from: 2,
            to: 1,
            term: 3,
            message: Message::ClientResponse {
                id: vec![0x01],
                response: Ok(Response::Write(vec![0xaf])),
            },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(2)).forwarded(vec![]);
        assert_messages(
            &mut node_rx,
            vec![Envelope {
                from: 1,
                to: 1,
                term: 3,
                message: Message::ClientResponse {
                    id: vec![0x01],
                    response: Ok(Response::Write(vec![0xaf])),
                },
            }],
        );
        Ok(())
    }

    #[test]
    // ClientRequest returns Error::Abort when there is no leader.
    fn step_clientrequest_no_leader() -> Result<()> {
        let (mut follower, mut node_rx) = setup()?;
        follower.role = Follower::new(None, None);
        let mut node = Node::Follower(follower);

        node = node.step(Envelope {
            from: 1,
            to: 1,
            term: 3,
            message: Message::ClientRequest { id: vec![0x01], request: Request::Write(vec![0xaf]) },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(None).forwarded(vec![]);
        assert_messages(
            &mut node_rx,
            vec![Envelope {
                from: 1,
                to: 1,
                term: 3,
                message: Message::ClientResponse { id: vec![0x01], response: Err(Error::Abort) },
            }],
        );
        Ok(())
    }

    // ClientRequest is forwarded, but aborted when a new leader appears.
    #[test]
    fn step_clientrequest_aborted() -> Result<()> {
        let (follower, mut node_rx) = setup()?;
        let mut node = Node::Follower(follower);

        node = node.step(Envelope {
            from: 1,
            to: 1,
            term: 3,
            message: Message::ClientRequest { id: vec![0x01], request: Request::Write(vec![0xaf]) },
        })?;
        assert_node(&mut node).is_follower().term(3).leader(Some(2)).forwarded(vec![vec![0x01]]);
        assert_messages(
            &mut node_rx,
            vec![Envelope {
                from: 1,
                to: 2,
                term: 3,
                message: Message::ClientRequest {
                    id: vec![0x01],
                    request: Request::Write(vec![0xaf]),
                },
            }],
        );

        // When a new leader appears, the proxied request is aborted.
        node = node.step(Envelope {
            from: 3,
            to: 1,
            term: 4,
            message: Message::Heartbeat { commit_index: 3, commit_term: 2, read_seq: 7 },
        })?;
        assert_node(&mut node).is_follower().term(4).leader(Some(3)).forwarded(vec![]);
        assert_messages(
            &mut node_rx,
            vec![
                Envelope {
                    from: 1,
                    to: 1,
                    term: 3,
                    message: Message::ClientResponse {
                        id: vec![0x01],
                        response: Err(Error::Abort),
                    },
                },
                Envelope {
                    from: 1,
                    to: 3,
                    term: 4,
                    message: Message::HeartbeatResponse {
                        last_index: 3,
                        last_term: 2,
                        read_seq: 7,
                    },
                },
            ],
        );
        Ok(())
    }

    #[test]
    fn tick() -> Result<()> {
        let (follower, mut node_rx) = setup()?;
        let peers = follower.peers.clone();
        let timeout = follower.role.election_timeout;
        let mut node = Node::Follower(follower);

        // Make sure heartbeats reset election timeout
        assert!(timeout > 0);
        for _ in 0..(3 * timeout) {
            assert_node(&mut node).is_follower().term(3).leader(Some(2));
            node = node.tick()?;
            node = node.step(Envelope {
                from: 2,
                to: 1,
                term: 3,
                message: Message::Heartbeat { commit_index: 2, commit_term: 1, read_seq: 7 },
            })?;
            assert_messages(
                &mut node_rx,
                vec![Envelope {
                    from: 1,
                    to: 2,
                    term: 3,
                    message: Message::HeartbeatResponse {
                        last_index: 3,
                        last_term: 2,
                        read_seq: 7,
                    },
                }],
            )
        }

        for _ in 0..timeout {
            assert_node(&mut node).is_follower().term(3).leader(Some(2));
            node = node.tick()?;
        }
        assert_node(&mut node).is_candidate().term(4);

        for to in peers.iter().copied().sorted() {
            assert_eq!(
                node_rx.try_recv()?,
                Envelope {
                    from: 1,
                    to,
                    term: 4,
                    message: Message::Campaign { last_index: 3, last_term: 2 },
                },
            );
        }
        Ok(())
    }
}
