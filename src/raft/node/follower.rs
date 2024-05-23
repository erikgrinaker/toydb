use super::super::{Envelope, Log, Message, RequestID, State};
use super::{Candidate, Node, NodeID, RawNode, Role, Term, Ticks};
use crate::error::{Error, Result};

use ::log::{debug, info};
use itertools::Itertools as _;
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
    pub fn new(leader: Option<NodeID>, voted_for: Option<NodeID>, election_timeout: Ticks) -> Self {
        Self { leader, voted_for, leader_seen: 0, election_timeout, forwarded: HashSet::new() }
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
        heartbeat_interval: Ticks,
        election_timeout_range: std::ops::Range<Ticks>,
    ) -> Result<Self> {
        let (term, voted_for) = log.get_term()?;
        let role = Follower::new(None, voted_for, 0);
        let mut node = Self {
            id,
            peers,
            term,
            log,
            state,
            node_tx,
            heartbeat_interval,
            election_timeout_range,
            role,
        };
        node.role.election_timeout = node.gen_election_timeout();
        Ok(node)
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

        let election_timeout = self.gen_election_timeout();
        let mut node = self.into_role(Candidate::new(election_timeout));
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
            self.role =
                Follower::new(Some(leader), self.role.voted_for, self.role.election_timeout);
        } else {
            // We found a new term, but we don't necessarily know who the leader
            // is yet. We'll find out when we step a message from it.
            assert_ne!(term, self.term, "Can't become leaderless follower in current term");
            info!("Discovered new term {}", term);
            self.term = term;
            self.log.set_term(term, None)?;
            self.role = Follower::new(None, None, self.gen_election_timeout());
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
                    Some(leader) => assert_eq!(msg.from, leader, "multiple leaders in term"),
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
                    Some(leader) => assert_eq!(from, leader, "multiple leaders in term"),
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
        // Sort the IDs for test determinism.
        for id in std::mem::take(&mut self.role.forwarded).into_iter().sorted() {
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
