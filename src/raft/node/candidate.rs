use super::super::{Envelope, Message};
use super::{Follower, Leader, Node, NodeID, RawNode, Role, Term, Ticks};
use crate::error::{Error, Result};

use ::log::{debug, info};
use std::collections::HashSet;

/// A candidate is campaigning to become a leader.
#[derive(Clone, Debug, PartialEq)]
pub struct Candidate {
    /// Votes received (including ourself).
    votes: HashSet<NodeID>,
    /// Ticks elapsed since election start.
    election_duration: Ticks,
    /// Election timeout, in ticks.
    election_timeout: Ticks,
}

impl Candidate {
    /// Creates a new candidate role.
    pub fn new(election_timeout: Ticks) -> Self {
        Self { votes: HashSet::new(), election_duration: 0, election_timeout }
    }
}

impl Role for Candidate {}

impl RawNode<Candidate> {
    /// Asserts internal invariants.
    fn assert(&mut self) -> Result<()> {
        self.assert_node()?;

        assert_ne!(self.term, 0, "Candidates can't have term 0");
        assert!(self.role.votes.contains(&self.id), "Candidate did not vote for self");
        debug_assert_eq!(Some(self.id), self.log.get_term()?.1, "Log vote does not match self");

        assert!(
            self.role.election_duration < self.role.election_timeout,
            "Election timeout passed"
        );

        Ok(())
    }

    /// Transitions the candidate to a follower. We either lost the election and
    /// follow the winner, or we discovered a new term in which case we step
    /// into it as a leaderless follower.
    pub(super) fn into_follower(
        mut self,
        term: Term,
        leader: Option<NodeID>,
    ) -> Result<RawNode<Follower>> {
        assert!(term >= self.term, "Term regression {} -> {}", self.term, term);

        let election_timeout = self.gen_election_timeout();
        if let Some(leader) = leader {
            // We lost the election, follow the winner.
            assert_eq!(term, self.term, "Can't follow leader in different term");
            info!("Lost election, following leader {} in term {}", leader, term);
            let voted_for = Some(self.id); // by definition
            Ok(self.into_role(Follower::new(Some(leader), voted_for, election_timeout)))
        } else {
            // We found a new term, but we don't necessarily know who the leader
            // is yet. We'll find out when we step a message from it.
            assert_ne!(term, self.term, "Can't become leaderless follower in current term");
            info!("Discovered new term {}", term);
            self.term = term;
            self.log.set_term(term, None)?;
            Ok(self.into_role(Follower::new(None, None, election_timeout)))
        }
    }

    /// Transitions the candidate to a leader. We won the election.
    pub(super) fn into_leader(self) -> Result<RawNode<Leader>> {
        info!("Won election for term {}, becoming leader", self.term);
        let peers = self.peers.clone();
        let (last_index, _) = self.log.get_last_index();
        let mut node = self.into_role(Leader::new(peers, last_index));

        // Propose an empty command when assuming leadership, to disambiguate
        // previous entries in the log. See section 8 in the Raft paper.
        //
        // We do this prior to the heartbeat, to avoid a wasted replication
        // roundtrip if the heartbeat response indicates the peer is behind.
        node.propose(None)?;
        node.heartbeat()?;

        Ok(node)
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
            return self.into_follower(msg.term, None)?.step(msg);
        }

        match msg.message {
            // Don't grant votes for other candidates who also campaign.
            Message::Campaign { .. } => {
                self.send(msg.from, Message::CampaignResponse { vote: false })?
            }

            // If we received a vote, record it. If the vote gives us quorum,
            // assume leadership.
            Message::CampaignResponse { vote: true } => {
                self.role.votes.insert(msg.from);
                if self.role.votes.len() as u8 >= self.quorum_size() {
                    return Ok(self.into_leader()?.into());
                }
            }

            // We didn't get a vote. :(
            Message::CampaignResponse { vote: false } => {}

            // If we receive a heartbeat or entries in this term, we lost the
            // election and have a new leader. Follow it and step the message.
            Message::Heartbeat { .. } | Message::Append { .. } => {
                return self.into_follower(msg.term, Some(msg.from))?.step(msg);
            }

            // Abort any inbound client requests while candidate.
            Message::ClientRequest { id, .. } => {
                self.send(msg.from, Message::ClientResponse { id, response: Err(Error::Abort) })?;
            }

            // We're not a leader in this term, nor are we forwarding requests,
            // so we shouldn't see these.
            Message::HeartbeatResponse { .. }
            | Message::AppendResponse { .. }
            | Message::ClientResponse { .. } => panic!("Received unexpected message {:?}", msg),
        }
        Ok(self.into())
    }

    /// Processes a logical clock tick.
    pub fn tick(mut self) -> Result<Node> {
        self.assert()?;

        self.role.election_duration += 1;
        if self.role.election_duration >= self.role.election_timeout {
            self.campaign()?;
        }
        Ok(self.into())
    }

    /// Campaign for leadership by increasing the term, voting for ourself, and
    /// soliciting votes from all peers.
    pub(super) fn campaign(&mut self) -> Result<()> {
        let term = self.term + 1;
        info!("Starting new election for term {}", term);
        self.role = Candidate::new(self.gen_election_timeout());
        self.role.votes.insert(self.id); // vote for ourself
        self.term = term;
        self.log.set_term(term, Some(self.id))?;

        let (last_index, last_term) = self.log.get_last_index();
        self.broadcast(Message::Campaign { last_index, last_term })?;
        Ok(())
    }
}
