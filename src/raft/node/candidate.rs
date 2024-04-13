use super::super::{Envelope, Message};
use super::{rand_election_timeout, Follower, Leader, Node, NodeID, RawNode, Role, Term, Ticks};
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
    pub fn new() -> Self {
        Self {
            votes: HashSet::new(),
            election_duration: 0,
            election_timeout: rand_election_timeout(),
        }
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

        if let Some(leader) = leader {
            // We lost the election, follow the winner.
            assert_eq!(term, self.term, "Can't follow leader in different term");
            info!("Lost election, following leader {} in term {}", leader, term);
            let voted_for = Some(self.id); // by definition
            Ok(self.into_role(Follower::new(Some(leader), voted_for)))
        } else {
            // We found a new term, but we don't necessarily know who the leader
            // is yet. We'll find out when we step a message from it.
            assert_ne!(term, self.term, "Can't become leaderless follower in current term");
            info!("Discovered new term {}", term);
            self.term = term;
            self.log.set_term(term, None)?;
            Ok(self.into_role(Follower::new(None, None)))
        }
    }

    /// Transitions the candidate to a leader. We won the election.
    pub(super) fn into_leader(self) -> Result<RawNode<Leader>> {
        info!("Won election for term {}, becoming leader", self.term);
        let peers = self.peers.clone();
        let (last_index, _) = self.log.get_last_index();
        let mut node = self.into_role(Leader::new(peers, last_index));
        node.heartbeat()?;

        // Propose an empty command when assuming leadership, to disambiguate
        // previous entries in the log. See section 8 in the Raft paper.
        node.propose(None)?;
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
            // Ignore other candidates when we're also campaigning.
            Message::Campaign { .. } => {}

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
        self.role = Candidate::new();
        self.role.votes.insert(self.id); // vote for ourself
        self.term = term;
        self.log.set_term(term, Some(self.id))?;

        let (last_index, last_term) = self.log.get_last_index();
        self.broadcast(Message::Campaign { last_index, last_term })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::state::tests::TestState;
    use super::super::super::{Entry, Log, Request};
    use super::super::tests::{assert_messages, assert_node};
    use super::*;
    use crate::storage;
    use itertools::Itertools as _;

    #[allow(clippy::type_complexity)]
    fn setup() -> Result<(RawNode<Candidate>, crossbeam::channel::Receiver<Envelope>)> {
        let (node_tx, node_rx) = crossbeam::channel::unbounded();
        let state = Box::new(TestState::new(0));
        let mut log = Log::new(storage::Memory::new(), false)?;

        log.append(1, Some(vec![0x01]))?;
        log.append(1, Some(vec![0x02]))?;
        log.append(2, Some(vec![0x03]))?;
        log.commit(2)?;
        log.set_term(3, Some(1))?;

        let mut node = RawNode {
            id: 1,
            peers: HashSet::from([2, 3, 4, 5]),
            term: 3,
            log,
            state,
            node_tx,
            role: Candidate::new(),
        };
        node.role.votes.insert(1);
        Ok((node, node_rx))
    }

    #[test]
    // Heartbeat for current term converts to follower and emits ConfirmLeader.
    fn step_heartbeat_current_term() -> Result<()> {
        let (candidate, mut node_rx) = setup()?;
        let mut node = candidate.step(Envelope {
            from: 2,
            to: 1,
            term: 3,
            message: Message::Heartbeat { commit_index: 2, commit_term: 1, read_seq: 7 },
        })?;
        assert_node(&mut node).is_follower().term(3);
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
    // Heartbeat for future term converts to follower and emits ConfirmLeader.
    fn step_heartbeat_future_term() -> Result<()> {
        let (candidate, mut node_rx) = setup()?;
        let mut node = candidate.step(Envelope {
            from: 2,
            to: 1,
            term: 4,
            message: Message::Heartbeat { commit_index: 2, commit_term: 1, read_seq: 7 },
        })?;
        assert_node(&mut node).is_follower().term(4);
        assert_messages(
            &mut node_rx,
            vec![Envelope {
                from: 1,
                to: 2,
                term: 4,
                message: Message::HeartbeatResponse { last_index: 3, last_term: 2, read_seq: 7 },
            }],
        );
        Ok(())
    }

    #[test]
    // Heartbeat for past term is ignored
    fn step_heartbeat_past_term() -> Result<()> {
        let (candidate, mut node_rx) = setup()?;
        let mut node = candidate.step(Envelope {
            from: 2,
            to: 1,
            term: 2,
            message: Message::Heartbeat { commit_index: 1, commit_term: 1, read_seq: 7 },
        })?;
        assert_node(&mut node).is_candidate().term(3);
        assert_messages(&mut node_rx, vec![]);
        Ok(())
    }

    #[test]
    fn step_grantvote() -> Result<()> {
        let (candidate, mut node_rx) = setup()?;
        let peers = candidate.peers.clone();
        let mut node = Node::Candidate(candidate);

        // The first vote is not sufficient for a quorum (3 votes including self)
        node = node.step(Envelope {
            from: 3,
            to: 1,
            term: 3,
            message: Message::CampaignResponse { vote: true },
        })?;
        assert_node(&mut node).is_candidate().term(3);
        assert_messages(&mut node_rx, vec![]);

        // However, the second external vote makes us leader
        node = node.step(Envelope {
            from: 5,
            to: 1,
            term: 3,
            message: Message::CampaignResponse { vote: true },
        })?;
        assert_node(&mut node).is_leader().term(3);

        for to in peers.iter().copied().sorted() {
            assert_eq!(
                node_rx.try_recv()?,
                Envelope {
                    from: 1,
                    to,
                    term: 3,
                    message: Message::Heartbeat { commit_index: 2, commit_term: 1, read_seq: 0 },
                },
            );
        }

        for to in peers.iter().copied() {
            assert_eq!(
                node_rx.try_recv()?,
                Envelope {
                    from: 1,
                    to,
                    term: 3,
                    message: Message::Append {
                        base_index: 3,
                        base_term: 2,
                        entries: vec![Entry { index: 4, term: 3, command: None }],
                    },
                }
            )
        }

        assert_messages(&mut node_rx, vec![]);
        Ok(())
    }

    #[test]
    // ClientRequest returns Error::Abort.
    fn step_clientrequest() -> Result<()> {
        let (candidate, mut node_rx) = setup()?;
        let mut node = Node::Candidate(candidate);

        node = node.step(Envelope {
            from: 1,
            to: 1,
            term: 3,
            message: Message::ClientRequest { id: vec![0x01], request: Request::Write(vec![0xaf]) },
        })?;
        assert_node(&mut node).is_candidate().term(3);
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

    #[test]
    fn tick() -> Result<()> {
        let (candidate, node_rx) = setup()?;
        let peers = candidate.peers.clone();
        let timeout = candidate.role.election_timeout;
        let mut node = Node::Candidate(candidate);

        assert!(timeout > 0);
        for _ in 0..timeout {
            assert_node(&mut node).is_candidate().term(3);
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
