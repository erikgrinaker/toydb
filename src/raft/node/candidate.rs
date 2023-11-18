use super::super::{Address, Event, Message, Response};
use super::{
    Follower, Leader, Node, NodeID, RoleNode, Term, ELECTION_TIMEOUT_MAX, ELECTION_TIMEOUT_MIN,
};
use crate::error::Result;

use ::log::{debug, info, warn};
use rand::Rng as _;

/// A candidate is campaigning to become a leader.
#[derive(Debug)]
pub struct Candidate {
    /// Ticks elapsed since election start.
    election_ticks: u64,
    /// Election timeout, in ticks.
    election_timeout: u64,
    /// Votes received (including ourself).
    votes: u64,
}

impl Candidate {
    /// Creates a new candidate role.
    pub fn new() -> Self {
        Self {
            votes: 1, // We always start with a vote for ourselves.
            election_ticks: 0,
            election_timeout: rand::thread_rng()
                .gen_range(ELECTION_TIMEOUT_MIN..=ELECTION_TIMEOUT_MAX),
        }
    }
}

impl RoleNode<Candidate> {
    /// Transition to follower role.
    fn become_follower(mut self, term: Term, leader: NodeID) -> Result<RoleNode<Follower>> {
        info!("Discovered leader {} for term {}, following", leader, term);
        self.term = term;
        self.log.set_term(term, None)?;
        let mut node = self.become_role(Follower::new(Some(leader), None))?;
        node.abort_proxied()?;
        node.forward_queued(Address::Node(leader))?;
        Ok(node)
    }

    /// Transition to leader role.
    fn become_leader(self) -> Result<RoleNode<Leader>> {
        info!("Won election for term {}, becoming leader", self.term);
        let peers = self.peers.clone();
        let (last_index, _) = self.log.get_last_index();
        let (commit_index, commit_term) = self.log.get_commit_index();
        let mut node = self.become_role(Leader::new(peers, last_index))?;
        node.send(Address::Broadcast, Event::Heartbeat { commit_index, commit_term })?;
        node.append(None)?;
        node.abort_proxied()?;
        Ok(node)
    }

    /// Processes a message.
    pub fn step(mut self, msg: Message) -> Result<Node> {
        if let Err(err) = self.validate(&msg) {
            warn!("Ignoring invalid message: {}", err);
            return Ok(self.into());
        }
        if msg.term > self.term {
            if let Address::Node(from) = msg.from {
                return self.become_follower(msg.term, from)?.step(msg);
            }
        }

        match msg.event {
            Event::Heartbeat { .. } => {
                if let Address::Node(from) = msg.from {
                    return self.become_follower(msg.term, from)?.step(msg);
                }
            }

            Event::GrantVote => {
                debug!("Received term {} vote from {:?}", self.term, msg.from);
                self.role.votes += 1;
                if self.role.votes >= self.quorum() {
                    let queued = std::mem::take(&mut self.queued_reqs);
                    let mut node: Node = self.become_leader()?.into();
                    for (from, event) in queued {
                        node = node.step(Message { from, to: Address::Local, term: 0, event })?;
                    }
                    return Ok(node);
                }
            }

            Event::ClientRequest { .. } => self.queued_reqs.push((msg.from, msg.event)),

            Event::ClientResponse { id, mut response } => {
                if let Ok(Response::Status(ref mut status)) = response {
                    status.server = self.id;
                }
                self.proxied_reqs.remove(&id);
                self.send(Address::Client, Event::ClientResponse { id, response })?;
            }

            // Ignore other candidates when we're also campaigning
            Event::SolicitVote { .. } => {}

            Event::ConfirmLeader { .. }
            | Event::ReplicateEntries { .. }
            | Event::AcceptEntries { .. }
            | Event::RejectEntries { .. } => warn!("Received unexpected message {:?}", msg),
        }
        Ok(self.into())
    }

    /// Processes a logical clock tick.
    pub fn tick(mut self) -> Result<Node> {
        // If the election times out, start a new one for the next term.
        self.role.election_ticks += 1;
        if self.role.election_ticks >= self.role.election_timeout {
            info!("Election timed out, starting new election for term {}", self.term + 1);
            let (last_index, last_term) = self.log.get_last_index();
            self.term += 1;
            self.log.set_term(self.term, None)?;
            self.role = Candidate::new();
            self.send(Address::Broadcast, Event::SolicitVote { last_index, last_term })?;
        }
        Ok(self.into())
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::{Entry, Instruction, Log, Request};
    use super::super::tests::{assert_messages, assert_node};
    use super::*;
    use crate::storage;
    use std::collections::HashMap;
    use tokio::sync::mpsc;

    #[allow(clippy::type_complexity)]
    fn setup() -> Result<(
        RoleNode<Candidate>,
        mpsc::UnboundedReceiver<Message>,
        mpsc::UnboundedReceiver<Instruction>,
    )> {
        let (node_tx, mut node_rx) = mpsc::unbounded_channel();
        let (state_tx, state_rx) = mpsc::unbounded_channel();
        let mut log = Log::new(Box::new(storage::engine::Memory::new()), false)?;
        log.append(1, Some(vec![0x01]))?;
        log.append(1, Some(vec![0x02]))?;
        log.append(2, Some(vec![0x03]))?;
        log.commit(2)?;
        log.set_term(3, None)?;

        let mut node = RoleNode {
            id: 1,
            peers: vec![2, 3, 4, 5],
            term: 3,
            log,
            node_tx,
            state_tx,
            queued_reqs: Vec::new(),
            proxied_reqs: HashMap::new(),
            role: Candidate::new(),
        };
        node = match node.step(Message {
            from: Address::Client,
            to: Address::Local,
            term: 0,
            event: Event::ClientRequest { id: vec![0xaf], request: Request::Query(vec![0xf0]) },
        })? {
            Node::Candidate(c) => c,
            _ => panic!("Unexpected node type"),
        };
        assert_messages(&mut node_rx, vec![]);
        Ok((node, node_rx, state_rx))
    }

    #[test]
    // Heartbeat for current term converts to follower, forwards the queued request from setup(),
    // and emits ConfirmLeader.
    fn step_heartbeat_current_term() -> Result<()> {
        let (candidate, mut node_rx, mut state_rx) = setup()?;
        let mut node = candidate.step(Message {
            from: Address::Node(2),
            to: Address::Node(1),
            term: 3,
            event: Event::Heartbeat { commit_index: 2, commit_term: 1 },
        })?;
        assert_node(&mut node).is_follower().term(3);
        assert_messages(
            &mut node_rx,
            vec![
                Message {
                    from: Address::Local,
                    to: Address::Node(2),
                    term: 0,
                    event: Event::ClientRequest {
                        id: vec![0xaf],
                        request: Request::Query(vec![0xf0]),
                    },
                },
                Message {
                    from: Address::Local,
                    to: Address::Node(2),
                    term: 3,
                    event: Event::ConfirmLeader { commit_index: 2, has_committed: true },
                },
            ],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // Heartbeat for future term converts to follower, forwards queued request, and emits
    // ConfirmLeader event
    fn step_heartbeat_future_term() -> Result<()> {
        let (candidate, mut node_rx, mut state_rx) = setup()?;
        let mut node = candidate.step(Message {
            from: Address::Node(2),
            to: Address::Node(1),
            term: 4,
            event: Event::Heartbeat { commit_index: 2, commit_term: 1 },
        })?;
        assert_node(&mut node).is_follower().term(4);
        assert_messages(
            &mut node_rx,
            vec![
                Message {
                    from: Address::Local,
                    to: Address::Node(2),
                    term: 0,
                    event: Event::ClientRequest {
                        id: vec![0xaf],
                        request: Request::Query(vec![0xf0]),
                    },
                },
                Message {
                    from: Address::Local,
                    to: Address::Node(2),
                    term: 4,
                    event: Event::ConfirmLeader { commit_index: 2, has_committed: true },
                },
            ],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // Heartbeat for past term is ignored
    fn step_heartbeat_past_term() -> Result<()> {
        let (candidate, mut node_rx, mut state_rx) = setup()?;
        let mut node = candidate.step(Message {
            from: Address::Node(2),
            to: Address::Node(1),
            term: 2,
            event: Event::Heartbeat { commit_index: 1, commit_term: 1 },
        })?;
        assert_node(&mut node).is_candidate().term(3);
        assert_messages(&mut node_rx, vec![]);
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    fn step_grantvote() -> Result<()> {
        let (candidate, mut node_rx, mut state_rx) = setup()?;
        let peers = candidate.peers.clone();
        let mut node = Node::Candidate(candidate);

        // The first vote is not sufficient for a quorum (3 votes including self)
        node = node.step(Message {
            from: Address::Node(3),
            to: Address::Node(1),
            term: 3,
            event: Event::GrantVote,
        })?;
        assert_node(&mut node).is_candidate().term(3);
        assert_messages(&mut node_rx, vec![]);
        assert_messages(&mut state_rx, vec![]);

        // However, the second external vote makes us leader
        node = node.step(Message {
            from: Address::Node(5),
            to: Address::Node(1),
            term: 3,
            event: Event::GrantVote,
        })?;
        assert_node(&mut node).is_leader().term(3);

        assert_eq!(
            node_rx.try_recv()?,
            Message {
                from: Address::Local,
                to: Address::Broadcast,
                term: 3,
                event: Event::Heartbeat { commit_index: 2, commit_term: 1 },
            },
        );

        for to in peers.iter().cloned() {
            assert_eq!(
                node_rx.try_recv()?,
                Message {
                    from: Address::Local,
                    to: Address::Node(to),
                    term: 3,
                    event: Event::ReplicateEntries {
                        base_index: 3,
                        base_term: 2,
                        entries: vec![Entry { index: 4, term: 3, command: None }],
                    },
                }
            )
        }

        // Now that we're leader, we process the queued request
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Broadcast,
                term: 3,
                event: Event::Heartbeat { commit_index: 2, commit_term: 1 },
            }],
        );
        assert_messages(
            &mut state_rx,
            vec![
                Instruction::Query {
                    id: vec![0xaf],
                    address: Address::Client,
                    command: vec![0xf0],
                    term: 3,
                    index: 2,
                    quorum: 3,
                },
                Instruction::Vote { term: 3, index: 2, address: Address::Local },
            ],
        );
        Ok(())
    }

    #[test]
    fn tick() -> Result<()> {
        let (candidate, mut node_rx, mut state_rx) = setup()?;
        let timeout = candidate.role.election_timeout;
        let mut node = Node::Candidate(candidate);

        assert!(timeout > 0);
        for _ in 0..timeout {
            assert_node(&mut node).is_candidate().term(3);
            node = node.tick()?;
        }
        assert_node(&mut node).is_candidate().term(4);

        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Broadcast,
                term: 4,
                event: Event::SolicitVote { last_index: 3, last_term: 2 },
            }],
        );
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }
}
