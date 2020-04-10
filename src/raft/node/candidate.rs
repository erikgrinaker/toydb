use super::*;
use rand::Rng;

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
            election_ticks: 0,
            election_timeout: rand::thread_rng()
                .gen_range(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX),
            // We always start with a vote for ourselves.
            votes: 1,
        }
    }
}

impl<L: kv::storage::Storage, S: State> RoleNode<Candidate, L, S> {
    /// Transition to follower role.
    fn become_follower(
        mut self,
        term: u64,
        leader: &str,
    ) -> Result<RoleNode<Follower, L, S>, Error> {
        info!("Discovered leader {} for term {}, following", leader, term);
        self.save_term(term, None)?;
        self.become_role(Follower::new(Some(leader), None))
    }

    /// Transition to leader role.
    fn become_leader(self) -> Result<RoleNode<Leader, L, S>, Error> {
        info!("Won election for term {}, becoming leader", self.term);
        let peers = self.peers.clone();
        let (last_index, _) = self.log.get_last();
        let (commit_index, commit_term) = self.log.get_committed();
        let mut node = self.become_role(Leader::new(peers, last_index))?;
        node.broadcast(Event::Heartbeat { commit_index, commit_term })?;
        node.append(None)?;
        Ok(node)
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
            Event::Heartbeat { .. } => {
                if let Some(from) = &msg.from {
                    return self.become_follower(msg.term, from)?.step(msg);
                }
            }
            Event::GrantVote => {
                debug!("Received term {} vote from {:?}", self.term, msg.from);
                self.role.votes += 1;
                if self.role.votes >= self.quorum() {
                    return Ok(self.become_leader()?.into());
                }
            }
            Event::ConfirmLeader { .. } => {}
            Event::SolicitVote { .. } => {}
            Event::ReplicateEntries { .. } => {}
            Event::AcceptEntries { .. } => {}
            Event::RejectEntries { .. } => {}
            // FIXME These should be queued or something
            Event::QueryState { .. } => {}
            Event::MutateState { .. } => {}
            Event::RespondState { .. } => {}
            Event::RespondError { .. } => {}
        }
        Ok(self.into())
    }

    /// Processes a logical clock tick.
    pub fn tick(mut self) -> Result<Node<L, S>, Error> {
        while let Some(_) = self.log.apply(&mut self.state)? {}
        // If the election times out, start a new one for the next term.
        self.role.election_ticks += 1;
        if self.role.election_ticks >= self.role.election_timeout {
            info!("Election timed out, starting new election for term {}", self.term + 1);
            self.save_term(self.term + 1, None)?;
            self.role = Candidate::new();
            let (last_index, last_term) = self.log.get_last();
            self.broadcast(Event::SolicitVote { last_index, last_term })?;
        }
        Ok(self.into())
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{assert_messages, assert_node, TestState};
    use super::*;
    use crossbeam::channel::Receiver;

    fn setup() -> (RoleNode<Candidate, kv::storage::Memory, TestState>, Receiver<Message>) {
        let (sender, receiver) = crossbeam::channel::unbounded();
        let mut state = TestState::new();
        let mut log = Log::new(kv::Simple::new(kv::storage::Memory::new())).unwrap();
        log.append(Entry { term: 1, command: Some(vec![0x01]) }).unwrap();
        log.append(Entry { term: 1, command: Some(vec![0x02]) }).unwrap();
        log.append(Entry { term: 2, command: Some(vec![0x03]) }).unwrap();
        log.commit(2).unwrap();
        log.apply(&mut state).unwrap();

        let mut node = RoleNode {
            id: "a".into(),
            peers: vec!["b".into(), "c".into(), "d".into(), "e".into()],
            term: 3,
            log,
            state,
            sender,
            role: Candidate::new(),
        };
        node.save_term(3, None).unwrap();
        (node, receiver)
    }

    #[test]
    // Heartbeat for current term converts to follower and emits ConfirmLeader event
    fn step_heartbeat_current_term() {
        let (candidate, rx) = setup();
        let node = candidate
            .step(Message {
                from: Some("b".into()),
                to: Some("a".into()),
                term: 3,
                event: Event::Heartbeat { commit_index: 1, commit_term: 1 },
            })
            .unwrap();
        assert_node(&node).is_follower().term(3);
        assert_messages(
            &rx,
            vec![Message {
                from: Some("a".into()),
                to: Some("b".into()),
                term: 3,
                event: Event::ConfirmLeader { commit_index: 1, has_committed: true },
            }],
        );
    }

    #[test]
    // Heartbeat for future term converts to follower and emits ConfirmLeader event
    fn step_heartbeat_future_term() {
        let (candidate, rx) = setup();
        let node = candidate
            .step(Message {
                from: Some("b".into()),
                to: Some("a".into()),
                term: 4,
                event: Event::Heartbeat { commit_index: 1, commit_term: 1 },
            })
            .unwrap();
        assert_node(&node).is_follower().term(4);
        assert_messages(
            &rx,
            vec![Message {
                from: Some("a".into()),
                to: Some("b".into()),
                term: 4,
                event: Event::ConfirmLeader { commit_index: 1, has_committed: true },
            }],
        );
    }

    #[test]
    // Heartbeat for past term is ignored
    fn step_heartbeat_past_term() {
        let (candidate, rx) = setup();
        let node = candidate
            .step(Message {
                from: Some("b".into()),
                to: Some("a".into()),
                term: 2,
                event: Event::Heartbeat { commit_index: 1, commit_term: 1 },
            })
            .unwrap();
        assert_node(&node).is_candidate().term(3);
        assert_messages(&rx, vec![]);
    }

    #[test]
    fn step_grantvote() {
        let (candidate, rx) = setup();
        let peers = candidate.peers.clone();
        let mut node = Node::Candidate(candidate);

        // The first vote is not sufficient for a quorum (3 votes including self)
        node = node
            .step(Message {
                from: Some("c".into()),
                to: Some("a".into()),
                term: 3,
                event: Event::GrantVote,
            })
            .unwrap();
        assert_node(&node).is_candidate().term(3);
        assert_messages(&rx, vec![]);

        // However, the second external vote makes us leader
        node = node
            .step(Message {
                from: Some("e".into()),
                to: Some("a".into()),
                term: 3,
                event: Event::GrantVote,
            })
            .unwrap();
        assert_node(&node).is_leader().term(3);

        for to in peers.iter().cloned() {
            assert!(!rx.is_empty());
            assert_eq!(
                rx.recv().unwrap(),
                Message {
                    from: Some("a".into()),
                    to: Some(to),
                    term: 3,
                    event: Event::Heartbeat { commit_index: 2, commit_term: 1 },
                }
            )
        }

        for to in peers.iter().cloned() {
            assert!(!rx.is_empty());
            assert_eq!(
                rx.recv().unwrap(),
                Message {
                    from: Some("a".into()),
                    to: Some(to),
                    term: 3,
                    event: Event::ReplicateEntries {
                        base_index: 3,
                        base_term: 2,
                        entries: vec![Entry { term: 3, command: None }],
                    },
                }
            )
        }

        assert_messages(&rx, vec![]);
    }

    #[test]
    fn tick() {
        let (candidate, rx) = setup();
        let timeout = candidate.role.election_timeout;
        let peers = candidate.peers.clone();
        let mut node = Node::Candidate(candidate);

        assert!(timeout > 0);
        for i in 0..timeout {
            assert_node(&node).is_candidate().term(3).applied(if i > 0 { 2 } else { 1 });
            node = node.tick().unwrap();
        }
        assert_node(&node).is_candidate().term(4);

        for to in peers.into_iter() {
            assert!(!rx.is_empty());
            assert_eq!(
                rx.recv().unwrap(),
                Message {
                    from: Some("a".into()),
                    to: Some(to),
                    term: 4,
                    event: Event::SolicitVote { last_index: 3, last_term: 2 },
                }
            )
        }
    }
}
