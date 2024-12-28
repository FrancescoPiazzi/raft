use std::collections::HashSet as Set;
use actum::prelude::*;
use std::time::Duration;

use raft::config::{DEFAULT_HEARTBEAT_PERIOD, DEFAULT_REPLICATION_PERIOD};
use raft::server::raft_server;
use raft::state_machine::StateMachine;
use tokio::sync::mpsc;
use tokio::time::sleep;

use raft::messages::add_peer::AddPeer;
use raft::messages::poll_state::{PollStateRequest, ServerStateOnlyForTesting};

#[derive(Clone)]
struct ExampleStateMachine {
    set: Set<u64>,
}

impl ExampleStateMachine {
    fn new() -> Self {
        ExampleStateMachine { set: Set::new() }
    }
}

// reminder: the apply method MUST be idempotent, meaning that
// calling it with the same input more than once must not have any effect
impl StateMachine<u64, usize> for ExampleStateMachine {
    fn apply(&mut self, entry: &u64) -> usize {
        self.set.insert(*entry);
        self.set.len()
    }
}


#[tokio::test]
async fn main() {
    let state_machine_1 = ExampleStateMachine::new();
    let state_machine_2 = state_machine_1.clone();
    let mut actor_1 = actum(move |cell, me| async move {
        let me = (1, me);
        raft_server(
            cell,
            me,
            1,
            state_machine_1,
            Duration::from_millis(100)..Duration::from_millis(101),
            DEFAULT_HEARTBEAT_PERIOD,
            DEFAULT_REPLICATION_PERIOD,
        )
        .await
    });
    let mut actor_2 = actum(move |cell, me| async move {
        let me = (2, me);
        raft_server(
            cell,
            me,
            1,
            state_machine_2,
            Duration::from_millis(200)..Duration::from_millis(201),
            DEFAULT_HEARTBEAT_PERIOD,
            DEFAULT_REPLICATION_PERIOD,
        )
        .await
    });

    let task_1 = actor_1.task.run_task();
    let task_2 = actor_2.task.run_task();

    let _ = actor_1.m_ref.try_send(AddPeer{peer_id: 2, peer_ref:actor_2.m_ref.clone()}.into());
    let _ = actor_2.m_ref.try_send(AddPeer{peer_id: 1, peer_ref:actor_1.m_ref.clone()}.into());

    // TODO: this is not a good way to make sure a leader is elected
    sleep(Duration::from_millis(500)).await;

    let (tx, mut rx) = mpsc::channel(10);
    let _ = actor_2.m_ref.try_send(PollStateRequest{reply_to: tx}.into());
    let response = rx.recv().await.unwrap();
    assert_eq!(response.state, ServerStateOnlyForTesting::Follower);

    let (tx, mut rx) = mpsc::channel(10);
    let _ = actor_1.m_ref.try_send(PollStateRequest{reply_to: tx}.into());
    let response = rx.recv().await.unwrap();
    assert_eq!(response.state, ServerStateOnlyForTesting::Leader);

    task_1.await;
    task_2.await;
}
