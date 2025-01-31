use std::collections::HashSet as Set;
use std::time::Duration;

use oxidized_float::types::AppendEntriesClientResponse;
use oxidized_float::util::*;
use oxidized_float::state_machine::StateMachine;
use oxidized_float::messages::append_entries_client::AppendEntriesClientRequest;
use tokio::sync::mpsc;
use tokio::time::sleep;


#[derive(Clone)]
struct ExampleStateMachine {
    set: Set<u64>,
}

impl ExampleStateMachine {
    fn new() -> Self {
        ExampleStateMachine { set: Set::new() }
    }
}

impl StateMachine<u64, usize> for ExampleStateMachine {
    fn apply(&mut self, entry: &u64) -> usize {
        self.set.insert(*entry);
        self.set.len()
    }
}


#[tokio::test]
async fn simple_replication_single() {
    let n_servers = 5;
    let time_to_elect_leader = Duration::from_millis(500);
    let time_to_agree_on_value = Duration::from_millis(500);

    let state_machine = ExampleStateMachine::new();
    let (mut refs, _, handles, guards) = split_init(n_servers, state_machine);

    let (tx, _rx) = mpsc::channel::<AppendEntriesClientResponse<u64, usize>>(10);

    sleep(time_to_elect_leader).await;

    // just send the same message to everyone, one is the leader and will accept it
    let msg = AppendEntriesClientRequest {
        reply_to: tx,
        entries_to_replicate: vec![1],
    };
    for i in 0..n_servers {
        refs[i].try_send(msg.clone().into()).unwrap();
    }

    sleep(time_to_agree_on_value).await;

    let _ = guards.into_iter().for_each(|g| drop(g));
    let mut state_machines: Vec<ExampleStateMachine> = Vec::new();
    for handle in handles {
        state_machines.push(handle.await.unwrap());
    }

    for state_machine in state_machines {
        assert_eq!(state_machine.set.len(), 1);
    }
}