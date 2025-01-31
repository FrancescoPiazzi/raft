use std::time::Duration;

use oxidized_float::prelude::*;

use tokio::sync::mpsc;
use tokio::time::sleep;

mod test_state_machine;
use crate::test_state_machine::TestStateMachine;


#[tokio::test]
async fn simple_replication_random_leader() {
    let n_servers = 5;
    let time_to_elect_leader = Duration::from_millis(500);
    let time_to_agree_on_value = Duration::from_millis(500);

    let (mut refs, _, handles, guards) = init_split_servers(n_servers, TestStateMachine::new());

    let (tx, _rx) = mpsc::channel::<AppendEntriesClientResponse<u64, usize>>(10);

    sleep(time_to_elect_leader).await;

    // send the same message to everyone, one is the leader and will accept it
    let msg = AppendEntriesClientRequest {
        reply_to: tx,
        entries_to_replicate: vec![1],
    };
    for i in 0..n_servers {
        refs[i].try_send(msg.clone().into()).unwrap();
    }

    sleep(time_to_agree_on_value).await;

    let _ = guards.into_iter().for_each(|g| drop(g));
    let mut state_machines: Vec<TestStateMachine> = Vec::new();
    for handle in handles {
        state_machines.push(handle.await.unwrap());
    }

    for state_machine in state_machines {
        assert_eq!(state_machine.set.len(), 1);
    }
}
