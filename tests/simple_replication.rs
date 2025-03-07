use std::time::Duration;

use oxidized_float::prelude::*;

use tokio::sync::mpsc;
use tokio::time::sleep;

mod test_state_machine;
use crate::test_state_machine::TestStateMachine;

#[tokio::test]
async fn simple_replication_random_leader() {
    tracing_subscriber::fmt()
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::NONE)
        .with_target(false)
        .with_line_number(false)
        // .without_time()
        .with_max_level(tracing::Level::TRACE)
        .init();

    let n_servers = 5;
    let time_to_elect_leader = Duration::from_millis(300);
    let time_to_agree_on_value = Duration::from_millis(200);

    let SplitServers {
        server_id_vec,
        mut server_ref_vec,
        guard_vec,
        handle_vec,
    } = spawn_raft_servers(
        n_servers,
        TestStateMachine::new(),
        Some(Duration::from_millis(100)..Duration::from_millis(250)),
        Some(Duration::from_millis(50)),
    );

    send_peer_refs::<u64, usize>(&server_ref_vec, &server_id_vec);

    let (tx, _rx) = mpsc::channel::<AppendEntriesClientResponse<u64, usize>>(10);

    sleep(time_to_elect_leader).await;

    // send the same message to everyone, one is the leader and will accept it
    let msg = AppendEntriesClientRequest {
        reply_to: tx,
        entries_to_replicate: vec![1],
    };
    for i in 0..n_servers {
        server_ref_vec[i].try_send(msg.clone().into()).unwrap();
    }

    sleep(time_to_agree_on_value).await;

    let _ = guard_vec.into_iter().for_each(|g| drop(g));
    let mut state_machines: Vec<TestStateMachine> = Vec::new();
    for handle in handle_vec {
        state_machines.push(handle.await.unwrap());
    }

    for state_machine in state_machines {
        assert_eq!(state_machine.set.len(), 1);
    }
}
