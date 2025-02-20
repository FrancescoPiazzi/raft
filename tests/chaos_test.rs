use std::time::Duration;

use oxidized_float::prelude::*;
use rand::seq::SliceRandom;
use tracing::{info_span, Instrument};

mod test_state_machine;

use crate::test_state_machine::TestStateMachine;

const USE_FILE_LOGS: bool = false;

async fn chaos_test_inner(
    n_servers: usize,
    entry_batches: Vec<Vec<u64>>,
    request_timeout: Duration,
    time_to_converge_after_last_request: Duration,
    test_duration: Duration,
    message_drop_probability: f64,
) {
    let SplitServersWithTestkit {
        server_id_vec,
        server_ref_vec,
        guard_vec,
        handle_vec,
        testkit_vec,
    } = spawn_raft_servers_testkit(
        n_servers,
        TestStateMachine::new(),
        Some(Duration::from_millis(350)..Duration::from_millis(550)),
        Some(Duration::from_millis(100)),
        Some(n_servers),
    );

    let mut testkit_handles = Vec::with_capacity(n_servers);
    for (i, testkit) in testkit_vec.into_iter().enumerate() {
        let tk_span = info_span!("testkit", i);
        let handle = tokio::spawn(
            run_testkit_until_actor_returns_and_drop_messages(testkit, message_drop_probability)
                .instrument(tk_span)
        );
        testkit_handles.push(handle);
    }

    send_peer_refs::<u64, usize>(&server_ref_vec, &server_id_vec);

    // tokio::time::sleep(Duration::from_secs(1)).await;

    tracing::debug!("beginning requesting replications");
    let replication_start_timestamp = tokio::time::Instant::now();
    while tokio::time::Instant::now() - replication_start_timestamp < test_duration {
        let batch_to_replicate = entry_batches.choose(&mut rand::thread_rng()).unwrap();
        request_entry_replication::<TestStateMachine, u64, usize>(
            &server_ref_vec, batch_to_replicate.clone(), request_timeout
        ).instrument(info_span!("replicator")).await;
        // tokio::time::sleep(request_interval).await;
    }
    tokio::time::sleep(time_to_converge_after_last_request).await;

    tracing::debug!("dropping actor guards");
    for guard in guard_vec {
        drop(guard);
    }

    tracing::debug!("waiting for servers to return and collecting their state machines");
    let mut state_machines = Vec::<TestStateMachine>::with_capacity(n_servers);
    for handle in handle_vec.into_iter() {
        state_machines.push(handle.await.unwrap());
    }

    let reference_state_machine = state_machines.first().unwrap().clone();
    for (i, state_machine) in state_machines.into_iter().enumerate() {
        tracing::debug!("comparing state machine {} with the reference", i);
        assert_eq!(state_machine, reference_state_machine);
    }

    for handle in testkit_handles.into_iter() {
        handle.await.expect("the testkit loop should return after that the actor has returned");
    }
}

#[tokio::test]
async fn chaos_test() {
    const SLOW_FACTOR: u64 = 1;
    let n_servers = 3;

    #[allow(unused_variables)]
    let guards;
    if USE_FILE_LOGS {
        guards = split_file_logs(n_servers);
    } else {
        tracing_subscriber::fmt()
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::NONE)
            .with_target(false)
            .with_line_number(false)
            .with_max_level(tracing::Level::TRACE)
            .init();
    }

    let entry_batches = vec![
        vec![1, 2, 3],
        vec![21, 22, 23, 24, 25],
    ];

    for i in 1..=10{
        chaos_test_inner(n_servers, entry_batches.clone(), 
            Duration::from_millis(500*SLOW_FACTOR), 
            Duration::from_secs(3*SLOW_FACTOR), 
            Duration::from_secs(5*SLOW_FACTOR), 
            0.3
        ).await;
        tracing::info!("chaos test iteration {} passed", i);
    }
}
