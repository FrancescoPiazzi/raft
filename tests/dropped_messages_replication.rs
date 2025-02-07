use std::time::Duration;

use actum::prelude::*;
use actum::testkit::testkit;
use oxidized_float::prelude::*;
use oxidized_float::server::raft_server;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{info_span, Instrument};

mod test_state_machine;

use crate::test_state_machine::TestStateMachine;
use oxidized_float::util::run_testkit_until_actor_returns;

const USE_FILE_LOGS: bool = false;

async fn dropped_messages_replication(
    n_servers: usize,
    time_to_elect_leader: Duration,
    time_to_replicate_entry: Duration,
) {
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

    // spawn all followers with the utility function
    let SplitServersWithTestkit {
        mut server_id_vec,
        mut server_ref_vec,
        mut guard_vec,
        mut handle_vec,
        mut testkit_vec,
    } = spawn_raft_servers_testkit(
        n_servers - 1,
        TestStateMachine::new(),
        Some(Duration::from_millis(1000)..Duration::from_millis(2000)),
        None, // heartbeat period does not matter, these will be followers
        Some(n_servers),
    );

    // manually create and spawn the leader
    let leader_id = (n_servers - 1) as u32;
    let (leader, leader_tk) = testkit(move |cell, _| async move {
        raft_server(
            cell,
            leader_id,
            n_servers - 1,
            TestStateMachine::new(),
            Duration::from_millis(100)..Duration::from_millis(101),
            Duration::from_millis(10), // if I set this too low the test will fail, idk why
        )
        .await
    });
    let handle = tokio::spawn(leader.task.run_task().instrument(info_span!("leader")));
    let mut leader_ref = leader.m_ref; // useful to contact the leader later

    server_ref_vec.push(leader_ref.clone());
    server_id_vec.push(leader_id);
    handle_vec.push(handle);
    guard_vec.push(leader.guard);
    testkit_vec.push(leader_tk);

    let mut testkit_handles = Vec::with_capacity(n_servers);
    for testkit in testkit_vec {
        let handle = tokio::spawn(run_testkit_until_actor_returns(testkit));
        testkit_handles.push(handle);
    }

    send_peer_refs::<u64, usize>(&server_ref_vec, &server_id_vec);

    let (tx, mut rx) = mpsc::channel::<AppendEntriesClientResponse<u64, usize>>(10);

    tracing::debug!("sleeping for {}, so that a leader can be elected", time_to_elect_leader.as_secs());
    sleep(time_to_elect_leader).await;

    tracing::debug!("sending AppendEntriesClientRequest to leader");
    let msg = AppendEntriesClientRequest {
        reply_to: tx,
        entries_to_replicate: vec![1],
    };
    leader_ref.try_send(msg.into()).unwrap();

    tracing::debug!("sleeping for {}, so that servers can the entry can be replicated", time_to_replicate_entry.as_secs());
    sleep(time_to_replicate_entry).await;

    let recv = rx.recv().await.unwrap();
    assert!(matches!(recv, AppendEntriesClientResponse(Ok(1))));

    tracing::debug!("dropping actor guards");
    for guard in guard_vec {
        drop(guard);
    }

    tracing::debug!("waiting for servers to return and collecting their state machines");
    let mut state_machines = Vec::<TestStateMachine>::with_capacity(n_servers);
    for (i, handle) in handle_vec.into_iter().rev().enumerate() {
        tracing::debug!("waiting for server {} to return", i);
        state_machines.push(handle.await.unwrap());
        tracing::debug!("server {} returned", i);
    }

    // very loose check, but if it passes at least something has gone right
    for i in 0..n_servers {
        assert_eq!(state_machines[i].set.len(), 1);
    }

    tracing::debug!("waiting for testkits to return");
    for handle in testkit_handles.into_iter() {
        handle
            .await
            .expect("the testkit loop should return after that the actor has returned");
    }
}

#[tokio::test]
async fn dropped_messages_replication_wrap() {
    dropped_messages_replication(5, Duration::from_millis(1000), Duration::from_millis(1000)).await;
}
