use std::time::Duration;

use actum::prelude::*;
use actum::testkit::testkit;
use oxidized_float::prelude::*;
use oxidized_float::server::raft_server;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{info_span, Instrument};

mod message_forwarder;
mod test_state_machine;

use crate::message_forwarder::init_message_forwarder;
use crate::test_state_machine::TestStateMachine;

#[tokio::test]
async fn dropped_messages_replication() {
    tracing_subscriber::fmt()
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::NEW | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        )
        .with_target(false)
        .with_line_number(true)
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let n_servers = 5;
    let time_to_elect_leader = Duration::from_millis(1000);
    let time_to_agree_on_value = Duration::from_millis(1000);

    // use the utility function to create all the followers with one call
    let (
        SplitServers {
            mut server_id_vec,
            mut server_ref_vec,
            mut guard_vec,
            mut handle_vec,
        },
        mut testkits,
    ) = spawn_raft_servers_testkit(
        n_servers - 1,
        TestStateMachine::new(),
        Some(Duration::from_millis(1000)..Duration::from_millis(2000)),
        None, // heartbeat period does not matter, these will be followers
        Some(n_servers),
    );

    // manually create the leader
    let leader_id = (n_servers - 1) as u32;
    let (actor, tk) = testkit(move |cell, _| async move {
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
    let handle = tokio::spawn(actor.task.run_task().instrument(info_span!("leader")));
    let mut leader_ref = actor.m_ref.clone(); // useful to contact the leader later

    server_ref_vec.push(actor.m_ref);
    server_id_vec.push(leader_id);
    handle_vec.push(handle);
    guard_vec.push(actor.guard);
    testkits.push(tk);

    let message_forwarders_handles = testkits
        .into_iter()
        .map(|tk| init_message_forwarder(tk))
        .collect::<Vec<_>>();

    send_peer_refs::<TestStateMachine, u64, usize>(&server_ref_vec, &server_id_vec);

    let (tx, mut rx) = mpsc::channel::<AppendEntriesClientResponse<u64, usize>>(10);

    sleep(time_to_elect_leader).await;

    tracing::debug!("Sending request");

    let msg = AppendEntriesClientRequest {
        reply_to: tx,
        entries_to_replicate: vec![1],
    };
    leader_ref.try_send(msg.into()).unwrap();

    sleep(time_to_agree_on_value).await;

    let recv = rx.recv().await.unwrap();
    assert!(matches!(recv, AppendEntriesClientResponse(Ok(1))));

    tracing::debug!("Response ok");

    let _ = guard_vec.into_iter().for_each(|g| drop(g));
    let mut state_machines: Vec<TestStateMachine> = Vec::new();
    for (i, handle) in handle_vec.into_iter().rev().enumerate() {
        tracing::debug!("Waiting for server {} to return", i);
        state_machines.push(handle.await.unwrap());
        tracing::debug!("Server {} returned", i);
        // sleep(Duration::from_millis(200)).await;
    }

    // very loose check, but if it passes at least something has gone right
    for i in 0..n_servers {
        assert_eq!(state_machines[i].set.len(), 1);
    }

    for handle in message_forwarders_handles.into_iter() {
        handle
            .await
            .expect("the testkit loop should return after that the actor has returned");
    }
}
