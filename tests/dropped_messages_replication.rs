use std::time::Duration;

use actum::prelude::*;
use actum::testkit::testkit;
use oxidized_float::prelude::*;
use oxidized_float::server::raft_server;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{info_span, Instrument};

mod test_state_machine;
mod message_forwarder;

use crate::test_state_machine::TestStateMachine;
use crate::message_forwarder::init_message_forwarder;


#[tokio::test]
async fn dropped_messages_replication() {
    // tracing_subscriber::fmt()
    //     .with_span_events(
    //         tracing_subscriber::fmt::format::FmtSpan::NEW | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
    //     )
    //     .with_target(false)
    //     .with_line_number(true)
    //     .with_max_level(tracing::Level::TRACE)
    //     .init();

    let n_servers = 5;
    let time_to_elect_leader = Duration::from_millis(1000);
    let time_to_agree_on_value = Duration::from_millis(2000);
 
    // use the utility function to create all the followers with one call
    let ((mut refs, mut ids, mut handles, mut guards), mut testkits) = spawn_raft_servers_testkit(
        n_servers-1, 
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
            Duration::from_millis(200), // if I set this too low the test will fail, idk wht
        )
        .await
    });
    let handle = tokio::spawn(actor.task.run_task().instrument(info_span!("leader")));
    let mut leader_ref = actor.m_ref.clone();   // useful to contact the leader later
    
    refs.push(actor.m_ref);
    ids.push(leader_id);
    handles.push(handle);
    guards.push(actor.guard);
    testkits.push(tk);

    let message_forwarders_handles = testkits.into_iter().map(|tk| init_message_forwarder(tk)).collect::<Vec<_>>();

    send_peer_refs::<TestStateMachine, u64, usize>(&refs, &ids);

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

    let _ = guards.into_iter().for_each(|g| drop(g));
    let mut state_machines: Vec<TestStateMachine> = Vec::new();
    for handle in handles.into_iter() {
        tracing::debug!("Waiting for server to return");
        state_machines.push(handle.await.unwrap());
    }

    // very loose check, but if it passes at least something has gone right
    for i in 0..n_servers {
        assert_eq!(state_machines[i].set.len(), 1);
    }

    for handle in message_forwarders_handles.into_iter() {
        handle.abort();
    }
}
