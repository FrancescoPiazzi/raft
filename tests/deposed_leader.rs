use std::time::Duration;

use actum::effect::Effect;
use actum::{prelude::*, testkit::Testkit};
use actum::testkit::testkit;
use oxidized_float::messages::RaftMessage;
use oxidized_float::prelude::*;
use oxidized_float::server::raft_server;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{info_span, Instrument};

mod test_state_machine;

use crate::test_state_machine::TestStateMachine;

#[tokio::test]
async fn deposed_leader() {
    tracing_subscriber::fmt()
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::NONE)
        .with_target(false)
        .with_line_number(false)
        .with_max_level(tracing::Level::TRACE)
        .init();

    let (server0, server0_tk) = testkit(move |cell, _| async move {
        raft_server(
            cell,
            0,
            1,
            TestStateMachine::new(),
            Duration::from_millis(100)..Duration::from_millis(101),
            Duration::from_millis(30),
        )
        .await
    });
    let (server1, server1_tk) = testkit(move |cell, _| async move {
        raft_server(
            cell,
            1,
            1,
            TestStateMachine::new(),
            Duration::from_millis(500)..Duration::from_millis(501),
            Duration::from_millis(30),
        )
        .await
    });
    let server0_ref = server0.m_ref.clone();
    let mut server1_ref = server1.m_ref.clone();

    let handle0 = tokio::spawn(server0.task.run_task().instrument(info_span!("server", id=0)));
    let handle1 = tokio::spawn(server1.task.run_task().instrument(info_span!("server", id=1)));

    let server0_tk_handle = tokio::spawn(run_testkit_until_actor_returns(server0_tk)
        .instrument(info_span!("testkit", id=0)
    ));
    let server1_tk_handle = tokio::spawn(run_testkit_until_actor_returns_drop_append_entries(server1_tk)
        .instrument(info_span!("testkit", id=1)
    ));

    send_peer_refs::<u64, usize>(&vec![server0_ref.clone(), server1_ref.clone()], &[0, 1]);

    let (tx, mut rx) = mpsc::channel::<AppendEntriesClientResponse<u64, usize>>(10);

    sleep(Duration::from_millis(1000)).await;

    tracing::debug!("sending AppendEntriesClientRequest to leader");
    let msg = AppendEntriesClientRequest {
        reply_to: tx,
        entries_to_replicate: vec![1],
    };
    server1_ref.try_send(msg.into()).unwrap();

    let recv = rx.recv().await.unwrap();
    assert!(matches!(recv, AppendEntriesClientResponse(Ok(1))));

    tracing::debug!("dropping actor guards");
    drop(server0.guard);
    drop(server1.guard);

    let _ = handle0.await;
    let _ = handle1.await;

    server0_tk_handle.await.expect("the testkit loop should return after that the actor has returned");
    server1_tk_handle.await.expect("the testkit loop should return after that the actor has returned");
}


pub async fn run_testkit_until_actor_returns_drop_append_entries<SMin, SMout>(
    mut testkit: Testkit<RaftMessage<SMin, SMout>>, 
) where
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    loop {
        match testkit.test_next_effect(|effect| {
            if let Effect::Recv(mut inner) = effect{
                match inner.recv {
                    Recv::Message(msg) => {
                        match msg {
                            RaftMessage::AppendEntriesRequest(_) => {
                                inner.discard();
                            }
                            _ => {}
                        }
                    },
                    _ => {}
                }
            }
        }).await {
            None => break,
            Some((returned, ())) => {
                testkit = returned;
            }
        }
    }
}