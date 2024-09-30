use tokio::task::JoinHandle;
use tracing::{info_span, Instrument};

use actum::{actor_cell::standard_actor::StandardBounds, drop_guard::ActorDropGuard, prelude::*};

mod raft;
use raft::actor::raft_actor;
use raft::config::N_NODES;
use raft::messages::RaftMessage;

mod client;
use client::client;

async fn simulator<AB, LogEntry>(
    mut cell: ActorCell<(), AB>,
    server_count: usize,
    client_message: Vec<LogEntry>,
) -> ActorCell<(), AB>
where
    ActorCell<(), AB>: ActorBounds<()>,
    LogEntry: Clone + Send + 'static,
{
    let mut refs: Vec<ActorRef<RaftMessage<LogEntry>>> = Vec::with_capacity(server_count);
    let mut ids: Vec<String> = Vec::with_capacity(server_count);
    #[allow(clippy::collection_is_never_read)] // we only need to store these 'cause otherwise the actors are dropped
    let mut guards: Vec<ActorDropGuard> = Vec::with_capacity(server_count);
    let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(server_count);

    for id in 0..server_count {
        let Actor { task, guard, m_ref } = cell
            .spawn(move |cell, me| async move { raft_actor(cell, me, server_count).await })
            .await
            .unwrap();

        let handle = tokio::spawn(task.run_task().instrument(info_span!("server", id)));

        refs.push(m_ref);
        ids.push(id.to_string());
        guards.push(guard);
        handles.push(handle);
    }

    for server_ref in &refs {
        for (i, other_server_ref) in refs.iter().enumerate() {
            if server_ref != other_server_ref {
                let mut server1 = server_ref.clone();
                let server2 = other_server_ref.clone();
                let _ = server1.try_send(RaftMessage::AddPeer(server2, ids[i].clone()));
            }
        }
    }

    tracing::info!("Neighbors initialized");

    let mut client_actor: Actor<RaftMessage<LogEntry>, _> = cell
        .spawn(move |cell, me| async move { client(cell, me, server_count).await })
        .await
        .unwrap();
    let client_handle = tokio::spawn(client_actor.task.run_task().instrument(info_span!("client")));

    for (i, server_ref) in refs.iter().enumerate() {
        let _ = client_actor.m_ref.try_send(RaftMessage::AddPeer(server_ref.clone(), ids[i].clone()));
    }
    let _ = client_actor.m_ref.try_send(RaftMessage::InitMessage(client_message));

    for handle in handles {
        let _ = handle.await;
    }
    let _ = client_handle.await;

    cell
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::NEW | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        )
        .with_target(false)
        .with_line_number(true)
        .with_max_level(tracing::Level::INFO)
        .init();

    // Note: guard must remain in scope
    #[allow(unused_variables)]
    let Actor { task, guard, .. } = actum(|cell, me| {
        simulator::<StandardBounds, String>(cell, N_NODES, vec!["Hello".to_string(), " raft!".to_string()])
    });
    task.run_task().await;
}
