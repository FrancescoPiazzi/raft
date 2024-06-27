use tracing::{info_span, Instrument};

use actum::prelude::*;

pub enum RaftMessage {
    AddPeer(ActorRef<RaftMessage>),
    RemovePeer(ActorRef<RaftMessage>),
    AppendEntry(String), // TODO: replace with any type that is Send + 'static
    AppendEntryResponse,
    RequestVote,
    RequestVoteResponse,
}

async fn raft_actor<AB>(mut cell: AB, _me: ActorRef<RaftMessage>)
where
    AB: ActorBounds<RaftMessage>,
{
    tracing::trace!("Raft actor running");
    let mut npeers = 0;
    loop {
        let msg = cell.recv().await.message();
        match msg {
            Some(raftmessage) => match raftmessage {
                RaftMessage::AddPeer(_peer) => {
                    npeers += 1;
                    tracing::info!("Peer added, total: {}", npeers);
                }
                _ => {
                    tracing::info!("Received a message");
                }
            },
            None => {
                tracing::info!("Received a None message, quitting");
                break;
            }
        }
    }
}

async fn simulator<AB>(mut cell: ActorCell<(), AB>, server_count: usize)
where
    ActorCell<(), AB>: ActorBounds<()>,
{
    let mut refs = Vec::with_capacity(server_count);
    let mut guards = Vec::with_capacity(server_count);
    let mut handles = Vec::with_capacity(server_count);

    for id in 0..server_count {
        let Actor { task, guard, m_ref } = cell.spawn(raft_actor).await.unwrap();
        let handle = tokio::spawn(task.run_task().instrument(info_span!("server", id)));
        refs.push(m_ref);
        guards.push(guard);
        handles.push(handle);
    }

    for server_ref in &refs {
        for other_server_ref in &refs {
            if !std::ptr::eq(server_ref, other_server_ref) {
                let mut server1 = server_ref.clone();
                let server2 = other_server_ref.clone();
                let _ = server1.try_send(RaftMessage::AddPeer(server2));
            }
        }
    }
    tracing::info!("Neighbors initialized");

    for handle in handles {
        let _ = handle.await;
    }
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
    let Actor { task, guard, .. } = actum::<(), _, _>(|cell, _| async {
        simulator(cell, 3).await;
    });
    task.run_task().await;
}
