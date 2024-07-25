
use tokio::task::JoinHandle;
use tracing::{info_span, Instrument};

use actum::{drop_guard::ActorDropGuard, prelude::*};

mod node;
use node::{raft_actor, RaftMessage};


async fn simulator<AB>(mut cell: ActorCell<(), AB>, server_count: usize) -> ActorCell<(), AB>
where
    ActorCell<(), AB>: ActorBounds<()>,
{
    let mut refs: Vec<ActorRef<RaftMessage>> = Vec::with_capacity(server_count);
    let mut guards: Vec<ActorDropGuard> = Vec::with_capacity(server_count);
    let mut handles: Vec<JoinHandle<()>>  = Vec::with_capacity(server_count);

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
        .with_max_level(tracing::Level::TRACE)
        .init();

    // Note: guard must remain in scope
    #[allow(unused_variables)]
    let Actor { task, guard, .. } = actum(|cell, me| simulator(cell, 5));
    task.run_task().await;
}
