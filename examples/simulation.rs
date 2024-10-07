use std::time::Duration;

use actum::drop_guard::ActorDropGuard;
use actum::prelude::*;
use raft::config::*;
use raft::messages::add_peer::AddPeer;
use raft::messages::RaftMessage;
use raft::server::raft_server;
use raft::messages::append_entries_client::AppendEntriesClientRequest;
use raft::types::AppendEntriesClientResponse;
use tokio::task::JoinHandle;
use tokio::sync::mpsc;
use tracing::{info_span, Instrument};

struct Server<LogEntry> {
    server_id: u32,
    server_ref: ActorRef<RaftMessage<LogEntry>>,
    #[allow(dead_code)] // guard is not used but must remain in scope or the actors are dropped as well
    guard: ActorDropGuard,
    handle: JoinHandle<()>,
}

fn spawn_raft_servers<LogEntry>(n_servers: usize) -> Vec<Server<LogEntry>>
where
    LogEntry: Clone + Send + 'static,
{
    let mut servers = Vec::with_capacity(n_servers);

    for id in 0..n_servers {
        let actor = actum::<RaftMessage<LogEntry>, _, _>(move |cell, me| async move {
            let me = (id as u32, me);
            let actor = raft_server(
                cell,
                me,
                n_servers - 1,
                DEFAULT_ELECTION_TIMEOUT,
                DEFAULT_HEARTBEAT_PERIOD,
                DEFAULT_REPLICATION_PERIOD,
            )
            .await;
            actor
        });
        let handle = tokio::spawn(actor.task.run_task().instrument(info_span!("server", id)));
        servers.push(Server {
            server_id: id as u32,
            server_ref: actor.m_ref,
            guard: actor.guard,
            handle,
        });
    }
    servers
}

fn send_peer_refs<LogEntry>(servers: &[Server<LogEntry>])
where
    LogEntry: Send + 'static,
{
    for i in 0..servers.len() {
        let (server_id, mut server_ref) = {
            let server = &servers[i];
            (server.server_id, server.server_ref.clone())
        };

        for other_server in servers {
            if server_id != other_server.server_id {
                let _ = server_ref.try_send(RaftMessage::AddPeer(AddPeer {
                    peer_id: other_server.server_id,
                    peer_ref: other_server.server_ref.clone(),
                }));
            }
        }
    }
}

async fn send_entries_to_duplicate<LogEntry>(
    servers: &Vec<Server<LogEntry>>, 
    entries: Vec<LogEntry>, 
    frequency: Duration,
    timeout: Duration, 
    (tx, mut rx): (mpsc::Sender<AppendEntriesClientResponse<LogEntry>>, mpsc::Receiver<AppendEntriesClientResponse<LogEntry>>)
)
where
    LogEntry: Clone + Send + 'static,
{
    let mut interval = tokio::time::interval(frequency);
    let mut leader = servers[0].server_ref.clone();

    loop {
        tracing::info!("SEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEENDDDD");

        let request = AppendEntriesClientRequest {
            entries_to_replicate: entries.clone(),
            reply_to: tx.clone(),
        };

        let _ = leader.try_send(request.into());

        // these are too many nested Results and Options but I don't know how to reduce them without losing expressiveness
        match tokio::time::timeout(timeout, rx.recv()).await {
            // we recieved an answer
            Ok(Some(response)) => {
                match response {
                    Ok(_) => {
                        tracing::info!("Recieved confirmation of successful entry replication");
                        interval.tick().await;
                    }
                    Err(Some(new_leader_ref)) => {
                        tracing::trace!("interrogated server is not the leader, switching to the indicated one");
                        leader = new_leader_ref;
                    }
                    Err(None) => {
                        tracing::trace!("interrogated server does not know who the leader is, switch to another random node");
                        let new_leader_index = rand::random::<usize>() % servers.len();
                        leader = servers[new_leader_index].server_ref.clone();
                        continue;
                    }
                }
            }
            // channel closed, break the loop
            Ok(None) => {
                break;
            }
            // no response, switch to another random node
            Err(_) => {
                let new_leader_index = rand::random::<usize>() % servers.len();
                leader = servers[new_leader_index].server_ref.clone();
            }
        }
    }
}

#[tokio::main]
async fn main() {
    type LogEntry = u64;

    tracing_subscriber::fmt()
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::NEW | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        )
        .with_target(false)
        .with_line_number(true)
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let (tx, rx) = mpsc::channel::<AppendEntriesClientResponse<LogEntry>>(1);

    let servers = spawn_raft_servers::<LogEntry>(5);
    send_peer_refs(&servers);

    tokio::time::sleep(Duration::from_millis(2500)).await;   // give the servers a moment to elect a leader

    send_entries_to_duplicate(&servers, vec![1, 2, 3], Duration::from_millis(700), Duration::from_millis(500), (tx, rx)).await;

    for server in servers {
        server.handle.await.unwrap();
    }
}
