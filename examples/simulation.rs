use actum::drop_guard::ActorDropGuard;
use actum::prelude::*;
use raft::config::*;
use raft::messages::add_peer::AddPeer;
use raft::messages::RaftMessage;
use raft::server::raft_server;
use tokio::task::JoinHandle;

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

    for i in 0..n_servers {
        let actor = actum::<RaftMessage<LogEntry>, _, _>(move |cell, me| async move {
            let me = (i as u32, me);
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
        let handle = tokio::spawn(actor.task.run_task());
        servers.push(Server {
            server_id: i as u32,
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

    let servers = spawn_raft_servers::<u64>(5);
    send_peer_refs(&servers);

    for server in servers {
        server.handle.await.unwrap();
    }
}
