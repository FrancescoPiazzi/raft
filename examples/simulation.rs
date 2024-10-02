use actum::drop_guard::ActorDropGuard;
use actum::prelude::*;
use raft::messages::RaftMessage;
use raft::server::raft_server;
use tokio::task::JoinHandle;
use tracing::{trace_span, Instrument};

struct Server<LogEntry> {
    server_id: u32,
    server_ref: ActorRef<RaftMessage<LogEntry>>,
    guard: ActorDropGuard,
    handle: JoinHandle<()>,
}

fn spawn_raft_servers<LogEntry>(n_servers: usize) -> Vec<Server<LogEntry>>
where
    LogEntry: Clone + Send + 'static,
{
    let mut servers = Vec::with_capacity(n_servers);

    for i in 0..n_servers {
        let mut actor = actum::<RaftMessage<LogEntry>, _, _>(move |cell, me| async move {
            let me = (i as u32, me);
            let actor = raft_server(cell, me, n_servers - 1).await;
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

#[tokio::main]
async fn main() {
    let mut servers = spawn_raft_servers::<u64>(5);

    for server in servers {
        server.handle.await.unwrap();
    }
}
