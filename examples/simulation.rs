use std::time::Duration;

use raft::state_machine::StateMachine;
use raft::messages::append_entries_client::AppendEntriesClientRequest;
use raft::types::AppendEntriesClientResponse;
use raft::util::{send_peer_refs, spawn_raft_servers, Server};
use tokio::sync::oneshot;
use tracing::instrument;


#[derive(Clone)]
struct ExampleStateMachine{
    sum: u64,
}

impl ExampleStateMachine {
    fn new() -> Self {
        ExampleStateMachine { sum: 0 }
    }
}

impl StateMachine<u64, bool> for ExampleStateMachine {
    fn apply(&mut self, entry: &u64) -> bool {
        self.sum += entry;
        tracing::debug!("State machine sum: {}", self.sum);
        self.sum%2 == 0
    }
}

/// example of a client that sends groups of random entries to be replicated
#[instrument(name = "client" skip(servers, entries, period, timeout))]
async fn send_entries_to_duplicate<SMin>(
    servers: &Vec<Server<SMin>>,
    entries: Vec<SMin>,
    period: Duration,
    timeout: Duration,
) where
    SMin: Clone + Send + 'static,
{
    let mut interval = tokio::time::interval(period);
    interval.tick().await; // first tick is immediate, skip it

    let mut leader = servers[0].server_ref.clone();

    loop {
        tracing::info!("Sending entries to replicate");

        let (tx, rx) = oneshot::channel::<AppendEntriesClientResponse<SMin>>();

        let request = AppendEntriesClientRequest {
            entries_to_replicate: entries.clone(),
            reply_to: tx,
        };

        let _ = leader.try_send(request.into());

        // these are too many nested Results but I don't know how to reduce them without losing expressiveness
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(Ok(_))) => {
                tracing::info!("Recieved confirmation of successful entry replication");
                interval.tick().await;
            }
            Ok(Ok(Err(Some(new_leader_ref)))) => {
                tracing::trace!("Interrogated server is not the leader, switching to the indicated one");
                leader = new_leader_ref;
            }
            Ok(Ok(Err(None))) | Err(_) => {
                tracing::trace!(
                    "Interrogated server does not know who the leader is or it did not answer, switch to another random node"
                );
                let new_leader_index = rand::random::<usize>() % servers.len();
                leader = servers[new_leader_index].server_ref.clone();
                continue;
            }
            Ok(Err(_)) => {
                tracing::error!("Channel closed, exiting");
                break;
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
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let servers = spawn_raft_servers(5, ExampleStateMachine::new());
    send_peer_refs(&servers);

    tokio::time::sleep(Duration::from_millis(2500)).await; // give the servers a moment to elect a leader

    send_entries_to_duplicate(
        &servers,
        vec![1],
        Duration::from_millis(1000),
        Duration::from_millis(2000),
    )
    .await;

    for server in servers {
        server.handle.await.unwrap();
    }
}
