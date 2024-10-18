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

        // these are too many nested Results and Options but I don't know how to reduce them without losing expressiveness
        match tokio::time::timeout(timeout, rx).await {
            // we recieved an answer
            Ok(Ok(response)) => match response {
                Ok(_) => {
                    tracing::info!("Recieved confirmation of successful entry replication");
                    interval.tick().await;
                }
                Err(Some(new_leader_ref)) => {
                    tracing::trace!("interrogated server is not the leader, switching to the indicated one");
                    leader = new_leader_ref;
                }
                Err(None) => {
                    tracing::trace!(
                        "interrogated server does not know who the leader is, switch to another random node"
                    );
                    let new_leader_index = rand::random::<usize>() % servers.len();
                    leader = servers[new_leader_index].server_ref.clone();
                    continue;
                }
            },
            // channel closed, break the loop
            Ok(Err(_)) => {
                tracing::error!("Channel closed, exiting");
                break;
            }
            // no response, switch to another random node
            Err(e) => {
                tracing::warn!("No response from the interrogated server, switching to another random node");
                tracing::warn!("Error: {:?}", e);
                let new_leader_index = rand::random::<usize>() % servers.len();
                leader = servers[new_leader_index].server_ref.clone();
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
