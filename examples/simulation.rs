use std::collections::HashSet as Set;
use std::fmt::Debug;
use std::time::Duration;

use raft::messages::append_entries_client::AppendEntriesClientRequest;
use raft::state_machine::StateMachine;
use raft::types::AppendEntriesClientResponse;
use raft::util::{send_peer_refs, spawn_raft_servers, Server};
use rand::seq::IteratorRandom;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::instrument;

#[derive(Clone)]
struct ExampleStateMachine {
    set: Set<u64>,
}

impl ExampleStateMachine {
    fn new() -> Self {
        ExampleStateMachine { set: Set::new() }
    }
}

// reminder: the apply method MUST be idempotent, meaning that
// calling it with the same input more than once must not have any effect
impl StateMachine<u64, usize> for ExampleStateMachine {
    fn apply(&mut self, entry: &u64) -> usize {
        self.set.insert(*entry);
        self.set.len()
    }
}

/// example of a client that sends groups of random entries to be replicated
#[instrument(name = "client" skip(servers, entries, period, timeout))]
async fn send_entries_to_duplicate<SMin, SMout>(
    servers: &Vec<Server<SMin, SMout>>,
    entries: Set<Vec<SMin>>,
    period: Duration,
    timeout: Duration,
) where
    SMin: Clone + Send + 'static,
    SMout: Send + Debug + 'static,
{
    let mut interval = tokio::time::interval(period);
    interval.tick().await; // first tick is immediate, skip it

    let mut leader = servers[0].server_ref.clone();
    let mut rng = rand::thread_rng();

    'client: loop {
        tracing::debug!("Sending entries to replicate");

        let (tx, mut rx) = mpsc::channel::<AppendEntriesClientResponse<SMin, SMout>>(10);

        let request = AppendEntriesClientRequest {
            entries_to_replicate: entries.iter().choose(&mut rng).unwrap().clone(),
            reply_to: tx,
        };
        let mut n_remaining_entries_to_replicate = request.entries_to_replicate.len();

        let _ = leader.try_send(request.into());

        while n_remaining_entries_to_replicate > 0 {
            match tokio::time::timeout(timeout, rx.recv()).await {
                Ok(Some(AppendEntriesClientResponse(Ok(result)))) => {
                    tracing::debug!(
                        "✅ Received confirmation of successful entry replication, result is: {:?}",
                        result
                    );
                    n_remaining_entries_to_replicate -= 1;
                }
                Ok(Some(AppendEntriesClientResponse(Err(Some(new_leader_ref))))) => {
                    tracing::debug!("Interrogated server is not the leader, switching to the indicated one");
                    leader = new_leader_ref;
                    break;
                }
                Ok(Some(AppendEntriesClientResponse(Err(None)))) | Err(_) => {
                    tracing::debug!(
                        "Interrogated server does not know who the leader is or it did not answer, \
                        switching to another random node"
                    );
                    leader = servers.iter().choose(&mut rng).unwrap().server_ref.clone();
                    sleep(Duration::from_millis(1000)).await;
                    break;
                }
                Ok(None) => {
                    tracing::error!("channel closed, exiting");
                    break 'client;
                }
            }
        }
        interval.tick().await;
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::NONE, // tracing_subscriber::fmt::format::FmtSpan::NEW | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        )
        .with_target(false)
        .with_line_number(false)
        .with_max_level(tracing::Level::TRACE)
        .init();

    let servers = spawn_raft_servers(5, ExampleStateMachine::new());
    send_peer_refs(&servers);

    tokio::time::sleep(Duration::from_millis(2000)).await; // give the servers a moment to elect a leader

    send_entries_to_duplicate(
        &servers,
        Set::from([vec![1, 3, 5, 7, 11], vec![1, 4, 9], vec![2, 4, 6]]),
        Duration::from_millis(1000),
        Duration::from_millis(2000),
    )
    .await;

    for server in servers {
        server.handle.await.unwrap();
    }
}
