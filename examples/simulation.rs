use std::collections::HashSet as Set;
use std::fmt::Debug;
use std::time::Duration;

use oxidized_float::messages::append_entries_client::AppendEntriesClientRequest;
use oxidized_float::state_machine::StateMachine;
use oxidized_float::types::AppendEntriesClientResponse;
use oxidized_float::util::{send_peer_refs, spawn_raft_servers, Server};
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
// calling it with the same input more than once must not have any effect after the first call
impl StateMachine<u64, usize> for ExampleStateMachine {
    fn apply(&mut self, entry: &u64) -> usize {
        self.set.insert(*entry);
        self.set.len()
    }
}

/// example of a client that sends groups of random entries to be replicated
/// awaits for the confirmation of each entry, and when a group is done it sends another one
/// also handles negative responses (e.g. the server is not the leader but knows who the leader is)
#[instrument(name = "client" skip(servers, entries, period, timeout))]
async fn send_entries_to_duplicate<SM, SMin, SMout>(
    servers: &Vec<Server<SM, SMin, SMout>>,
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

        // channel to recieve the result of the replication
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
                        switching to another random node in a second"
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
            tracing_subscriber::fmt::format::FmtSpan::NONE,
        )
        .with_target(false)
        .with_line_number(false)
        .with_max_level(tracing::Level::TRACE)
        .init();

    let servers = spawn_raft_servers(5, ExampleStateMachine::new());
    send_peer_refs(&servers);

    // give the servers a moment to elect a leader 
    // (not mandatory, but we won't get a useful answer if a leader is not elected yet)
    tokio::time::sleep(Duration::from_millis(2000)).await;

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
