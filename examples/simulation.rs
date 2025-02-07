use std::collections::HashSet as Set;
use std::fmt::Debug;
use std::time::Duration;

use oxidized_float::prelude::*;
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
async fn replicate_entries<SM, SMin, SMout>(
    servers: &Vec<Server<SM, SMin, SMout>>,
    entries: Set<Vec<SMin>>,
    timeout: Duration,
) where
    SMin: Clone + Send + 'static,
    SMout: Send + Debug + 'static,
{
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
    }
}

#[tokio::main]
async fn main() {
    let n_servers = 5;
    let separate_file_logs = false;

    if separate_file_logs {
        let mut _file_appender_guards = Vec::new(); // guards must remain in scope for the file appenders to work
        split_file_logs(n_servers, &mut _file_appender_guards).await;
    } else {
        tracing_subscriber::fmt()
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::NONE)
            .with_target(false)
            .with_line_number(false)
            .with_max_level(tracing::Level::TRACE)
            .init();
    }

    let split_servers = spawn_raft_servers(n_servers, ExampleStateMachine::new(), None, None);

    send_peer_refs::<u64, usize>(&split_servers.server_ref_vec, &split_servers.server_id_vec);

    let servers = split_servers.into_server_vec();

    println!("sleeping for two seconds, so that a leader can be elected.");
    tokio::time::sleep(Duration::from_secs(2)).await;

    replicate_entries(
        &servers,
        Set::from([vec![1, 3, 5, 7, 11], vec![1, 4, 9], vec![2, 4, 6]]),
        Duration::from_millis(1000),
    )
    .await;

    for server in servers {
        server.handle.await.unwrap();
    }
}
