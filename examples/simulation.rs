use oxidized_float::prelude::*;
use rand::seq::IteratorRandom;
use rand::Rng;
use std::collections::HashSet as Set;
use std::fmt::Debug;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{info_span, Instrument};

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

async fn replicate_entries<SM, SMin, SMout>(
    servers: &Vec<Server<SM, SMin, SMout>>,
    entries: Vec<SMin>,
    timeout: Duration,
) -> Vec<SMout>
where
    SMin: Clone + Send + 'static,
    SMout: Send + Debug + 'static,
{
    let mut rng = rand::thread_rng();

    let mut leader = servers[0].server_ref.clone();
    let mut output_vec = Vec::with_capacity(entries.len());

    'outer: loop {
        tracing::debug!("sending entries to replicate");

        // create a channel to receive the outputs of the state machine.
        let (tx, mut rx) = mpsc::channel::<AppendEntriesClientResponse<SMin, SMout>>(10);

        let request = AppendEntriesClientRequest {
            entries_to_replicate: entries.clone(),
            reply_to: tx,
        };
        let _ = leader.try_send(request.into());

        'inner: while output_vec.len() < entries.len() {
            match tokio::time::timeout(timeout, rx.recv()).await {
                Ok(Some(AppendEntriesClientResponse(Ok(output)))) => {
                    tracing::debug!("entry have been successfully replicated");
                    output_vec.push(output);
                }
                Ok(Some(AppendEntriesClientResponse(Err(Some(new_leader_ref))))) => {
                    tracing::debug!("this server is not the leader: switching to the provided leader");
                    leader = new_leader_ref;
                    break 'inner;
                }
                Ok(Some(AppendEntriesClientResponse(Err(None)))) | Err(_) => {
                    tracing::debug!("this server did not answer or does not know who the leader is: switching to another random server in a second.");
                    leader = servers.iter().choose(&mut rng).unwrap().server_ref.clone();
                    sleep(Duration::from_millis(1000)).await;
                    break 'inner;
                }
                Ok(None) => {
                    tracing::error!("channel closed, exiting");
                    break 'outer;
                }
            }
        }
    }

    output_vec
}

#[tokio::main]
async fn main() {
    let n_servers = 5;
    let separate_file_logs = true;

    #[allow(unused_variables)]
    let guards;
    if separate_file_logs {
        guards = split_file_logs(n_servers);
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

    let batch0 = vec![1, 3, 5, 7, 11];
    let batch1 = vec![1, 4, 9];
    let batch2 = vec![2, 4, 6];

    let choice = rand::thread_rng().gen_range(0..=2);

    match choice {
        0 => replicate_entries(&servers, batch0, Duration::from_millis(1000)).instrument(info_span!("replicator")),
        1 => replicate_entries(&servers, batch1, Duration::from_millis(1000)).instrument(info_span!("replicator")),
        2 => replicate_entries(&servers, batch2, Duration::from_millis(1000)).instrument(info_span!("replicator")),
        _ => unreachable!(),
    }
    .await;

    for server in servers {
        server.handle.await.unwrap();
    }
}
