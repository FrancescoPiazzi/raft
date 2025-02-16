use oxidized_float::prelude::*;
use std::collections::HashSet as Set;
use std::time::Duration;
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

    println!("sleeping for two seconds, so that a leader can be elected.");
    tokio::time::sleep(Duration::from_secs(2)).await;

    let entries = vec![1, 3, 5, 7, 11];

    request_entry_replication::<ExampleStateMachine, u64, usize>(
        &split_servers.server_ref_vec, entries, Duration::from_millis(1000)).instrument(info_span!("replicator")
    ).await;

    for guard in split_servers.guard_vec {
        drop(guard);
    }

    for handle in split_servers.handle_vec {
        handle.await.unwrap();
    }
}
