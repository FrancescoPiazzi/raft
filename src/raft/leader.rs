use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use actum::prelude::*;

use crate::raft::model::*;

// the leader is the interface of the system to the external world
// clients send messages to the leader, which is responsible for replicating them to the other nodes
// after receiving confirmation from the majority of the nodes, the leader commits the message as agreed
pub async fn leader<AB, LogEntry>(
    cell: &mut AB,
    common_data: &mut CommonData<LogEntry>,
    peer_refs: &mut Vec<ActorRef<RaftMessage<LogEntry>>>,
    me: &ActorRef<RaftMessage<LogEntry>>,
)
where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + Clone + 'static,
{
    tracing::info!("👑 State is leader");

    let heartbeat_period_ms = 1000;
    let stop_flag = Arc::new(AtomicBool::new(false));

    // spawn a separate thread to send heartbeats every 1000 milliseconds
    // TOASK: maybe this should be a separate actor? But the followers shouldn't be able to tell the difference
    // as the heartbeats should come from the leader, would they be able to?
    let heartbeat_handle = std::thread::spawn({
        let current_term = common_data.current_term;
        let mut peer_refs = peer_refs.clone();
        let me = me.clone();
        let stop_flag = stop_flag.clone();
        move || {
            while !stop_flag.load(Ordering::Relaxed) {
                let empty_msg = Vec::new();
                for peer in peer_refs.iter_mut() {
                    let _ = peer.try_send(RaftMessage::AppendEntries(AppendEntriesRPC {
                        term: current_term,
                        leader_ref: me.clone(),
                        prev_log_index: 0,
                        prev_log_term: 0,
                        entries: empty_msg.clone(),
                        leader_commit: 0,
                    }));
                }
                std::thread::sleep(std::time::Duration::from_millis(heartbeat_period_ms));
            }
        }
    });

    loop {
        // no timeouts when we are leaders
        let message = cell.recv().await.message().expect("Received a None message, quitting");

        match message {
            RaftMessage::AppendEntriesClient(mut append_entries_client_rpc) => {
                tracing::info!("⏭️ Received a message from a client, replicating it to the other nodes");
                for peer in peer_refs.iter_mut() {
                    let _ = peer.try_send(RaftMessage::AppendEntries(AppendEntriesRPC {
                        term: common_data.current_term,
                        leader_ref: me.clone(),
                        prev_log_index: 0,
                        prev_log_term: 0,
                        entries: append_entries_client_rpc.entries.clone(),
                        leader_commit: 0,
                    }));
                }
                // TOASK: send the confirmation here or when it's committed? (sending it here for now as test)
                let _ = append_entries_client_rpc
                    .client_ref
                    .try_send(RaftMessage::AppendEntriesClientResponse(Ok(())));
            }
            RaftMessage::AppendEntries(append_entries_rpc) => {
                tracing::trace!(
                    "Received an AppendEntries message as the leader, somone dared challenge me, are they right?"
                );
                if append_entries_rpc.term > common_data.current_term {
                    // TOASK: should it be >= ?
                    tracing::trace!("They are right, I'm stepping down");
                    break;
                } else {
                    tracing::trace!("They are wrong, long live the king!");
                }
            }
            RaftMessage::RequestVote(mut request_vote_rpc) => {
                tracing::trace!(
                    "Received a request vote message as the leader, somone dared challenge me, are they right?"
                );
                if request_vote_rpc.term > common_data.current_term {
                    // TOASK: should it be >= ?
                    tracing::trace!("They are right, I'm stepping down");
                    let _ = request_vote_rpc
                        .candidate_ref
                        .try_send(RaftMessage::RequestVoteResponse(true));
                    common_data.voted_for = Some(request_vote_rpc.candidate_ref);
                    break;
                } else {
                    tracing::trace!("They are wrong, long live the king!");
                    let _ = request_vote_rpc
                        .candidate_ref
                        .try_send(RaftMessage::RequestVoteResponse(false));
                }
            }
            RaftMessage::AppendEntryResponse(_term, _success) => {
                tracing::trace!("Received an AppendEntryResponse message");
            }
            _ => {}
        }
    }

    stop_flag.store(true, Ordering::Relaxed);
    heartbeat_handle.join().unwrap();
}
