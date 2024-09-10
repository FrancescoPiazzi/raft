use std::time::Duration;

use tokio::time::{self, MissedTickBehavior};

use crate::raft::common_state::CommonState;
use crate::raft::messages::*;
use actum::prelude::*;

use super::messages;

// the leader is the interface of the cluster to the external world
// clients send messages to the leader, which is responsible for replicating them to the other nodes
// after receiving confirmation from the majority of the nodes, the leader commits the message as agreed
// returns when another leader or candidate with a higher term is detected
pub async fn leader<AB, LogEntry>(
    cell: &mut AB,
    common_data: &mut CommonState<LogEntry>,
    peer_refs: &mut Vec<ActorRef<RaftMessage<LogEntry>>>,
    me: &ActorRef<RaftMessage<LogEntry>>,
    heartbeat_period: Duration,
) where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + Clone + 'static,
{
    // let current_term = common_data.current_term;
    // let mut peer_refs_clone = peer_refs.clone();

    // tokio::select! {
    //     _ = message_handler(cell, common_data, peer_refs, me) => {},
    //     _ = heartbeat_sender(heartbeat_period, current_term, &mut peer_refs_clone, me) => {},
    // }

    loop {
        tokio::select! {
            message = cell.recv() => {
                let message = message.message().expect("raft runs indefinitely");
                let become_follow = handle_message::<AB, LogEntry>(common_data, peer_refs, me, message);
                if become_follow {
                    break;
                }
            },
            _ = tokio::time::sleep(heartbeat_period) => {
                send_heartbeats(common_data.current_term, peer_refs, me);
            },
        }
    }
}

// sends one round of heartbeats to all followers
fn send_heartbeats<LogEntry>(
    current_term: u64,
    peer_refs: &mut [ActorRef<RaftMessage<LogEntry>>],
    me: &ActorRef<RaftMessage<LogEntry>>,
) where
    LogEntry: Send + Clone + 'static,
{
    let heartbeat_msg = RaftMessage::AppendEntries(AppendEntriesRPC {
        term: current_term,
        leader_ref: me.clone(),
        prev_log_index: 0,
        prev_log_term: 0,
        entries: Vec::new(),
        leader_commit: 0,
    });
    for peer in peer_refs.iter_mut() {
        let _ = peer.try_send(heartbeat_msg.clone());
    }
}

// sends heartbeats to all followers at a regular interval
async fn heartbeat_sender<LogEntry>(
    heartbeat_period: Duration,
    current_term: u64,
    peer_refs: &mut [ActorRef<RaftMessage<LogEntry>>],
    me: &ActorRef<RaftMessage<LogEntry>>,
) where
    LogEntry: Send + Clone + 'static,
{
    let heartbeat_msg = RaftMessage::AppendEntries(AppendEntriesRPC {
        term: current_term,
        leader_ref: me.clone(),
        prev_log_index: 0,
        prev_log_term: 0,
        entries: Vec::new(),
        leader_commit: 0,
    });
    
    let mut handles = Vec::with_capacity(peer_refs.len());
    for peer in peer_refs.iter_mut() {
        let heartbeat_msg = heartbeat_msg.clone();  // clone the message to move it into the closure
        let mut peer = peer.clone();
        let mut interval = time::interval(heartbeat_period);
        // no point in catching up if we miss a tick, just keep sending them at the same rate
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let handle = tokio::spawn(async move {
            loop {
                let _ = peer.try_send(heartbeat_msg.clone());   // clone the message every time we send it
                interval.tick().await;
            }
        });
        handles.push(handle);
    };

    for handle in handles {
        let _ = handle.await;
    }
}

// handles one message as leader
// returns true if we have to go back to a follower state, false otherwise
fn handle_message<AB, LogEntry>(
    common_data: &mut CommonState<LogEntry>,
    peer_refs: &mut [ActorRef<RaftMessage<LogEntry>>],
    me: &ActorRef<RaftMessage<LogEntry>>,
    message: RaftMessage<LogEntry>,
)-> bool
where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + Clone + 'static,
{
    match message {
        RaftMessage::AppendEntriesClient(mut append_entries_client_rpc) => {
            tracing::info!("⏭️ Received a message from a client, replicating it to the other nodes");
            let append_entries_msg = RaftMessage::AppendEntries(AppendEntriesRPC {
                term: common_data.current_term,
                leader_ref: me.clone(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: append_entries_client_rpc.entries.clone(),
                leader_commit: 0,
            });
            for peer in peer_refs.iter_mut() {
                let _ = peer.try_send(append_entries_msg.clone());
            }
            // TOASK: send the confirmation here or when it's committed?
            // (sending it here for now as there is no commit yet)
            let msg = RaftMessage::AppendEntriesClientResponse(Ok(()));
            let _ = append_entries_client_rpc.client_ref.try_send(msg);
        }
        RaftMessage::AppendEntries(append_entries_rpc) => {
            tracing::trace!("Received an AppendEntries message as the leader, somone challenged me");
            // TOASK: should it be >= ?
            if append_entries_rpc.term > common_data.current_term {
                tracing::trace!("They are right, I'm stepping down");
                return true;
            } else {
                tracing::trace!("They are wrong, long live the king!");
            }
        }
        RaftMessage::RequestVote(mut request_vote_rpc) => {
            tracing::trace!("Received a request vote message as the leader, somone challenged me");
            // TOASK: should it be >= ?
            let step_down_from_lead = request_vote_rpc.term > common_data.current_term;
            let msg = RaftMessage::RequestVoteResponse(step_down_from_lead);
            let _ = request_vote_rpc.candidate_ref.try_send(msg);
            if step_down_from_lead {
                tracing::trace!("They are right, granted vote and stepping down");
                common_data.voted_for = Some(request_vote_rpc.candidate_ref);
                return true;
            } else {
                tracing::trace!("They are wrong, long live the king!");
            }
        }
        RaftMessage::AppendEntryResponse(_term, _success) => {
            tracing::trace!("✔️ Received an AppendEntryResponse message");
        }
        // normal to recieve some extra votes if we just got elected but we don't care
        RaftMessage::RequestVoteResponse(_) => {}
        _ => {
            tracing::trace!(unhandled = ?message);
        }
    }
    false
}

// handles messages as leader
async fn message_handler<AB, LogEntry>(
    cell: &mut AB,
    common_data: &mut CommonState<LogEntry>,
    peer_refs: &mut [ActorRef<RaftMessage<LogEntry>>],
    me: &ActorRef<RaftMessage<LogEntry>>,
) where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + Clone + 'static,
{
    loop {
        let message = cell.recv().await.message().expect("raft runs indefinitely");

        match message {
            RaftMessage::AppendEntriesClient(mut append_entries_client_rpc) => {
                tracing::info!("⏭️ Received a message from a client, replicating it to the other nodes");
                let append_entries_msg = RaftMessage::AppendEntries(AppendEntriesRPC {
                    term: common_data.current_term,
                    leader_ref: me.clone(),
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: append_entries_client_rpc.entries.clone(),
                    leader_commit: 0,
                });
                for peer in peer_refs.iter_mut() {
                    let _ = peer.try_send(append_entries_msg.clone());
                }
                // TOASK: send the confirmation here or when it's committed?
                // (sending it here for now as there is no commit yet)
                let msg = RaftMessage::AppendEntriesClientResponse(Ok(()));
                let _ = append_entries_client_rpc.client_ref.try_send(msg);
            }
            RaftMessage::AppendEntries(append_entries_rpc) => {
                tracing::trace!("Received an AppendEntries message as the leader, somone challenged me");
                // TOASK: should it be >= ?
                if append_entries_rpc.term > common_data.current_term {
                    tracing::trace!("They are right, I'm stepping down");
                    break;
                } else {
                    tracing::trace!("They are wrong, long live the king!");
                }
            }
            RaftMessage::RequestVote(mut request_vote_rpc) => {
                tracing::trace!("Received a request vote message as the leader, somone challenged me");
                // TOASK: should it be >= ?
                let step_down_from_lead = request_vote_rpc.term > common_data.current_term;
                let msg = RaftMessage::RequestVoteResponse(step_down_from_lead);
                let _ = request_vote_rpc.candidate_ref.try_send(msg);
                if step_down_from_lead {
                    tracing::trace!("They are right, granted vote and stepping down");
                    common_data.voted_for = Some(request_vote_rpc.candidate_ref);
                    break;
                } else {
                    tracing::trace!("They are wrong, long live the king!");
                }
            }
            RaftMessage::AppendEntryResponse(_term, _success) => {
                tracing::trace!("✔️ Received an AppendEntryResponse message");
            }
            // normal to recieve some extra votes if we just got elected but we don't care
            RaftMessage::RequestVoteResponse(_) => {}
            _ => {
                tracing::trace!(unhandled = ?message);
            }
        }
    }
}
