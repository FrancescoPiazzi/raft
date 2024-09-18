use std::time::Duration;

use tokio::task::JoinSet;

use crate::raft::common_state::CommonState;
use crate::raft::messages::*;
use actum::prelude::*;


// the leader is the interface of the cluster to the external world
// clients send messages to the leader, which is responsible for replicating them to the other nodes
// after receiving confirmation from the majority of the nodes, the leader commits the message as agreed
// returns when another leader or candidate with a higher term is detected
pub async fn leader<AB, LogEntry>(
    cell: &mut AB,
    common_data: &mut CommonState<LogEntry>,
    peer_refs: &mut [ActorRef<RaftMessage<LogEntry>>],
    me: &ActorRef<RaftMessage<LogEntry>>,
    heartbeat_period: Duration,
) where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + Clone + 'static,
{
    let mut peer_timeouts = JoinSet::new();
    peer_refs.iter_mut().for_each(|peer| {
        peer_timeouts.spawn(wait_and_return(peer.clone(), heartbeat_period));
    });

    loop {
        tokio::select! {
            message = cell.recv() => {
                let message = message.message().expect("raft runs indefinitely");
                let become_follow = handle_message(common_data, peer_refs, me, message);
                if become_follow {
                    break;
                }
            },
            peer = peer_timeouts.join_next() => {
                let mut peer = peer.expect("leaders must have at least one follower").unwrap();
                send_heartbeat(common_data.current_term, &mut peer, me);
                peer_timeouts.spawn(wait_and_return(peer, heartbeat_period));
            },
        }
    }
}

// returns the given ActorRef after some time, used to send heartbeats to followers
async fn wait_and_return<LogEntry>(peer: ActorRef<RaftMessage<LogEntry>>, heartbeat_period: Duration) -> ActorRef<RaftMessage<LogEntry>> 
where LogEntry: Send + Clone + 'static
{
    tokio::time::sleep(heartbeat_period).await;
    peer
}

// send a heartbeat to a single follower
fn send_heartbeat<LogEntry>(
    current_term: u64,
    peer: &mut ActorRef<RaftMessage<LogEntry>>,
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
    let _ = peer.try_send(heartbeat_msg);
}


// handles one message as leader
// returns true if we have to go back to a follower state, false otherwise
fn handle_message<LogEntry>(
    common_data: &mut CommonState<LogEntry>,
    peer_refs: &mut [ActorRef<RaftMessage<LogEntry>>],
    me: &ActorRef<RaftMessage<LogEntry>>,
    message: RaftMessage<LogEntry>,
)-> bool
where
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
