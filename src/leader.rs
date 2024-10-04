use std::cmp::max;
use std::time::Duration;

use tokio::task::{JoinError, JoinSet};

use crate::common_state::CommonState;
use crate::messages::*;
use crate::messages::append_entries::AppendEntriesRequest;
use crate::messages::request_vote::RequestVoteReply;

use actum::actor_bounds::ActorBounds;
use actum::actor_ref::ActorRef;
use std::collections::{BTreeMap, BTreeSet, HashMap};

// the leader is the interface of the cluster to the external world
// clients send messages to the leader, which is responsible for replicating them to the other nodes
// after receiving confirmation from the majority of the nodes, the leader commits the message as agreed
// returns when another leader or candidate with a higher term is detected
pub async fn leader<'a, AB, LogEntry>(
    cell: &mut AB,
    me: (u32, &mut ActorRef<RaftMessage<LogEntry>>),
    common_state: &mut CommonState<LogEntry>,
    peers: &'a mut BTreeMap<u32, ActorRef<RaftMessage<LogEntry>>>,
    heartbeat_period: Duration,
    _replication_period: Duration,
) where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Clone + Send + 'static,
{
    let mut next_index = BTreeMap::<u32, u64>::new();
    let mut match_index = BTreeMap::<u32, u64>::new();
    for follower_id in peers.keys() {
        next_index.insert(*follower_id, 0);
        match_index.insert(*follower_id, 0);
    }

    let mut follower_timeouts = JoinSet::new();

    for follower_id in peers.keys() {
        let follower_id = *follower_id;
        follower_timeouts.spawn(async move {
            tokio::time::sleep(heartbeat_period).await;
            follower_id // id of timed out follower
        });
    }

    loop {
        tokio::select! {
            message = cell.recv() => {
                let message = message.message().expect("raft runs indefinitely");
                let become_follower = handle_message(&me, common_state, peers, &mut next_index, &mut match_index, message);
                if become_follower {
                    break;
                }
            },
            timed_out_follower_id = follower_timeouts.join_next() => {
                let Some(timed_out_follower_id) = timed_out_follower_id else {
                    debug_assert!(peers.is_empty());
                    continue;
                };

                let timed_out_follower_id = timed_out_follower_id.unwrap();
                let timed_out_follower_ref = peers.get_mut(&timed_out_follower_id).expect("all peers are known");
                let next_index_of_follower = *next_index.get(&timed_out_follower_id).unwrap();
                let match_index_of_follower = *match_index.get(&timed_out_follower_id).unwrap();
                send_append_entries_request(&me, common_state, timed_out_follower_ref, next_index_of_follower, match_index_of_follower);

                follower_timeouts.spawn(async move {
                    tokio::time::sleep(heartbeat_period).await;
                    timed_out_follower_id
                });
            },
        }
    }
}

fn send_append_entries_request<LogEntry>(
    me: &(u32, &mut ActorRef<RaftMessage<LogEntry>>),
    common_state: &CommonState<LogEntry>,
    follower_ref: &mut ActorRef<RaftMessage<LogEntry>>,
    next_index: u64,
    match_index: u64,
) where
    LogEntry: Clone + Send + 'static,
{
    // TODO: maybe try avoiding adding the term after each logentry before so I don't have to strip them here
    let entries_to_send = common_state.log[next_index as usize..]
        .iter()
        .map(|entry| entry.0.clone())
        .collect::<Vec<LogEntry>>();

    // TODO: fill the rest of the fields
    let request = AppendEntriesRequest::<LogEntry> {
        term: common_state.current_term,
        leader_id: me.0,
        prev_log_index: next_index,
        prev_log_term: if common_state.log.is_empty() {
            0
        } else {
            common_state.log[max(next_index as usize - 1, 0)].1
        },
        entries: entries_to_send,
        leader_commit: common_state.commit_index as u64,
    };

    let _ = follower_ref.try_send(request.into());
}

// handles one message as leader
// returns true if we have to go back to a follower state, false otherwise
fn handle_message<LogEntry>(
    me: &(u32, &mut ActorRef<RaftMessage<LogEntry>>),
    common_state: &mut CommonState<LogEntry>,
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<LogEntry>>>,
    next_index: &mut BTreeMap<u32, u64>,
    match_index: &mut BTreeMap<u32, u64>,
    message: RaftMessage<LogEntry>,
) -> bool
where
    LogEntry: Send + Clone + 'static,
{
    tracing::debug!(message = ?message);

    match message {
        RaftMessage::AppendEntriesClientRequest(mut append_entries_client) => {
            let mut entries = append_entries_client
                .entries_to_replicate
                .into_iter()
                .map(|entry| (entry, common_state.current_term))
                .collect();
            common_state.log.append(&mut entries);
        }
        RaftMessage::AppendEntriesRequest(append_entries_rpc) => {
            tracing::trace!("Received an AppendEntries message as the leader, somone challenged me");
            // TOASK: should it be >= ?
            if append_entries_rpc.term > common_state.current_term {
                tracing::trace!("They are right, I'm stepping down");
                return true;
            } else {
                tracing::trace!("They are wrong, long live the king!");
            }
        }
        RaftMessage::RequestVoteRequest(request_vote_rpc) => {
            tracing::trace!("Received a request vote message as the leader, somone challenged me");
            // TOASK: should it be >= ?
            let step_down_from_lead = request_vote_rpc.term > common_state.current_term;
            let msg = RaftMessage::RequestVoteReply(request_vote::RequestVoteReply{from: me.0, vote_granted: step_down_from_lead});
            let candidate_ref = peers.get_mut(&request_vote_rpc.candidate_id).expect("recieved a message from an unkown peer");
            let _ = candidate_ref.try_send(msg);
            if step_down_from_lead {
                tracing::trace!("They are right, granted vote and stepping down");
                common_state.voted_for = Some(request_vote_rpc.candidate_id);
                return true;
            } else {
                tracing::trace!("They are wrong, long live the king!");
            }
        }
        RaftMessage::AppendEntriesReply(reply) => {
            tracing::trace!("✔️ Received an AppendEntryResponse message");

            let next_idx = next_index.get_mut(&reply.from).expect("recieved a message from an unkown peer");
            let match_idx = match_index.get_mut(&reply.from).expect("recieved a message from an unkown peer");

            if reply.success {
                // common_data.log.len() might be 0 when getting a heartbeat response before any entry is added
                *match_idx = std::cmp::max(common_state.log.len() as i64 - 1, 0) as u64;
                *next_idx = common_state.log.len() as u64;
            
                check_for_commits(common_state, next_index, match_index);
            } else {
                *next_idx -= 1;
            }
        }
        // normal to recieve some extra votes if we just got elected but we don't care
        RaftMessage::RequestVoteReply(_) => {}
        _ => {
            tracing::trace!(unhandled = ?message);
        }
    }
    false
}

// commits all the log entries that are replicated on the majority of the nodes
fn check_for_commits<LogEntry>(
    common_data: &mut CommonState<LogEntry>,
    next_index: &mut BTreeMap<u32, u64>,
    match_index: &mut BTreeMap<u32, u64>,
) where
    LogEntry: Send + Clone + 'static,
{
    let mut i = common_data.commit_index + 1;
    // len() check probably useless in theory but better safe than sorry I guess
    while i < common_data.log.len() && common_data.log[i].1 == common_data.current_term && majority(match_index, i as u64)
    {
        common_data.commit_index += 1;
        i += 1;
    }
    common_data.commit();
}

// returns true if most the followers have the log entry at index i
fn majority(match_index: &mut BTreeMap<u32, u64>, i: u64) -> bool
{
    let mut count = 0;
    for match_idx in match_index.values() {
        if *match_idx >= i {
            count += 1;
        }
    }
    count >= match_index.len() / 2
}
