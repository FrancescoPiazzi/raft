use std::cmp::max;
use std::time::Duration;

use tokio::task::JoinSet;

use crate::common_state::CommonState;
use crate::messages::*;
use actum::prelude::*;

use super::common::commit;
use std::collections::HashMap;

#[derive(Clone)]
struct Follower<LogEntry> {
    actor_ref: ActorRef<RaftMessage<LogEntry>>,
    next_index: u64,
    match_index: u64,
}

// the leader is the interface of the cluster to the external world
// clients send messages to the leader, which is responsible for replicating them to the other nodes
// after receiving confirmation from the majority of the nodes, the leader commits the message as agreed
// returns when another leader or candidate with a higher term is detected
pub async fn leader<AB, LogEntry>(
    cell: &mut AB,
    common_data: &mut CommonState<LogEntry>,
    peer_refs: &[ActorRef<RaftMessage<LogEntry>>],
    peer_ids: &[String],
    me: &ActorRef<RaftMessage<LogEntry>>,
    heartbeat_period: Duration,
    _replication_period: Duration,
) where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + Clone + 'static,
{
    let mut followers: HashMap<String, Follower<LogEntry>> = peer_refs
        .iter()
        .zip(peer_ids.iter())
        .map(|(peer, id)| (id.clone(), Follower {
            actor_ref: peer.clone(),
            next_index: 0,
            match_index: 0,
        }))
        .collect();

    let mut followers_timeouts = JoinSet::new();
    for follower in followers.keys() {
        followers_timeouts.spawn(wait_and_return(follower.clone(), heartbeat_period));
    }

    loop {
        tokio::select! {
            message = cell.recv() => {
                let message = message.message().expect("raft runs indefinitely");
                let become_follow = handle_message(common_data, message, &mut followers);
                if become_follow {
                    break;
                }
            },
            timed_out_follower = followers_timeouts.join_next() => {
                let timed_out_follower_id = timed_out_follower
                    .expect("leaders must have at least one follower")
                    .unwrap();
                let timed_out_follower = followers.get_mut(&timed_out_follower_id)
                    .expect("timed out follower not found");
                update_follower(common_data, timed_out_follower, me);
                followers_timeouts.spawn(wait_and_return(timed_out_follower_id, heartbeat_period));
            },
        }
    }
}

// returns the given object after some time, useful to keep track of follower timeouts
async fn wait_and_return<T>(x: T, heartbeat_period: Duration) -> T 
where T: Send + Clone + 'static
{
    tokio::time::sleep(heartbeat_period).await;
    x
}

// send an appendentriesRPC to a single follower
fn update_follower<LogEntry>(
    common_data: &CommonState<LogEntry>,
    follower: &mut Follower<LogEntry>,
    me: &ActorRef<RaftMessage<LogEntry>>,
) where
    LogEntry: Send + Clone + 'static,
{
    // TODO: maybe try avoiding adding the term after each logentry before so I don't have to strip them here
    let stuff_to_send = common_data.log[follower.next_index as usize..]
        .iter()
        .map(|x| x.0.clone())
        .collect();

    // TODO: fill the rest of the fields
    let append_entries_msg = RaftMessage::AppendEntries(AppendEntriesRPC {
        term: common_data.current_term,
        leader_ref: me.clone(),
        prev_log_index: follower.next_index,
        prev_log_term: if common_data.log.is_empty() {
            0
        } else {
            common_data.log[max(follower.next_index as i64 - 1, 0) as usize].1
        },
        entries: stuff_to_send,
        leader_commit: common_data.commit_index as u64,
    });

    let _ = follower.actor_ref.try_send(append_entries_msg);
}

// handles one message as leader
// returns true if we have to go back to a follower state, false otherwise
fn handle_message<LogEntry>(
    common_data: &mut CommonState<LogEntry>,
    message: RaftMessage<LogEntry>,
    follower_states: &mut HashMap<String, Follower<LogEntry>>,
) -> bool
where
    LogEntry: Send + Clone + 'static,
{
    match message {
        RaftMessage::AppendEntriesClient(mut append_entries_client_rpc) => {
            tracing::info!("⏭️ Received a message from a client, replicating it to the other nodes");

            let mut entries = append_entries_client_rpc
                .entries
                .clone()
                .into_iter()
                .map(|entry| (entry, common_data.current_term))
                .collect();
            common_data.log.append(&mut entries);

            // TODO: send the confirmation when it's committed instead of here
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
        RaftMessage::AppendEntryResponse(follower_id, _term, success) => {
            tracing::trace!("✔️ Received an AppendEntryResponse message");

            let follower = follower_states.get_mut(&follower_id).expect("recieved a message from an unkown follower");

            if success {
                // common_data.log.len() might be 0 when getting a heartbeat response before any entry is added
                follower.match_index = (std::cmp::max(common_data.log.len() as i64 - 1, 0)) as u64;
                follower.next_index = common_data.log.len() as u64;

                check_for_commits(common_data, follower_states);
            } else {
                follower.next_index -= 1;
            }
        }
        // normal to recieve some extra votes if we just got elected but we don't care
        RaftMessage::RequestVoteResponse(_) => {}
        _ => {
            tracing::trace!(unhandled = ?message);
        }
    }
    false
}

// commits all the log entries that are replicated on the majority of the nodes
fn check_for_commits<LogEntry>(common_data: &mut CommonState<LogEntry>, follower_states: &HashMap<String, Follower<LogEntry>>)
where
    LogEntry: Send + Clone + 'static,
{
    let mut i = common_data.commit_index + 1;
    // len() check probably useless in theory but better safe than sorry I guess
    while i < common_data.log.len() 
        && common_data.log[i].1 == common_data.current_term 
        && majority(follower_states, i)
    {
        common_data.commit_index += 1;
        i += 1;
    }
    commit(common_data);
}

// returns true if most the followers have the log entry at index i
fn majority<LogEntry>(follower_states: &HashMap<String, Follower<LogEntry>>, i: usize) -> bool
where
    LogEntry: Send + Clone + 'static,
{
    let mut count = 0;
    for follower in follower_states.values() {
        if follower.match_index >= i as u64 {
            count += 1;
        }
    }
    count >= follower_states.len() / 2
}