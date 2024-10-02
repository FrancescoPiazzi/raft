use std::cmp::max;
use std::time::Duration;

use tokio::task::{JoinError, JoinSet};

use crate::common_state::CommonState;
use crate::messages::*;

use crate::messages::append_entries::AppendEntriesRequest;
use actum::actor_bounds::ActorBounds;
use actum::actor_ref::ActorRef;
use std::collections::{BTreeMap, BTreeSet, HashMap};

// the leader is the interface of the cluster to the external world
// clients send messages to the leader, which is responsible for replicating them to the other nodes
// after receiving confirmation from the majority of the nodes, the leader commits the message as agreed
// returns when another leader or candidate with a higher term is detected
pub async fn leader<AB, LogEntry>(
    cell: &mut AB,
    me: (u32, &mut ActorRef<RaftMessage<LogEntry>>),
    common_state: &mut CommonState<LogEntry>,
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<LogEntry>>>,
    heartbeat_period: Duration,
    replication_period: Duration,
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
        follower_timeouts.spawn(async {
            tokio::time::sleep(heartbeat_period).await;
            *follower_id // id of timed out follower
        });
    }

    loop {
        tokio::select! {
            message = cell.recv() => {
                let message = message.message().expect("raft runs indefinitely");
                let become_follower = handle_message(common_state, peers, &mut next_index, &mut match_index, message);
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
                send_append_entries_request(me, common_state, (timed_out_follower_id, timed_out_follower_ref));

                follower_timeouts.spawn(async {
                    tokio::time::sleep(heartbeat_period).await;
                    timed_out_follower_id
                });
            },
        }
    }
}

fn send_append_entries_request<LogEntry>(
    me: (u32, &mut ActorRef<RaftMessage<LogEntry>>),
    common_state: &CommonState<LogEntry>,
    follower: (u32, &mut ActorRef<LogEntry>),
    next_index: &BTreeMap<u32, u64>,
    match_index: &BTreeMap<u32, u64>,
) where
    LogEntry: Clone + Send + 'static,
{
    // TODO: maybe try avoiding adding the term after each logentry before so I don't have to strip them here
    let next_index_of_follower = *next_index.get(&follower.0).unwrap();

    let entries_to_send = common_state.log[next_index_of_follower as usize..]
        .iter()
        .map(|entry| entry.0.clone())
        .collect::<Vec<LogEntry>>();

    // TODO: fill the rest of the fields
    let request = AppendEntriesRequest::<LogEntry> {
        term: common_state.current_term,
        leader_id: me.0,
        prev_log_index: *next_index.get(&follower.0).unwrap(),
        prev_log_term: if common_state.log.is_empty() {
            0
        } else {
            common_state.log[max(next_index_of_follower as usize - 1, 0)].1
        },
        entries: entries_to_send,
        leader_commit: common_state.commit_index as u64,
    };

    let _ = follower.1.try_send(request.into());
}

// handles one message as leader
// returns true if we have to go back to a follower state, false otherwise
fn handle_message<LogEntry>(
    common_state: &mut CommonState<LogEntry>,
    peers: &mut BTreeSet<RaftMessage<LogEntry>>,
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

            // TODO: send the confirmation when it's committed instead of here
            let msg = RaftMessage::AppendEntriesClientResponse(Ok(()));
            let _ = append_entries_client.client_ref.try_send(msg);
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
        RaftMessage::RequestVoteRequest(mut request_vote_rpc) => {
            tracing::trace!("Received a request vote message as the leader, somone challenged me");
            // TOASK: should it be >= ?
            let step_down_from_lead = request_vote_rpc.term > common_state.current_term;
            let msg = RaftMessage::RequestVoteReply(step_down_from_lead);
            let _ = request_vote_rpc.candidate_ref.try_send(msg);
            if step_down_from_lead {
                tracing::trace!("They are right, granted vote and stepping down");
                common_state.voted_for = Some(request_vote_rpc.candidate_ref);
                return true;
            } else {
                tracing::trace!("They are wrong, long live the king!");
            }
        }
        RaftMessage::AppendEntriesReply(reply) => {
            tracing::trace!("✔️ Received an AppendEntryResponse message");

            let follower = follower_states
                .get_mut(&follower_id)
                .expect("recieved a message from an unkown follower");

            if success {
                // common_data.log.len() might be 0 when getting a heartbeat response before any entry is added
                follower.match_index = (std::cmp::max(common_state.log.len() as i64 - 1, 0)) as u64;
                follower.next_index = common_state.log.len() as u64;

                check_for_commits(common_state, follower_states);
            } else {
                follower.next_index -= 1;
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
    follower_states: &HashMap<u32, Follower<LogEntry>>,
) where
    LogEntry: Send + Clone + 'static,
{
    let mut i = common_data.commit_index + 1;
    // len() check probably useless in theory but better safe than sorry I guess
    while i < common_data.log.len() && common_data.log[i].1 == common_data.current_term && majority(follower_states, i)
    {
        common_data.commit_index += 1;
        i += 1;
    }
    common_data.commit();
}

// returns true if most the followers have the log entry at index i
fn majority<LogEntry>(follower_states: &HashMap<u32, Follower<LogEntry>>, i: usize) -> bool
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
