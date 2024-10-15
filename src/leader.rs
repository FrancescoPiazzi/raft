use std::cmp::max;
use std::collections::{BTreeMap, VecDeque as Queue};
use std::time::Duration;

use actum::actor_bounds::ActorBounds;
use actum::actor_ref::ActorRef;
use tokio::task::JoinSet;
use tokio::sync::oneshot;

use crate::common_state::CommonState;
use crate::messages::append_entries::AppendEntriesRequest;
use crate::messages::*;
use crate::types::AppendEntriesClientResponse;

// the leader is the interface of the cluster to the external world
// clients send messages to the leader, which is responsible for replicating them to the other nodes
// after receiving confirmation from the majority of the nodes, the leader commits the message as agreed
// returns when another leader or candidate with a higher term is detected
pub async fn leader<'a, AB, LogEntry>(
    cell: &mut AB,
    me: u32,
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
        next_index.insert(*follower_id, common_state.log.len() as u64 + 1);
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

    // Track the length of log entries sent to each follower but not yet acknowledged.
    // This is used to calculate next_index. If an AppendEntriesResponse is lost, the follower
    // will correct us if next_index is too high, or overwrite logs if it's too low.
    // The queue handles multiple messages sent before receiving a response, assuming
    // replies are generally in order. If not, the same correction logic applies.
    let mut messages_len_per_follower = BTreeMap::<u32, Queue<usize>>::new();
    for follower_id in peers.keys() {
        messages_len_per_follower.insert(*follower_id, Queue::new());
    }

    // Tracks the oneshot that we have to answer on per entry group
    let mut client_per_entry_group = BTreeMap::<usize, oneshot::Sender<AppendEntriesClientResponse<LogEntry>>>::new();

    loop {
        tokio::select! {
            message = cell.recv() => {
                let message = message.message().expect("raft runs indefinitely");
                let become_follower = handle_message(
                    me,
                    common_state,
                    peers,
                    &mut next_index,
                    &mut match_index,
                    &mut messages_len_per_follower,
                    &mut client_per_entry_group,
                    message
                );
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
                let messages_len = messages_len_per_follower.get_mut(&timed_out_follower_id).expect("all followers have a message len queue");
                send_append_entries_request(me, common_state, messages_len, timed_out_follower_ref, next_index_of_follower);

                follower_timeouts.spawn(async move {
                    tokio::time::sleep(heartbeat_period).await;
                    timed_out_follower_id
                });
            },
        }
    }
}

fn send_append_entries_request<LogEntry>(
    me: u32,
    common_state: &CommonState<LogEntry>,
    messages_len: &mut Queue<usize>,
    follower_ref: &mut ActorRef<RaftMessage<LogEntry>>,
    next_index: u64,
) where
    LogEntry: Clone + Send + 'static,
{
    let entries_to_send = common_state.log[next_index as usize..].to_vec();

    messages_len.push_back(entries_to_send.len());
    tracing::trace!("Sending {} entries to follower", entries_to_send.len());

    let request = AppendEntriesRequest::<LogEntry> {
        term: common_state.current_term,
        leader_id: me,
        prev_log_index: next_index,
        // TODO: move all this if into a method in Log
        prev_log_term: if common_state.log.is_empty() {
            0
        } else {
            common_state.log.get_term(max(next_index as i64 - 1, 1) as usize)
        },
        entries: entries_to_send,
        leader_commit: common_state.commit_index as u64,
    };

    let _ = follower_ref.try_send(request.into());
}

// handles one message as leader
// returns true if we have to go back to a follower state, false otherwise
fn handle_message<LogEntry>(
    me: u32,
    common_state: &mut CommonState<LogEntry>,
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<LogEntry>>>,
    next_index: &mut BTreeMap<u32, u64>,
    match_index: &mut BTreeMap<u32, u64>,
    messages_len_per_follower: &mut BTreeMap<u32, Queue<usize>>,
    client_per_entry_group: &mut BTreeMap<usize, oneshot::Sender<AppendEntriesClientResponse<LogEntry>>>,
    message: RaftMessage<LogEntry>,
) -> bool
where
    LogEntry: Send + Clone + 'static,
{
    tracing::trace!(message = ?message);

    match message {
        RaftMessage::AppendEntriesClientRequest(append_entries_client) => {
            tracing::debug!("Received a client message, replicating it");

            // empty requests can cause problems when keeping track of the sender per request
            if append_entries_client.entries_to_replicate.is_empty() {
                return false;
            }

            common_state.log.append(append_entries_client.entries_to_replicate, common_state.current_term);
            client_per_entry_group.insert(common_state.log.len(), append_entries_client.reply_to);
        }
        RaftMessage::AppendEntriesRequest(append_entries_rpc) => {
            tracing::debug!("Received an AppendEntries message as the leader, somone challenged me");
            // TOASK: should it be >= ?
            // pretty sure it should be >, and if they are == we check commit_index
            if append_entries_rpc.term > common_state.current_term {
                tracing::debug!("They are right, I'm stepping down");
                return true;
            } else {
                tracing::debug!("They are wrong, long live the king!");
            }
        }
        RaftMessage::RequestVoteRequest(request_vote_rpc) => {
            tracing::debug!("Received a request vote message as the leader, somone challenged me");
            // TOASK: should it be >= ?
            // pretty sure it should be >, and if they are == we check commit_index
            let step_down_from_lead = request_vote_rpc.term > common_state.current_term;
            let msg = request_vote::RequestVoteReply {
                from: me,
                vote_granted: step_down_from_lead,
            };
            let candidate_ref = peers
                .get_mut(&request_vote_rpc.candidate_id)
                .expect("recieved a message from an unkown peer");
            let _ = candidate_ref.try_send(msg.into());
            if step_down_from_lead {
                tracing::debug!("They are right, granted vote and stepping down");
                common_state.voted_for = Some(request_vote_rpc.candidate_id);
                return true;
            } else {
                tracing::debug!("They are wrong, long live the king!");
            }
        }
        RaftMessage::AppendEntriesReply(reply) => {
            let next_idx = next_index
                .get_mut(&reply.from)
                .expect("recieved a message from an unkown peer");
            let match_idx = match_index
                .get_mut(&reply.from)
                .expect("recieved a message from an unkown peer");

            if reply.success {
                let n_logentry_request = messages_len_per_follower
                    .get_mut(&reply.from)
                    .expect("recieved a message from an unkown peer")
                    .pop_front()
                    .unwrap_or(0); // TODO: should always be Some, since we always push a value before each append entries request

                tracing::trace!("Received success from follower {} for {} entries", reply.from, n_logentry_request);
                // follow has match index = 4, and next index = 5
                // follow is excpecting 5, we send 3 entries: 5, 6, 7
                // when we get a response, match is 7 (n_logentry_request + next_idx - 1) and next is 8 (match + 1)
                *match_idx = n_logentry_request as u64 + *next_idx - 1;
                *next_idx = *match_idx + 1;

                check_for_commits(common_state, match_index, client_per_entry_group);
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
    match_index: &BTreeMap<u32, u64>,
    client_per_entry_group: &mut BTreeMap<usize, oneshot::Sender<AppendEntriesClientResponse<LogEntry>>>,
) where
    LogEntry: Send + Clone + 'static,
{
    // length check is probably useless but better safe than sorry TODO: pretty sure there should be a +1 somewhere here
    while common_data.commit_index < common_data.log.len()
        /* && common_data.log.get_term(common_data.commit_index) == common_data.current_term */ // TODO: this needs to be added, but for now it crashes when the first entry is added, prob an off by 1 error
        && majority(match_index, common_data.commit_index as u64 + 1)
    {
        common_data.commit_index += 1;
    }

    // TODO: this here does not help performance, but moving it out would mean taking
    // an empty parameter for "no reason"
    let mut newly_committed_entries = Some(Vec::new());
    common_data.commit( &mut newly_committed_entries);

    for i in newly_committed_entries.unwrap() {
        if let Some(sender) = client_per_entry_group.remove(&i) {
            let _ = sender.send(AppendEntriesClientResponse::Ok(()));
        }
    }
}

// returns true if most the followers have the log entry at index i
// TODO: this could be optimized for large groups of entries being sent together
// by getting the median match_index instead of checking all of them
fn majority(match_index: &BTreeMap<u32, u64>, i: u64) -> bool {
    let mut count = 1;  // count ourselves
    for match_idx in match_index.values() {
        if *match_idx >= i {
            count += 1;
        }
    }
    // first +1 for self to get n servers, second +1 for majority
    count >= (match_index.len() + 1) / 2 + 1
}