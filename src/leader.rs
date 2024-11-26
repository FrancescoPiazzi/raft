use std::cmp::max;
use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use actum::actor_bounds::ActorBounds;
use actum::actor_ref::ActorRef;
use peer_state::PeerState;
use tokio::sync::oneshot;
use tokio::task::JoinSet;

use crate::common_state::CommonState;
use crate::messages::append_entries::AppendEntriesRequest;
use crate::messages::*;
use crate::state_machine::StateMachine;
use crate::types::AppendEntriesClientResponse;

mod peer_state;

/// Behavior of the Raft server in leader state.
///
/// Returns when a message with a higher term is received.
pub async fn leader_behavior<AB, SM, SMin, SMout>(
    cell: &mut AB,
    me: u32,
    common_state: &mut CommonState<SM, SMin, SMout>,
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin>>>,
    heartbeat_period: Duration,
) where
    SM: StateMachine<SMin, SMout> + Send,
    AB: ActorBounds<RaftMessage<SMin>>,
    SMin: Clone + Send + 'static,
    SMout: Send,
{
    let mut peers_state = BTreeMap::new();
    for (id, _) in peers.iter_mut() {
        peers_state.insert(*id, PeerState::new((common_state.log.len() + 1) as u64));
    }

    let mut follower_timeouts = JoinSet::new();
    for follower_id in peers.keys() {
        let follower_id = *follower_id;
        follower_timeouts.spawn(async move {
            tokio::time::sleep(heartbeat_period).await;
            follower_id // return id of timed out follower
        });
    }

    // optimization: used as buffer where to append newly commited entries.
    let mut newly_committed_entries_buf = Vec::<usize>::new();

    // Tracks the oneshot that we have to answer on per entry group
    let mut client_per_entry_group = BTreeMap::<usize, oneshot::Sender<AppendEntriesClientResponse<SMin>>>::new();

    loop {
        tokio::select! {
            message = cell.recv() => {
                let message = message.message().expect("raft runs indefinitely");
                let step_down = handle_message_as_leader(
                    me,
                    common_state,
                    peers,
                    &mut peers_state,
                    &mut client_per_entry_group,
                    &mut newly_committed_entries_buf,
                    message
                );
                if step_down {
                    tracing::trace!("step down");
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
                let timed_out_follower_state = peers_state.get_mut(&timed_out_follower_id).expect("all peers are known");
                let next_index_of_follower = timed_out_follower_state.next_index;
                let messages_len = &mut timed_out_follower_state.messages_len;
                send_append_entries_request(me, common_state, messages_len, timed_out_follower_ref, next_index_of_follower);

                follower_timeouts.spawn(async move {
                    tokio::time::sleep(heartbeat_period).await;
                    timed_out_follower_id
                });
            },
        }
    }
}

fn send_append_entries_request<SM, SMin, SMout>(
    me: u32,
    common_state: &CommonState<SM, SMin, SMout>,
    messages_len: &mut VecDeque<usize>,
    follower_ref: &mut ActorRef<RaftMessage<SMin>>,
    next_index: u64,
) where
    SMin: Clone + Send + 'static,
{
    let mut entries_to_send = Vec::new();
    // TODO/TOASK: this should always be the case (I think), but it's not
    // if next_index <= common_state.log.len() as u64{
        entries_to_send = common_state.log[next_index as usize..].to_vec();
    // }

    messages_len.push_back(entries_to_send.len());
    tracing::trace!("Sending {} entries to follower", entries_to_send.len());

    let request = AppendEntriesRequest::<SMin> {
        term: common_state.current_term,
        leader_id: me,
        prev_log_index: next_index - 1,
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

/// Returns true if we should step down, false otherwise.
fn handle_message_as_leader<SM, SMin, SMout>(
    me: u32,
    common_state: &mut CommonState<SM, SMin, SMout>,
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin>>>,
    peers_state: &mut BTreeMap<u32, PeerState>,
    client_per_entry_group: &mut BTreeMap<usize, oneshot::Sender<AppendEntriesClientResponse<SMin>>>,
    newly_committed_entries_buf: &mut Vec<usize>,
    message: RaftMessage<SMin>,
) -> bool
where
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Send + Clone + 'static,
{
    tracing::trace!(message = ?message);

    match message {
        RaftMessage::AppendEntriesClientRequest(append_entries_client) => {
            tracing::trace!("Received a client message, replicating it");

            // empty requests can cause problems when keeping track of the sender per request
            if append_entries_client.entries_to_replicate.is_empty() {
                return false;
            }

            common_state
                .log
                .append(append_entries_client.entries_to_replicate, common_state.current_term);
            client_per_entry_group.insert(common_state.log.len(), append_entries_client.reply_to);
        }
        RaftMessage::AppendEntriesRequest(append_entries_rpc) => {
            if append_entries_rpc.term > common_state.current_term
                || (append_entries_rpc.term == common_state.current_term
                    && append_entries_rpc.leader_commit >= common_state.commit_index as u64)
            {
                common_state.current_term = append_entries_rpc.term;
                return true;
            }
        }
        RaftMessage::RequestVoteRequest(request_vote_rpc) => {
            let step_down = request_vote_rpc.term > common_state.current_term;
            let reply = request_vote::RequestVoteReply {
                from: me,
                vote_granted: step_down,
            };
            if let Some(candidate_ref) = peers.get_mut(&request_vote_rpc.candidate_id) {
                let _ = candidate_ref.try_send(reply.into());
            }
            if step_down {
                common_state.voted_for = Some(request_vote_rpc.candidate_id);
                common_state.current_term = request_vote_rpc.term;
                return true;
            }
        }
        RaftMessage::AppendEntriesReply(reply) => {
            let Some(peer_state) = peers_state.get_mut(&reply.from) else {
                return false;
            };
            let peer_next_idx = &mut peer_state.next_index;
            let peer_match_idx = &mut peer_state.match_index;

            if reply.success {
                // should always be Some, since we always push a value before each append entries request
                let request_len = peer_state.messages_len.pop_front().unwrap_or(0);
                *peer_match_idx = request_len as u64 + *peer_next_idx - 1;
                *peer_next_idx = *peer_match_idx + 1;

                tracing::trace!("----> match idx: {}, next idx: {}", peer_match_idx, peer_next_idx);

                commit_log_entries_replicated_on_majority(
                    common_state,
                    peers_state,
                    client_per_entry_group,
                    newly_committed_entries_buf,
                );
            } else {
                *peer_next_idx -= 1;
            }
        }
        RaftMessage::RequestVoteReply(_) => {} // ignore extra votes if we were already elected
        _ => {
            tracing::trace!(unhandled = ?message);
        }
    }
    false
}

/// Commits the log entries that have been replicated on the majority of the servers.
fn commit_log_entries_replicated_on_majority<SM, SMin, SMout>(
    common_state: &mut CommonState<SM, SMin, SMout>,
    peers_state: &BTreeMap<u32, PeerState>,
    client_per_entry_group: &mut BTreeMap<usize, oneshot::Sender<AppendEntriesClientResponse<SMin>>>,
    newly_committed_entries_buf: &mut Vec<usize>,
) where
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Clone,
{
    let mut i = common_state.commit_index + 1;
    while i <= common_state.log.len()
        && common_state.log.get_term(i) == common_state.current_term
        && majority_of_servers_have_log_entry(peers_state, i as u64)
    {
        i += 1;
    }
    common_state.commit_index = max(i - 1, 0);

    common_state.commit_log_entries_up_to_commit_index(Some(newly_committed_entries_buf));

    for entry in newly_committed_entries_buf {
        if let Some(sender) = client_per_entry_group.remove(&entry) {
            let _ = sender.send(AppendEntriesClientResponse::Ok(()));
        }
    }
}

/// Returns whether the majority of servers, including self, have the log entry at the given index.
fn majority_of_servers_have_log_entry(peers_state: &BTreeMap<u32, PeerState>, index: u64) -> bool {
    // TOASK: this could be optimized for large groups of entries being sent together
    // by getting the median match_index instead of checking all of them, worth it?
    let count_including_self = 1 + peers_state.values().filter(|state| state.match_index >= index).count();
    count_including_self > (peers_state.len() + 1) / 2
}
