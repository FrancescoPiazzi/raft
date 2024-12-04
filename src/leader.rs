use std::cmp::max;
use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use actum::actor_bounds::ActorBounds;
use actum::actor_ref::ActorRef;
use peer_state::PeerState;
use request_vote::RequestVoteReply;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::common_state::CommonState;
use crate::messages::append_entries::{AppendEntriesReply, AppendEntriesRequest};
use crate::messages::append_entries_client::AppendEntriesClientRequest;
use crate::messages::request_vote::RequestVoteRequest;
use crate::messages::*;
use crate::state_machine::StateMachine;
use crate::types::AppendEntriesClientResponse;

mod peer_state;

/// Behavior of the Raft server in leader state.
///
/// Returns when a message with a higher term is received, returning the message.
pub async fn leader_behavior<AB, SM, SMin, SMout>(
    cell: &mut AB,
    me: u32,
    common_state: &mut CommonState<SM, SMin, SMout>,
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin, SMout>>>,
    heartbeat_period: Duration,
) -> RaftMessage<SMin, SMout> where
    SM: StateMachine<SMin, SMout> + Send,
    AB: ActorBounds<RaftMessage<SMin, SMout>>,
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
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

    // optimization: used as buffer where to append the output of the state machine given the newly commited entries.
    let mut committed_entries_smout_buf = Vec::<SMout>::new();

    // Tracks the mpsc that we have to answer on per entry
    let mut client_channel_per_input = VecDeque::<mpsc::Sender<AppendEntriesClientResponse<SMin, SMout>>>::new();

    loop {
        tokio::select! {
            message = cell.recv() => {
                let message = message.message().expect("raft runs indefinitely");
                
                if handle_message_as_leader(
                    me,
                    common_state,
                    peers,
                    &mut peers_state,
                    &mut client_channel_per_input,
                    &mut committed_entries_smout_buf,
                    &message
                ) {
                    return message;
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
    follower_ref: &mut ActorRef<RaftMessage<SMin, SMout>>,
    next_index: u64,
) where
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    let entries_to_send = common_state.log[next_index as usize..].to_vec();

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
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin, SMout>>>,
    peers_state: &mut BTreeMap<u32, PeerState>,
    client_channel_per_input: &mut VecDeque<mpsc::Sender<AppendEntriesClientResponse<SMin, SMout>>>,
    committed_entries_smout_buf: &mut Vec<SMout>,
    message: &RaftMessage<SMin, SMout>,
) -> bool 
where
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Send + Clone + 'static,
    SMout: Send + 'static,
{
    tracing::trace!(message = ?message);

    if common_state.update_term(&message) {
        tracing::trace!("step down");
        return true;
    }

    match message {
        RaftMessage::AppendEntriesClientRequest(append_entries_client) => {
            handle_append_entries_client_request(common_state, client_channel_per_input, append_entries_client);
        }
        // the term is surely too low so I ignore it, had it been too high I would have caught it in the update_term check before
        RaftMessage::AppendEntriesRequest(_) => {}
        RaftMessage::RequestVoteRequest(request_vote_rpc) => {
            handle_request_vote_request(me, common_state, peers, request_vote_rpc)
        }
        RaftMessage::AppendEntriesReply(reply) => {
            handle_append_entries_reply(
                common_state,
                peers_state,
                client_channel_per_input,
                committed_entries_smout_buf,
                reply,
            );
        }
        RaftMessage::RequestVoteReply(_) => {} // ignore extra votes if we were already elected
        _ => {
            tracing::trace!(unhandled = ?message);
        }
    }

    false
}

#[tracing::instrument(level = "trace", skip_all)]
fn handle_append_entries_client_request<SM, SMin, SMout>(
    common_state: &mut CommonState<SM, SMin, SMout>,
    client_channel_per_input: &mut VecDeque<mpsc::Sender<AppendEntriesClientResponse<SMin, SMout>>>,
    request: &AppendEntriesClientRequest<SMin, SMout>,
) where
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Send + Clone + 'static,
{
    tracing::trace!("Received a client message, replicating it");

    for _ in 0..request.entries_to_replicate.len() {
        client_channel_per_input.push_back(request.reply_to.clone());
    }
    common_state
        .log
        .append(request.entries_to_replicate.clone(), common_state.current_term);
}

/// request vote requests are always refused as leaders, because if the term of the request
/// had been higher, we would have already turned back into a follower at the update_term check
#[tracing::instrument(level = "trace", skip_all)]
fn handle_request_vote_request<SM, SMin, SMout>(
    me: u32,
    common_state: &mut CommonState<SM, SMin, SMout>,
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin, SMout>>>,
    request: &RequestVoteRequest,
) where
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    if let Some(candidate_ref) = peers.get_mut(&request.candidate_id) {
        let _ = candidate_ref.try_send(
            RequestVoteReply {
                from: me,
                term: common_state.current_term,
                vote_granted: false,
            }
            .into(),
        );
    }
}

#[tracing::instrument(level = "trace", skip_all)]
fn handle_append_entries_reply<SM, SMin, SMout>(
    common_state: &mut CommonState<SM, SMin, SMout>,
    peers_state: &mut BTreeMap<u32, PeerState>,
    client_channel_per_input: &mut VecDeque<mpsc::Sender<AppendEntriesClientResponse<SMin, SMout>>>,
    committed_entries_smout_buf: &mut Vec<SMout>,
    reply: &AppendEntriesReply,
) where
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Clone,
{
    let Some(peer_state) = peers_state.get_mut(&reply.from) else {
        tracing::error!("Couldn't get the state of peer {}", reply.from);
        return;
    };
    let peer_next_idx = &mut peer_state.next_index;
    let peer_match_idx = &mut peer_state.match_index;

    if reply.success {
        // should always be Some, since we always push a value before each append entries request
        let request_len = peer_state.messages_len.pop_front().unwrap_or(0);
        *peer_match_idx = request_len as u64 + *peer_next_idx - 1;
        *peer_next_idx = *peer_match_idx + 1;

        tracing::trace!(
            "follower {}: match idx: {}, next idx: {}",
            reply.from,
            peer_match_idx,
            peer_next_idx
        );

        commit_log_entries_replicated_on_majority(
            common_state,
            peers_state,
            client_channel_per_input,
            committed_entries_smout_buf,
        );
    } else {
        *peer_next_idx -= 1;
    }
}

/// Commits the log entries that have been replicated on the majority of the servers.
fn commit_log_entries_replicated_on_majority<SM, SMin, SMout>(
    common_state: &mut CommonState<SM, SMin, SMout>,
    peers_state: &BTreeMap<u32, PeerState>,
    client_channel_per_input: &mut VecDeque<mpsc::Sender<AppendEntriesClientResponse<SMin, SMout>>>,
    committed_entries_smout_buf: &mut Vec<SMout>,
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

    let old_last_applied = common_state.last_applied;

    common_state.commit_log_entries_up_to_commit_index(Some(committed_entries_smout_buf));

    for _ in (old_last_applied + 1)..=common_state.commit_index {
        if let Some(sender) = client_channel_per_input.pop_front() {
            let _ = sender.try_send(AppendEntriesClientResponse(Ok(committed_entries_smout_buf
                .pop()
                .unwrap())));
        } else {
            tracing::error!("No client channel to send the response to");
        }
    }
    assert!(committed_entries_smout_buf.is_empty());
}

/// Returns whether the majority of servers, including self, have the log entry at the given index.
fn majority_of_servers_have_log_entry(peers_state: &BTreeMap<u32, PeerState>, index: u64) -> bool {
    // TOASK: this could be optimized for large groups of entries being sent together
    // by getting the median match_index instead of checking all of them, worth it?
    let count_including_self = 1 + peers_state.values().filter(|state| state.match_index >= index).count();
    count_including_self > (peers_state.len() + 1) / 2
}
