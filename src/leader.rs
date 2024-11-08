use std::cmp::max;
use std::collections::{BTreeMap, VecDeque as Queue};
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

/// the leader is the interface of the cluster to the external world
/// clients send messages to the leader, which is responsible for replicating them to the other nodes
/// after receiving confirmation from the majority of the nodes, the leader commits the message as agreed
/// returns when another leader or candidate with a higher term is detected
pub async fn leader<'a, AB, SM, SMin, SMout>(
    cell: &mut AB,
    me: u32,
    common_state: &mut CommonState<SM, SMin, SMout>,
    peers: &'a mut BTreeMap<u32, ActorRef<RaftMessage<SMin>>>,
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

    // Tracks the oneshot that we have to answer on per entry group
    let mut client_per_entry_group = BTreeMap::<usize, oneshot::Sender<AppendEntriesClientResponse<SMin>>>::new();

    loop {
        tokio::select! {
            message = cell.recv() => {
                let message = message.message().expect("raft runs indefinitely");
                let become_follower = handle_message(
                    me,
                    common_state,
                    peers,
                    &mut peers_state,
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
    messages_len: &mut Queue<usize>,
    follower_ref: &mut ActorRef<RaftMessage<SMin>>,
    next_index: u64,
) where
    SMin: Clone + Send + 'static,
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

// handles one message as leader
// returns true if we have to go back to a follower state, false otherwise
fn handle_message<SM, SMin, SMout>(
    me: u32,
    common_state: &mut CommonState<SM, SMin, SMout>,
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin>>>,
    peers_state: &mut BTreeMap<u32, PeerState>,
    client_per_entry_group: &mut BTreeMap<usize, oneshot::Sender<AppendEntriesClientResponse<SMin>>>,
    message: RaftMessage<SMin>,
) -> bool
where
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Send + Clone + 'static,
{
    tracing::trace!(message = ?message);

    match message {
        RaftMessage::AppendEntriesClientRequest(append_entries_client) => {
            tracing::debug!("Received a client message, replicating it");

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
            tracing::debug!("Received an AppendEntries message as the leader, somone challenged me");
            if append_entries_rpc.term > common_state.current_term
                || (append_entries_rpc.term == common_state.current_term
                    && append_entries_rpc.leader_commit >= common_state.commit_index as u64)
            {
                tracing::debug!("They are right, I'm stepping down");
                common_state.current_term = append_entries_rpc.term;
                return true;
            } else {
                tracing::debug!("They are wrong, long live the king!");
            }
        }
        RaftMessage::RequestVoteRequest(request_vote_rpc) => {
            tracing::debug!("Received a request vote message as the leader, somone challenged me");
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
                common_state.current_term = request_vote_rpc.term;
                return true;
            } else {
                tracing::debug!("They are wrong, long live the king!");
            }
        }
        RaftMessage::AppendEntriesReply(reply) => {
            let peer_state = peers_state
                .get_mut(&reply.from)
                .expect("recieved a message from an unkown peer");
            let peer_next_idx = &mut peer_state.next_index;
            let peer_match_idx = &mut peer_state.match_index;

            if reply.success {
                // should always be Some, since we always push a value before each append entries request
                let request_len = peer_state.messages_len.pop_front().unwrap_or(0);
                tracing::trace!(
                    "Received success from follower {} for {} entries",
                    reply.from,
                    request_len
                );
                *peer_match_idx = request_len as u64 + *peer_next_idx - 1;
                *peer_next_idx = *peer_match_idx + 1;

                try_commit_log_entries(common_state, peers_state, client_per_entry_group);
            } else {
                *peer_next_idx -= 1;
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

/// Commits the log entries that have been replicated on the majority of the servrers.
fn try_commit_log_entries<SM, SMin, SMout>(
    common_data: &mut CommonState<SM, SMin, SMout>,
    peers_state: &BTreeMap<u32, PeerState>,
    client_per_entry_group: &mut BTreeMap<usize, oneshot::Sender<AppendEntriesClientResponse<SMin>>>,
) where
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Clone,
{
    let mut i = common_data.commit_index + 1;
    while i <= common_data.log.len()
        && common_data.log.get_term(i) == common_data.current_term
        && majority(peers_state, i as u64)
    {
        i += 1;
    }
    common_data.commit_index = max(i - 1, 0);

    // TODO: this here does not help performance, but moving it out would mean taking
    // an empty parameter for "no reason"
    let mut newly_committed_entries = Some(Vec::new());
    common_data.commit(&mut newly_committed_entries);

    for i in newly_committed_entries.unwrap() {
        if let Some(sender) = client_per_entry_group.remove(&i) {
            let _ = sender.send(AppendEntriesClientResponse::Ok(()));
        }
    }
}

// returns true if most the followers have the log entry at index i
// TOASK: this could be optimized for large groups of entries being sent together
// by getting the median match_index instead of checking all of them, worth it?
fn majority(peers_state: &BTreeMap<u32, PeerState>, i: u64) -> bool {
    let mut count = 1; // count ourselves
    for peer_state in peers_state.values() {
        if peer_state.match_index >= i {
            count += 1;
        }
    }
    count > (peers_state.len() + 1) / 2
}
