use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use actum::actor_bounds::{ActorBounds, Recv};
use actum::actor_ref::ActorRef;
use peer_state::PeerState;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::common_message_handling::{handle_append_entries_request, handle_vote_request, RaftState};
use crate::common_state::CommonState;
use crate::messages::append_entries::{AppendEntriesReply, AppendEntriesRequest};
use crate::messages::append_entries_client::AppendEntriesClientRequest;
use crate::messages::*;
use crate::state_machine::StateMachine;
use crate::types::AppendEntriesClientResponse;

mod peer_state;

#[derive(Debug, Eq, PartialEq)]
pub enum LeaderResult {
    Deposed,
    Stopped,
    NoMoreSenders,
}

/// Behavior of the Raft server in leader state.
pub async fn leader_behavior<AB, SM, SMin, SMout>(
    cell: &mut AB,
    common_state: &mut CommonState<SM, SMin, SMout>,
    heartbeat_period: Duration,
) -> LeaderResult
where
    SM: StateMachine<SMin, SMout> + Send,
    AB: ActorBounds<RaftMessage<SMin, SMout>>,
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    let mut peers_state = BTreeMap::new();
    for (id, _) in common_state.peers.iter_mut() {
        // TLA: 233-234 would want initial_next_index to be log.len()+1, however I think that in my version this might
        // create an inconsistent log in the following scenario: log(A) = [a, b, c], log(B) = [a, b, d], both have
        // commit_index = 2, A becomes the leader, recieves a replication request with x, the logs become
        // [a, b, c, x] and [a, b, d, x], if the majority recieves this new update, x is committed, along with the
        // inconsistent c/d at position 3, I think logcabin has checks to prevent this, since I don't
        // I just play it safer and use commit index instead of log length, they will be the same most of the time
        peers_state.insert(*id, PeerState::new((common_state.commit_index + 1) as u64));
    }

    let mut follower_timeouts = JoinSet::new();
    for follower_id in common_state.peers.keys() {
        let follower_id = *follower_id;
        follower_timeouts.spawn(async move {
            tokio::time::sleep(heartbeat_period).await;
            follower_id // return id of timed out follower
        });
    }

    // optimization: used as buffer where to append the output of the state machine given the newly commited entries.
    let mut committed_entries_smout_buf = Vec::<SMout>::new();

    // Tracks the mpsc that we have to answer on per entry
    let mut client_channel_per_input =
        VecDeque::<(mpsc::Sender<AppendEntriesClientResponse<SMin, SMout>>, usize)>::new();

    loop {
        tokio::select! {
            message = cell.recv() => {
                match message {
                    Recv::Message(message) => {
                        if handle_message_as_leader(
                            common_state,
                            &mut peers_state,
                            &mut client_channel_per_input,
                            &mut committed_entries_smout_buf,
                            message
                        ) {
                            return LeaderResult::Deposed;
                        }
                    }
                    Recv::Stopped(Some(message)) => {
                        tracing::trace!(unhandled = ?message);
                        while let Recv::Stopped(Some(message)) = cell.recv().await {
                            tracing::trace!(unhandled = ?message);
                        }
                        return LeaderResult::Stopped;
                    }
                    Recv::Stopped(None) => {
                        return LeaderResult::Stopped;
                    }
                    Recv::NoMoreSenders => return LeaderResult::NoMoreSenders,
                }
            },
            timed_out_follower_id = follower_timeouts.join_next() => {
                let Some(timed_out_follower_id) = timed_out_follower_id else {
                    debug_assert!(common_state.peers.is_empty());
                    continue;
                };

                let timed_out_follower_id = timed_out_follower_id.unwrap();
                let mut timed_out_follower_ref = 
                    common_state.peers.get_mut(&timed_out_follower_id).expect("all peers are known").clone();
                let timed_out_follower_state = 
                    peers_state.get_mut(&timed_out_follower_id).expect("all peers are known");
                let next_index_of_follower = timed_out_follower_state.next_index;
                send_append_entries_request(common_state, &mut timed_out_follower_ref, next_index_of_follower);

                follower_timeouts.spawn(async move {
                    tokio::time::sleep(heartbeat_period).await;
                    timed_out_follower_id
                });
            },
        }
    }
}

fn send_append_entries_request<SM, SMin, SMout>(
    common_state: &CommonState<SM, SMin, SMout>,
    follower_ref: &mut ActorRef<RaftMessage<SMin, SMout>>,
    next_index: u64,
) where
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    let entries_to_send = common_state.log[next_index as usize..].to_vec();

    let prev_log_index = next_index - 1; // TLA: 207
                                         // TLA: 208-211
    let prev_log_term = if prev_log_index == 0 {
        0
    } else {
        common_state.log.get_term(prev_log_index as usize)
    };

    // TLA: 215-225 (mcommitIndex   |-> Min({commitIndex[i], lastEntry}) is done when commit_index is updated)
    let request = AppendEntriesRequest::<SMin> {
        term: common_state.current_term,
        leader_id: common_state.me,
        prev_log_index,
        prev_log_term,
        entries: entries_to_send,
        leader_commit: common_state.commit_index as u64,
    };

    let _ = follower_ref.try_send(request.into());
}

/// Returns true if we should step down, false otherwise.
fn handle_message_as_leader<SM, SMin, SMout>(
    common_state: &mut CommonState<SM, SMin, SMout>,
    peers_state: &mut BTreeMap<u32, PeerState>,
    client_channel_per_input: &mut VecDeque<(mpsc::Sender<AppendEntriesClientResponse<SMin, SMout>>, usize)>,
    committed_entries_smout_buf: &mut Vec<SMout>,
    message: RaftMessage<SMin, SMout>,
) -> bool
where
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Send + Clone + 'static,
    SMout: Send + 'static,
{
    // tracing::trace!(message = ?message);

    match message {
        RaftMessage::AppendEntriesClientRequest(append_entries_client) => {
            handle_append_entries_client_request(common_state, client_channel_per_input, append_entries_client);
            false
        }
        RaftMessage::AppendEntriesRequest(append_entries_request) => {
            handle_append_entries_request(common_state, RaftState::Leader, append_entries_request)
        }
        RaftMessage::RequestVoteRequest(request_vote_rpc) => handle_vote_request(common_state, request_vote_rpc),
        RaftMessage::AppendEntriesReply(reply) => {
            handle_append_entries_reply(
                common_state,
                peers_state,
                client_channel_per_input,
                committed_entries_smout_buf,
                reply,
            );
            false
        }
        RaftMessage::RequestVoteReply(reply) => {
            // ignore extra votes if we were already elected
            tracing::trace!(extra_vote = ?reply);
            false
        }
        RaftMessage::PollState(request) => {
            let _ = request.reply_to.try_send(poll_state::PollStateResponse {
                state: poll_state::ServerStateOnlyForTesting::Leader,
            });
            false
        }
        _ => {
            tracing::trace!(unhandled = ?message);
            false
        }
    }
}

// TLA: 245-253 (add it to your log and that's it) (logcabin and other implementations immediately send
// the new entry to the followers to reduce latency, but it's not necessary for correctness,
// it will be sent to all followers when their respective timeouts trigger)
#[tracing::instrument(level = "trace", skip_all)]
fn handle_append_entries_client_request<SM, SMin, SMout>(
    common_state: &mut CommonState<SM, SMin, SMout>,
    client_channel_per_input: &mut VecDeque<(mpsc::Sender<AppendEntriesClientResponse<SMin, SMout>>, usize)>,
    request: AppendEntriesClientRequest<SMin, SMout>,
) where
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Send + Clone + 'static,
{
    tracing::debug!("Received a client message, replicating it");

    client_channel_per_input.push_back((request.reply_to, request.entries_to_replicate.len()));

    common_state
        .log
        .append(request.entries_to_replicate, common_state.current_term);
}

// #[tracing::instrument(level = "trace", skip_all)]
fn handle_append_entries_reply<SM, SMin, SMout>(
    common_state: &mut CommonState<SM, SMin, SMout>,
    peers_state: &mut BTreeMap<u32, PeerState>,
    client_channel_per_input: &mut VecDeque<(mpsc::Sender<AppendEntriesClientResponse<SMin, SMout>>, usize)>,
    committed_entries_smout_buf: &mut Vec<SMout>,
    reply: AppendEntriesReply,
) where
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Clone,
{
    // TLA: 394 (also TLA: 435, since it ignores requests with stale terms)
    if reply.term != common_state.current_term {
        return;
    }

    let Some(peer_state) = peers_state.get_mut(&reply.from) else {
        tracing::error!("Couldn't get the state of peer {}", reply.from);
        return;
    };
    let peer_next_idx = &mut peer_state.next_index;
    let peer_match_idx = &mut peer_state.match_index;

    if reply.success {
        // TLA: 386-397 uses a field called mmatchIndex in the match, which is not in the paper
        // and I can't figure out what logcabin does, this should be right tough
        *peer_match_idx = reply.last_log_index;
        *peer_next_idx = *peer_match_idx + 1;

        commit_log_entries_replicated_on_majority(
            common_state,
            peers_state,
            client_channel_per_input,
            committed_entries_smout_buf,
        );
    } else {
        // TLA: 400 optimization: instead of decreasing by 1, have the follower send the index it is at
        // so we don't have to guess
        *peer_next_idx = reply.last_log_index + 1;
    }
}

/// Commits the log entries that have been replicated on the majority of the servers.
fn commit_log_entries_replicated_on_majority<SM, SMin, SMout>(
    common_state: &mut CommonState<SM, SMin, SMout>,
    peers_state: &BTreeMap<u32, PeerState>,
    client_channel_per_input: &mut VecDeque<(mpsc::Sender<AppendEntriesClientResponse<SMin, SMout>>, usize)>,
    committed_entries_smout_buf: &mut Vec<SMout>,
) where
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Clone,
{
    // TLA: 259-276, the implementation is different from how the TLA describes it, but it should be equivalent
    let mut n = common_state.commit_index;
    // this could be optimized for large groups of entries being sent together
    // by getting the median match_index instead of checking all of them
    #[allow(clippy::int_plus_one)] // imo it's more clear this way: the next n (n+1), must be in the log's bounds
    while n + 1 <= common_state.log.len()
        && common_state.log.get_term(n + 1) == common_state.current_term
        && majority_of_servers_have_log_entry(peers_state, n as u64 + 1)
    {
        n += 1;
    }
    common_state.commit_index = n;

    let old_last_applied = common_state.last_applied;

    common_state.commit_log_entries_up_to_commit_index_buf(committed_entries_smout_buf);

    for _ in (old_last_applied + 1)..=common_state.commit_index {
        if let Some((sender, uses_left)) = client_channel_per_input.front_mut() {
            let response = AppendEntriesClientResponse(Ok(committed_entries_smout_buf.pop().unwrap()));
            let _ = sender.try_send(response);
            *uses_left -= 1;
            if *uses_left == 0 {
                client_channel_per_input.pop_front();
            }
        } else {
            tracing::error!("No client channel to send the response to");
        }
    }
    assert!(committed_entries_smout_buf.is_empty());
}

/// Returns whether the majority of servers, including self, have the log entry at the given index.
fn majority_of_servers_have_log_entry(peers_state: &BTreeMap<u32, PeerState>, index: u64) -> bool {
    let count_including_self = 1 + peers_state.values().filter(|state| state.match_index >= index).count();
    let n_servers = peers_state.len() + 1;
    count_including_self >= (n_servers + 1) / 2
}

#[cfg(test)]
mod tests {
    use crate::state_machine::VoidStateMachine;

    use super::*;

    #[test]
    fn test_handle_append_entries_reply() {
        let n_servers = 5;

        let mut common_state = CommonState::new(VoidStateMachine::new(), 1);
        let mut peers_state = BTreeMap::new();
        let mut client_channel_per_input = VecDeque::new();
        let mut committed_entries_smout_buf = Vec::new();

        common_state.log.append(vec![()], 0);

        // we are server 0, so we have peers [1,4]
        for i in 1..n_servers {
            peers_state.insert(i, PeerState::new(1));
        }

        let reply = AppendEntriesReply {
            from: 1,
            term: 0,
            success: true,
            last_log_index: 1,
        };

        handle_append_entries_reply(
            &mut common_state,
            &mut peers_state,
            &mut client_channel_per_input,
            &mut committed_entries_smout_buf,
            reply,
        );

        assert_eq!(peers_state.get(&1).unwrap().next_index, 2);
        assert_eq!(peers_state.get(&1).unwrap().match_index, 1);
        for _ in 2..n_servers {
            assert_eq!(peers_state.get(&2).unwrap().next_index, 1);
            assert_eq!(peers_state.get(&2).unwrap().match_index, 0);
        }
        assert_eq!(common_state.commit_index, 0);

        common_state.check_validity();
    }

    #[test]
    fn test_handle_append_entries_reply_commit_entry() {
        let n_servers = 5;

        let mut common_state = CommonState::new(VoidStateMachine::new(), 1);
        let mut peers_state = BTreeMap::new();
        let mut client_channel_per_input = VecDeque::new();
        let mut committed_entries_smout_buf = Vec::new();

        common_state.log.append(vec![()], 0);

        for i in 1..=n_servers {
            peers_state.insert(i, PeerState::new(1));
        }
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        client_channel_per_input.push_back((tx, 1));

        let mut reply = AppendEntriesReply {
            from: 1,
            term: 0,
            success: true,
            last_log_index: 1,
        };

        handle_append_entries_reply(
            &mut common_state,
            &mut peers_state,
            &mut client_channel_per_input,
            &mut committed_entries_smout_buf,
            reply.clone(),
        );
        reply.from = 2;
        handle_append_entries_reply(
            &mut common_state,
            &mut peers_state,
            &mut client_channel_per_input,
            &mut committed_entries_smout_buf,
            reply,
        );

        assert_eq!(common_state.commit_index, 1);

        let response = rx.try_recv().unwrap();
        assert!(response.0.is_ok_and(|inner| inner == ()));

        common_state.check_validity();
    }
}
