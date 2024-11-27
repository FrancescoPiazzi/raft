use std::cmp::min;
use std::collections::BTreeMap;
use std::ops::Range;
use std::time::Duration;

use crate::common_state::CommonState;
use crate::messages::append_entries::{AppendEntriesReply, AppendEntriesRequest};
use crate::messages::append_entries_client::AppendEntriesClientRequest;
use crate::messages::request_vote::{RequestVoteReply, RequestVoteRequest};
use crate::messages::*;
use crate::state_machine::StateMachine;
use actum::actor_bounds::ActorBounds;
use actum::actor_ref::ActorRef;
use rand::{thread_rng, Rng};
use tokio::time::timeout;

/// Behavior of the Raft server in follower state.
///
/// Returns when no message from the leader is received after `election_timeout`.
pub async fn follower_behavior<AB, SM, SMin, SMout>(
    cell: &mut AB,
    me: u32,
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin>>>,
    common_state: &mut CommonState<SM, SMin, SMout>,
    election_timeout: Range<Duration>,
) where
    AB: ActorBounds<RaftMessage<SMin>>,
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Clone + Send + 'static,
    SMout: Send,
{
    let election_timeout = thread_rng().gen_range(election_timeout);
    let mut leader_id: Option<u32> = None;

    loop {
        let Ok(message) = timeout(election_timeout, cell.recv()).await else {
            tracing::debug!("election timeout");
            return;
        };

        let message = message.message().expect("raft runs indefinitely");
        tracing::trace!(message = ?message);

        match message {
            RaftMessage::AppendEntriesRequest(request) => {
                handle_append_entries_request(me, common_state, peers, &mut leader_id, request);
            }
            RaftMessage::RequestVoteRequest(request) => {
                handle_vote_request(me, common_state, peers, request);
            }
            RaftMessage::AppendEntriesClientRequest(request) => {
                handle_append_entries_client_request(peers, leader_id.as_mut(), request);
            }
            other => {
                tracing::trace!(unhandled = ?other);
            }
        }
    }
}

#[tracing::instrument(level = "trace", skip_all)]
fn handle_append_entries_request<SM, SMin, SMout>(
    me: u32,
    common_state: &mut CommonState<SM, SMin, SMout>,
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin>>>,
    leader_id: &mut Option<u32>,
    request: AppendEntriesRequest<SMin>,
) where
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Clone + Send + 'static,
    SMout: Send,
{
    if request.term < common_state.current_term {
        tracing::trace!("request term = {} < current term = {}", request.term, common_state.current_term);

        if let Some(sender_ref) = peers.get_mut(&request.leader_id) {
            let reply = AppendEntriesReply {
                from: me,
                term: common_state.current_term,
                success: false,
            };
            let _ = sender_ref.try_send(reply.into());
        }

        return;
    }

    if request.term > common_state.current_term {
        tracing::trace!("previous term = {}, new term = {}",
                        common_state.current_term, request.term);
        common_state.current_term = request.term;
        common_state.voted_for = None;

        if let Some(leader_id) = leader_id{
            tracing::trace!("previous leader = {}, new leader = {}",
                            leader_id, request.leader_id);
        }
        *leader_id = Some(request.leader_id);
    }

    if request.term == common_state.current_term {
        if let Some(leader_id) = leader_id.as_ref() {
            assert_eq!(request.leader_id, *leader_id);
        }
    }

    if request.prev_log_index > common_state.log.len() as u64 {
        tracing::trace!("previous log index = {}, log length: {}: ignoring",
                        request.prev_log_index, common_state.log.len());

        if let Some(sender_ref) = peers.get_mut(&request.leader_id) {
            let reply = AppendEntriesReply {
                from: me,
                term: common_state.current_term,
                success: false,
            };
            let _ = sender_ref.try_send(reply.into());
        }
        return;
    }

    if !request.is_heartbeat() {
        common_state
            .log
            .insert(request.entries, request.prev_log_index, request.term);
    }

    let leader_commit: usize = request.leader_commit.try_into().unwrap();

    if leader_commit > common_state.commit_index {
        tracing::trace!("leader commit is greater than follower commit, updating commit index");
        let new_commit_index = min(leader_commit, common_state.log.len());
        tracing::trace!("previous log index = {}, log length: {}: ignoring",
                        request.prev_log_index, common_state.log.len());    // TOASK: what is that ignoring? Did I put it there?

        common_state.commit_index = new_commit_index;
        common_state.commit_log_entries_up_to_commit_index(None);
    }

    if let Some(leader_id) = leader_id.as_mut() {
        // TOASK: this can happen? If the leader changes and we get a new term, we update it earlier, 
        // how can the leader change without the term progressing?
        if request.leader_id != *leader_id { /* leader changed */ }
    }

    if let Some(leader_ref) = peers.get_mut(&request.leader_id) {
        let reply = AppendEntriesReply {
            from: me,
            term: common_state.current_term,
            success: true,
        };
        let _ = leader_ref.try_send(reply.into());
    }
}

#[tracing::instrument(level = "trace", skip_all)]
fn handle_vote_request<SM, SMin, SMout>(
    me: u32,
    common_state: &mut CommonState<SM, SMin, SMout>,
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin>>>,
    request: RequestVoteRequest,
) where
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Clone + Send + 'static,
    SMout: Send,
{
    let vote_granted = request.term >= common_state.current_term
        && (common_state.voted_for.is_none() || *common_state.voted_for.as_ref().unwrap() == request.candidate_id);
    tracing::trace!(vote_granted);

    if let Some(candidate_ref) = peers.get_mut(&request.candidate_id) {
        let reply = RequestVoteReply { from: me, vote_granted };

        let _ = candidate_ref.try_send(reply.into());
    }
}

#[tracing::instrument(level = "trace", skip_all)]
fn handle_append_entries_client_request<SMin>(
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin>>>,
    leader_id: Option<&mut u32>,
    request: AppendEntriesClientRequest<SMin>,
) where
    SMin: Clone + Send + 'static,
{
    if let Some(leader_id) = leader_id {
        if let Some(leader_ref) = peers.get_mut(&leader_id) {
            tracing::debug!("redirecting the client to leader {}", leader_id);
            let reply = Err(Some(leader_ref.clone()));
            let _ = request.reply_to.send(reply);
        } else {
            tracing::debug!("no leader to redirect the client to");
            let _ = request.reply_to.send(Err(None));
        }
    } else {
        tracing::debug!("no leader to redirect the client to");
        let _ = request.reply_to.send(Err(None));
    }
}
