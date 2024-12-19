use std::cmp::min;
use std::collections::BTreeMap;

use crate::common_state::CommonState;
use crate::messages::request_vote::{RequestVoteReply, RequestVoteRequest};
use crate::messages::*;
use crate::state_machine::StateMachine;

use actum::actor_ref::ActorRef;
use append_entries::{AppendEntriesReply, AppendEntriesRequest};

/// Handles a vote request message, answering it with a positive or negative vote.
///
/// Returns `true` if the server should become a follower
#[tracing::instrument(level = "trace", skip_all)]
pub fn handle_vote_request<SM, SMin, SMout>(
    me: u32,
    common_state: &mut CommonState<SM, SMin, SMout>,
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin, SMout>>>,
    request: RequestVoteRequest,
) -> bool
where
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    let step_down = common_state.update_term(request.term);

    let vote_granted = common_state.can_grant_vote(&request);

    tracing::trace!("vote granted: {} for id: {}", vote_granted, request.candidate_id);

    if let Some(candidate_ref) = peers.get_mut(&request.candidate_id) {
        if vote_granted {
            common_state.voted_for = Some(request.candidate_id);
        }
        let reply = RequestVoteReply {
            from: me,
            term: common_state.current_term,
            vote_granted,
        };
        let _ = candidate_ref.try_send(reply.into());
    } else {
        tracing::error!("peer {} not found", request.candidate_id);
    }

    // TLA, L346
    assert_eq!(common_state.current_term, request.term);

    step_down
}


/// This enum is used ONLY for the functions calling handle_append_entries_request
/// so it can tell whether to panic or not if we recieve a message from a leader
/// with the same term as us, which is something that shouldn't happen anyway.
#[derive(Debug, PartialEq, Eq)]
pub enum RaftState {
    Follower,
    Candidate,
    Leader,
}

/// Handles an append entries request message, updating the log and sending a reply.
/// 
/// Returns `true` if the server should become a follower
#[tracing::instrument(level = "trace", skip_all)]
pub fn handle_append_entries_request<SM, SMin, SMout>(
    me: u32,
    common_state: &mut CommonState<SM, SMin, SMout>,
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin, SMout>>>,
    state: RaftState,
    request: AppendEntriesRequest<SMin>,
) -> bool where
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    let mut step_down = false;

    // set a negative reply by default, we will update it if we can append the entries
    let mut reply = AppendEntriesReply {
        from: me,
        term: common_state.current_term,
        success: false,
        last_log_index: common_state.log.len() as u64,
    };

    if common_state.update_term(request.term) {
        tracing::trace!("new term: {}, new leader: {}", request.term, request.leader_id);
        step_down = true;
    }

    if request.term < common_state.current_term {
        tracing::trace!(
            "request term = {} < current term = {}: ignoring",
            request.term,
            common_state.current_term
        );

        if let Some(sender_ref) = peers.get_mut(&request.leader_id) {
            let _ = sender_ref.try_send(reply.into());
        }
        return step_down;
    }

    if request.term == common_state.current_term {
        assert_ne!(state, RaftState::Leader, 
            "two leaders with the same term detected: {} and {} (me)",
            request.leader_id, me);
        if let Some(leader_id) = common_state.leader_id.as_ref() {
            assert_eq!(
                request.leader_id, *leader_id,
                "two leaders with the same term detected: {} and {}",
                request.leader_id, *leader_id
            );
        }
    }

    // update this here and not in update_term, as the update_term in handle_vote_request 
    // might have already updated the term, causing the update_term here to never return true
    // leaving us with the correct term, but no leader_id, we also can't set the leader_id in update_term
    // as we can't know whether the candidate will win the election
    common_state.leader_id = Some(request.leader_id);

    if request.prev_log_index > common_state.log.len() as u64 {
        tracing::trace!(
            "missing entries: previous log index = {}, log length: {}: ignoring",
            request.prev_log_index,
            common_state.log.len()
        );

        if let Some(sender_ref) = peers.get_mut(&request.leader_id) {
            let _ = sender_ref.try_send(reply.into());
        }
        return step_down;
    }

    common_state
        .log
        .insert(request.entries, request.prev_log_index, request.term);

    let leader_commit: usize = request.leader_commit.try_into().unwrap();

    if leader_commit > common_state.commit_index {
        tracing::trace!("leader commit is greater than follower commit, updating commit index");
        let new_commit_index = min(leader_commit, common_state.log.len());

        common_state.commit_index = new_commit_index;
        common_state.commit_log_entries_up_to_commit_index();
    }

    if let Some(leader_ref) = peers.get_mut(&request.leader_id) {
        reply.success = true;
        let _ = leader_ref.try_send(reply.into());
    }

    // TLA, L346
    assert_eq!(common_state.current_term, request.term);

    step_down
}