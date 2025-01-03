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

    // set a negative reply by default, we will update it if we can grant the vote
    let mut reply = RequestVoteReply {
        from: me,
        term: common_state.current_term,
        vote_granted: false,
    };

    if request.term < common_state.current_term {
        tracing::trace!(
            "request term = {} < current term = {}: ignoring",
            request.term,
            common_state.current_term
        );

        if let Some(sender_ref) = peers.get_mut(&request.candidate_id) {
            let _ = sender_ref.try_send(reply.into());
        }
        return step_down;
    }

    let vote_granted = common_state.can_grant_vote(&request);

    tracing::trace!("vote granted: {} for id: {}", vote_granted, request.candidate_id);

    if let Some(candidate_ref) = peers.get_mut(&request.candidate_id) {
        if vote_granted {
            common_state.voted_for = Some(request.candidate_id);
        }
        reply.vote_granted = vote_granted;
        let _ = candidate_ref.try_send(reply.into());
    } else {
        tracing::error!("peer {} not found", request.candidate_id);
    }

    step_down
}

/// This enum is used ONLY for MINOR differences in the behaviour of the server depending on its state,
/// such as whether to panic or not when we recieve an append entry with the same term as ours as the leader.
/// Which is not something that should hapen anyway
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
) -> bool
where
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    let mut step_down = false;

    if common_state.update_term(request.term) {
        tracing::trace!("new term: {}, new leader: {}", request.term, request.leader_id);
        step_down = true;
    }

    // set a negative reply by default, we will update it if we can append the entries
    let mut reply = AppendEntriesReply {
        from: me,
        term: common_state.current_term,
        success: false,
        last_log_index: common_state.log.len() as u64,
    };

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
        assert_ne!(
            state,
            RaftState::Leader,
            "two leaders with the same term detected: {} and {} (me)",
            request.leader_id,
            me
        );
        if let Some(leader_id) = common_state.leader_id.as_ref() {
            assert_eq!(
                request.leader_id, *leader_id,
                "two leaders with the same term detected: {} and {}",
                request.leader_id, *leader_id
            );
        }
    }

    // update this here and not in update_term, as the update_term in handle_vote_request()
    // might have already updated the term, causing the update_term here to never return true
    // leaving us with the correct term, but no leader_id. We also can't set the leader_id in update_term itself
    // as we can't know whether the candidate that sent the request will win the election
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


#[cfg(test)]
mod tests{
    use assert_matches::assert_matches;

    use super::*;
    use crate::state_machine::VoidStateMachine;

    #[test]
    fn grant_vote_and_step_down(){
        let mut common_state: CommonState<VoidStateMachine, (), ()> = CommonState::new(VoidStateMachine::new());
        let mut peers = BTreeMap::new();

        let mut candidate_channel = futures_channel::mpsc::channel(10);
        peers.insert(2, ActorRef::new(candidate_channel.0.clone()));

        let request = RequestVoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 1,
            last_log_term: 1,
        };

        let step_down = handle_vote_request(1, &mut common_state, &mut peers, request.clone());
        assert_eq!(step_down, true);

        let vote = candidate_channel.1.try_next().unwrap().unwrap();
        assert_matches!(vote, RaftMessage::RequestVoteReply(inner) if (
            inner.vote_granted == true && inner.term == 1 && inner.from == 1));

        common_state.check_validity();
    }

    #[test]
    fn reject_vote_term_too_low(){
        let mut common_state: CommonState<VoidStateMachine, (), ()> = CommonState::new(VoidStateMachine::new());
        common_state.current_term = 2;
        let mut peers = BTreeMap::new();

        let mut candidate_channel = futures_channel::mpsc::channel(10);
        peers.insert(2, ActorRef::new(candidate_channel.0.clone()));

        let request = RequestVoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 1,
            last_log_term: 1,
        };

        let step_down = handle_vote_request(1, &mut common_state, &mut peers, request.clone());
        assert_eq!(step_down, false);

        let vote = candidate_channel.1.try_next().unwrap().unwrap();
        assert_matches!(vote, RaftMessage::RequestVoteReply(inner) if (
            inner.vote_granted == false && inner.term == 2 && inner.from == 1));

        common_state.check_validity();
    }

    #[test]
    fn reject_vote_already_voted(){
        let mut common_state: CommonState<VoidStateMachine, (), ()> = CommonState::new(VoidStateMachine::new());
        common_state.current_term = 1; // needed or voted_for will be reset because of the term of the message
        common_state.voted_for = Some(1);
        let mut peers = BTreeMap::new();

        let mut candidate_channel = futures_channel::mpsc::channel(10);
        peers.insert(2, ActorRef::new(candidate_channel.0.clone()));

        let request = RequestVoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 1,
            last_log_term: 1,
        };

        let step_down = handle_vote_request(1, &mut common_state, &mut peers, request.clone());
        assert_eq!(step_down, false);

        let vote = candidate_channel.1.try_next().unwrap().unwrap();
        assert_matches!(vote, RaftMessage::RequestVoteReply(inner) if (
            inner.vote_granted == false && inner.term == 1 && inner.from == 1));

        common_state.check_validity();
    }

    #[test]
    /// The most normal case, where the request is valid, with no entries to commit
    fn test_handle_append_entries_request(){
        let mut common_state: CommonState<VoidStateMachine, (), ()> = CommonState::new(VoidStateMachine::new());
        common_state.current_term = 1;
        let mut peers = BTreeMap::new();

        let mut leader_channel = futures_channel::mpsc::channel(10);
        peers.insert(2, ActorRef::new(leader_channel.0.clone()));

        let request = AppendEntriesRequest {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![(), (), ()],
            leader_commit: 0,
        };

        let step_down = handle_append_entries_request(1, &mut common_state, &mut peers, RaftState::Follower, request);
        assert_eq!(step_down, false);

        let reply = leader_channel.1.try_next().unwrap().unwrap();
        assert_matches!(reply, RaftMessage::AppendEntriesReply(inner) if (
            inner.success == true && 
            inner.term == 1 && 
            inner.from == 1 && 
            inner.last_log_index == 0)
        );

        assert_eq!(common_state.log.len(), 3);
        assert_eq!(common_state.commit_index, 0);

        common_state.check_validity();
    }

    #[test]
    fn test_handle_append_entries_request_commit_entries(){
        let mut common_state: CommonState<VoidStateMachine, (), ()> = CommonState::new(VoidStateMachine::new());
        common_state.current_term = 1;
        let mut peers = BTreeMap::new();

        let mut leader_channel = futures_channel::mpsc::channel(10);
        peers.insert(2, ActorRef::new(leader_channel.0.clone()));

        let request = AppendEntriesRequest {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![(), (), ()],
            leader_commit: 2,
        };

        let step_down = handle_append_entries_request(1, &mut common_state, &mut peers, RaftState::Follower, request);
        assert_eq!(step_down, false);

        let reply = leader_channel.1.try_next().unwrap().unwrap();
        assert_matches!(reply, RaftMessage::AppendEntriesReply(inner) if (
            inner.success == true && 
            inner.term == 1 && 
            inner.from == 1 && 
            inner.last_log_index == 0)
        );

        assert_eq!(common_state.log.len(), 3);
        assert_eq!(common_state.commit_index, 2);

        common_state.check_validity();
    }

    #[test]
    fn reject_append_entries_request_term_too_low(){
        let mut common_state: CommonState<VoidStateMachine, (), ()> = CommonState::new(VoidStateMachine::new());
        common_state.current_term = 2;
        let mut peers = BTreeMap::new();

        let mut leader_channel = futures_channel::mpsc::channel(10);
        peers.insert(2, ActorRef::new(leader_channel.0.clone()));

        let request = AppendEntriesRequest {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![(), (), ()],
            leader_commit: 0,
        };

        let step_down = handle_append_entries_request(1, &mut common_state, &mut peers, RaftState::Follower, request);
        assert_eq!(step_down, false);

        let reply = leader_channel.1.try_next().unwrap().unwrap();
        assert_matches!(reply, RaftMessage::AppendEntriesReply(inner) if (
            inner.success == false && 
            inner.term == 2 && 
            inner.from == 1 && 
            inner.last_log_index == 0)
        );

        assert_eq!(common_state.log.len(), 0);
        assert_eq!(common_state.commit_index, 0);

        common_state.check_validity();
    }

    #[test]
    fn step_down_after_append_entries_request(){
        let mut common_state: CommonState<VoidStateMachine, (), ()> = CommonState::new(VoidStateMachine::new());
        common_state.current_term = 1;
        let mut peers = BTreeMap::new();

        let mut leader_channel = futures_channel::mpsc::channel(10);
        peers.insert(2, ActorRef::new(leader_channel.0.clone()));

        let request = AppendEntriesRequest {
            term: 2,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![(), (), ()],
            leader_commit: 0,
        };

        let step_down = handle_append_entries_request(1, &mut common_state, &mut peers, RaftState::Follower, request);
        assert_eq!(step_down, true);

        let reply = leader_channel.1.try_next().unwrap().unwrap();
        assert_matches!(reply, RaftMessage::AppendEntriesReply(inner) if (
            inner.success == true && 
            inner.term == 2 && 
            inner.from == 1 && 
            inner.last_log_index == 0)
        );

        assert_eq!(common_state.log.len(), 3);
        assert_eq!(common_state.commit_index, 0);

        common_state.check_validity();
    }

    #[test]
    #[should_panic]
    fn panic_two_leaders_with_same_term_detected(){
        let mut common_state: CommonState<VoidStateMachine, (), ()> = CommonState::new(VoidStateMachine::new());
        common_state.current_term = 1;
        let mut peers = BTreeMap::new();

        let request = AppendEntriesRequest {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![(), (), ()],
            leader_commit: 0,
        };

        handle_append_entries_request(1, &mut common_state, &mut peers, RaftState::Leader, request);

        common_state.check_validity();
    }

    #[test]
    fn reject_append_entries_hole_in_log(){
        let already_present_entries = vec![(), (), ()];
        let mut common_state: CommonState<VoidStateMachine, (), ()> = CommonState::new(VoidStateMachine::new());
        common_state.current_term = 1;
        common_state.log.insert(already_present_entries.clone(), 0, 1);
        let mut peers = BTreeMap::new();

        let mut leader_channel = futures_channel::mpsc::channel(10);
        peers.insert(2, ActorRef::new(leader_channel.0.clone()));

        let request = AppendEntriesRequest {
            term: 1,
            leader_id: 2,
            prev_log_index: 5,
            prev_log_term: 1,
            entries: vec![(), (), ()],
            leader_commit: 0,
        };

        let step_down = handle_append_entries_request(1, &mut common_state, &mut peers, RaftState::Follower, request);
        assert_eq!(step_down, false);

        let reply = leader_channel.1.try_next().unwrap().unwrap();
        assert_matches!(reply, RaftMessage::AppendEntriesReply(inner) if (
            inner.success == false && 
            inner.term == 1 && 
            inner.from == 1 && 
            inner.last_log_index == already_present_entries.len() as u64)
        );

        assert_eq!(common_state.log.len(), already_present_entries.len());
        assert_eq!(common_state.commit_index, 0);

        common_state.check_validity();
    }
}
