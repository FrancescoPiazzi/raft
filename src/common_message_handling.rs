use std::collections::BTreeMap;

use crate::common_state::CommonState;
use crate::messages::request_vote::{RequestVoteReply, RequestVoteRequest};
use crate::messages::*;
use crate::state_machine::StateMachine;

use actum::actor_ref::ActorRef;

/// Handles a vote request message, answering it with a positive or negative vote.
///
/// Returns `true` if the server should become a follower
#[tracing::instrument(level = "trace", skip_all)]
pub fn handle_vote_request<SM, SMin, SMout>(
    me: u32,
    common_state: &mut CommonState<SM, SMin, SMout>,
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin, SMout>>>,
    request: &RequestVoteRequest,
) -> bool
where
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    let step_down = common_state.update_term(request.term);

    let log_is_ok = common_state.log.is_log_ok(request);
    // use step_down as a proxy to know whether the message term is > than the current term,
    // since in any other case we don't vote for the candidate requesting the vote, no matter the state
    let vote_granted = step_down && log_is_ok && common_state.voted_for_allows_vote(request.candidate_id);

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
