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
use crate::types::AppendEntriesClientResponse;

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
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin, SMout>>>,
    common_state: &mut CommonState<SM, SMin, SMout>,
    election_timeout: Range<Duration>,
) where
    AB: ActorBounds<RaftMessage<SMin, SMout>>,
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    let election_timeout = thread_rng().gen_range(election_timeout);
    let mut leader_id: Option<u32> = None;

    loop {
        // TODO: do not reset the election timeout on every message, i.e. Client requests don't count
        let Ok(message) = timeout(election_timeout, cell.recv()).await else {
            tracing::debug!("election timeout");
            return;
        };
        let message = message.message().expect("raft runs indefinitely");

        handle_message_as_follower::<AB, SM, SMin, SMout>(me, peers, common_state, &mut leader_id, message);
    }
}

fn handle_message_as_follower<AB, SM, SMin, SMout>(
    me: u32,
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin, SMout>>>,
    common_state: &mut CommonState<SM, SMin, SMout>,
    leader_id: &mut Option<u32>,
    message: RaftMessage<SMin, SMout>,
) where
    AB: ActorBounds<RaftMessage<SMin, SMout>>,
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    tracing::trace!(message = ?message);

    common_state.update_term(&message);

    match message {
        RaftMessage::AppendEntriesRequest(request) => {
            handle_append_entries_request(me, common_state, peers, leader_id, request);
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

#[tracing::instrument(level = "trace", skip_all)]
fn handle_append_entries_request<SM, SMin, SMout>(
    me: u32,
    common_state: &mut CommonState<SM, SMin, SMout>,
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin, SMout>>>,
    leader_id: &mut Option<u32>,
    request: AppendEntriesRequest<SMin>,
) where
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    if request.term < common_state.current_term {
        tracing::trace!(
            "request term = {} < current term = {}: ignoring",
            request.term,
            common_state.current_term
        );

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

    *leader_id = Some(request.leader_id);

    if request.term == common_state.current_term {
        if let Some(leader_id) = leader_id.as_ref() {
            assert_eq!(
                request.leader_id, *leader_id,
                "two leaders with the same term detected: {} and {}",
                request.leader_id, *leader_id
            );
        }
    }

    if request.prev_log_index > common_state.log.len() as u64 {
        tracing::trace!(
            "missing entries: previous log index = {}, log length: {}: ignoring",
            request.prev_log_index,
            common_state.log.len()
        );

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

    common_state
        .log
        .insert(request.entries, request.prev_log_index, request.term);

    let leader_commit: usize = request.leader_commit.try_into().unwrap();

    if leader_commit > common_state.commit_index {
        tracing::trace!("leader commit is greater than follower commit, updating commit index");
        let new_commit_index = min(leader_commit, common_state.log.len());
        tracing::trace!(
            "previous log index = {}, log length: {}: ignoring",
            request.prev_log_index,
            common_state.log.len()
        ); // TOASK: what is that ignoring? Did I put it there?

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
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin, SMout>>>,
    request: RequestVoteRequest,
) where
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    let log_is_ok = common_state.log.is_log_ok(&request);
    let vote_granted = log_is_ok && common_state.voted_for.is_none();

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
}

#[tracing::instrument(level = "trace", skip_all)]
fn handle_append_entries_client_request<SMin, SMout>(
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin, SMout>>>,
    leader_id: Option<&mut u32>,
    request: AppendEntriesClientRequest<SMin, SMout>,
) where
    SMin: Clone + Send + 'static,
{
    if let Some(leader_id) = leader_id {
        if let Some(leader_ref) = peers.get_mut(leader_id) {
            tracing::debug!("redirecting the client to leader {}", leader_id);
            let reply = Err(Some(leader_ref.clone()));
            let _ = request.reply_to.try_send(AppendEntriesClientResponse(reply));
        } else {
            tracing::debug!("no leader to redirect the client to");
            let _ = request.reply_to.try_send(AppendEntriesClientResponse(Err(None)));
        }
    } else {
        tracing::debug!("no leader to redirect the client to");
        let _ = request.reply_to.try_send(AppendEntriesClientResponse(Err(None)));
    }
}
