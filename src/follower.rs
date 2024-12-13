use std::collections::BTreeMap;
use std::ops::Range;
use std::time::Duration;

use crate::common_message_handling::{handle_append_entries_request, handle_vote_request, RaftState};
use crate::common_state::CommonState;
use crate::messages::append_entries_client::AppendEntriesClientRequest;
use crate::messages::*;
use crate::state_machine::StateMachine;
use crate::types::AppendEntriesClientResponse;

use actum::actor_bounds::ActorBounds;
use actum::actor_ref::ActorRef;
use rand::{thread_rng, Rng};
use tokio::time::{timeout, Instant};

/// Behavior of the Raft server in follower state.
///
/// Returns when no message from the leader is received after `election_timeout`.
pub async fn follower_behavior<AB, SM, SMin, SMout>(
    cell: &mut AB,
    me: u32,
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin, SMout>>>,
    common_state: &mut CommonState<SM, SMin, SMout>,
    election_timeout_range: Range<Duration>,
    message_stash: &mut Vec<RaftMessage<SMin, SMout>>,
) where
    AB: ActorBounds<RaftMessage<SMin, SMout>>,
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    let mut election_timeout = thread_rng().gen_range(election_timeout_range.clone());

    for message in message_stash.drain(..) {
        match message {
            RaftMessage::AppendEntriesRequest(request) => {
                handle_append_entries_request(me, common_state, peers, RaftState::Follower, request);
            }
            RaftMessage::RequestVoteRequest(request) => {
                handle_vote_request(me, common_state, peers, request);
            }
            RaftMessage::AppendEntriesClientRequest(request) => {
                handle_append_entries_client_request(peers, common_state.leader_id.as_ref(), request);
            }
            other => {
                tracing::trace!(unhandled = ?other);
            }
        }
    }

    loop {
        let start_time = Instant::now();
        let reset_election_timeout;

        let Ok(message) = timeout(election_timeout, cell.recv()).await else {
            tracing::debug!("election timeout");
            return;
        };
        let message = message.message().expect("raft runs indefinitely");

        tracing::trace!(message = ?message);

        match message {
            RaftMessage::AppendEntriesRequest(request) => {
                handle_append_entries_request(me, common_state, peers, RaftState::Follower, request);
                reset_election_timeout = true;
            }
            RaftMessage::RequestVoteRequest(request) => {
                handle_vote_request(me, common_state, peers, request);
                reset_election_timeout = true;
            }
            RaftMessage::AppendEntriesClientRequest(request) => {
                handle_append_entries_client_request(peers, common_state.leader_id.as_ref(), request);
                reset_election_timeout = false;
            }
            other => {
                tracing::trace!(unhandled = ?other);
                reset_election_timeout = false;
            }
        }

        if reset_election_timeout {
            // we could also reset it to its original value, I just found it easier to reroll
            election_timeout = thread_rng().gen_range(election_timeout_range.clone());
        } else {
            if let Some(new_remaining_time_to_wait) = election_timeout.checked_sub(start_time.elapsed()) {
                election_timeout = new_remaining_time_to_wait;
            } else {
                tracing::trace!("election timeout");
                return;
            }
        }
    }
}

#[tracing::instrument(level = "trace", skip_all)]
fn handle_append_entries_client_request<SMin, SMout>(
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin, SMout>>>,
    leader_id: Option<&u32>,
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
