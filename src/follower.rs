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
) -> Result<(), ()>
where
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

        let Ok(message) = timeout(election_timeout, cell.recv()).await else {
            tracing::debug!("election timeout");
            return Ok(());
        };
        let Some(message) = message.message() else {
            return Err(());
        };

        tracing::trace!(message = ?message);

        #[allow(clippy::needless_late_init)] // don't want to wrap the whole match
        let reset_election_timeout;
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
        } else if let Some(new_remaining_time_to_wait) = election_timeout.checked_sub(start_time.elapsed()) {
            election_timeout = new_remaining_time_to_wait;
        } else {
            tracing::trace!("election timeout");
            return Ok(());
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures_channel::mpsc as futures_mpsc;
    use tokio::sync::mpsc as tokio_mpsc;

    #[tokio::test]
    async fn follower_should_redirect_client_to_leader() {
        let leader_chan = futures_mpsc::channel::<RaftMessage<u32, u32>>(10);
        let leader_ref = ActorRef::new(leader_chan.0);
        let mut peers = BTreeMap::from([(1, leader_ref.clone())]);

        let mut client_chan = tokio_mpsc::channel::<AppendEntriesClientResponse<u32, u32>>(10);

        let leader_id = Some(&1);
        let request = AppendEntriesClientRequest {
            reply_to: client_chan.0,
            entries_to_replicate: vec![],
        };

        handle_append_entries_client_request(&mut peers, leader_id, request.clone());

        let response = client_chan.1.recv().await.unwrap();
        let error = response.0.unwrap_err();
        let received_leader_ref = error.unwrap();
        assert!(received_leader_ref == leader_ref);
    }

    #[tokio::test]
    async fn follower_should_return_error_when_leader_is_not_known() {
        let mut peers = BTreeMap::new();

        let mut client_chan = tokio_mpsc::channel::<AppendEntriesClientResponse<u32, u32>>(10);

        let leader_id = Some(&1);
        let request = AppendEntriesClientRequest {
            reply_to: client_chan.0,
            entries_to_replicate: vec![],
        };

        handle_append_entries_client_request(&mut peers, leader_id, request.clone());

        let response = client_chan.1.recv().await.unwrap();
        let error = response.0.unwrap_err();
        assert!(error.is_none());
    }
}
