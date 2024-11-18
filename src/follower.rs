use std::cmp::min;
use std::collections::BTreeMap;
use std::ops::Range;
use std::time::Duration;

use actum::actor_bounds::ActorBounds;
use actum::actor_ref::ActorRef;
use rand::{thread_rng, Rng};
use tokio::time::timeout;

use crate::common_state::CommonState;
use crate::messages::append_entries::AppendEntriesReply;
use crate::messages::request_vote::RequestVoteReply;
use crate::messages::*;
use crate::state_machine::StateMachine;

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
    let mut leader_ref: Option<ActorRef<RaftMessage<SMin>>> = None;

    loop {
        let Ok(message) = timeout(election_timeout, cell.recv()).await else {
            tracing::trace!("election timeout");
            return;
        };

        let message = message.message().expect("raft runs indefinitely");
        tracing::trace!(message = ?message);

        match message {
            RaftMessage::AppendEntriesRequest(request) => {
                if request.term < common_state.current_term {
                    tracing::trace!("term is outdated: ignoring");
                    let reply = AppendEntriesReply::new(me, common_state.current_term, false);
                    let sender_ref = peers.get_mut(&request.leader_id).expect("all peers are known");
                    let _ = sender_ref.try_send(reply.into());
                    continue;
                }
                if request.prev_log_index > common_state.log.len() as u64 {
                    tracing::trace!(
                        "prev_log_index is invalid (received: {}, log length: {}): ignoring",
                        request.prev_log_index,
                        common_state.log.len()
                    );
                    let reply = AppendEntriesReply::new(me, common_state.current_term, false);
                    let sender_ref = peers.get_mut(&request.leader_id).expect("all peers are known");
                    let _ = sender_ref.try_send(reply.into());
                    continue;
                }

                common_state.current_term = request.term;

                if !request.entries.is_empty() {
                    common_state
                        .log
                        .insert(request.entries, request.prev_log_index, request.term);
                }
                let leader_commit: usize = request.leader_commit.try_into().unwrap();
                if leader_commit > common_state.commit_index {
                    tracing::trace!("leader commit is greater than follower commit, updating commit index");
                    let new_commit_index = min(leader_commit, common_state.log.len());
                    common_state.commit_index = new_commit_index;
                    common_state.commit_log_entries_up_to_commit_index(None);
                }

                leader_ref = Some(peers.get_mut(&request.leader_id).expect("all peers are known").clone());

                let reply = AppendEntriesReply {
                    from: me,
                    term: common_state.current_term,
                    success: true,
                };
                let _ = leader_ref.as_mut().unwrap().try_send(reply.into());
            }
            RaftMessage::RequestVoteRequest(request_vote_request) => {
                let vote_granted = request_vote_request.term >= common_state.current_term
                    && (common_state.voted_for.is_none()
                        || *common_state.voted_for.as_ref().unwrap() == request_vote_request.candidate_id);
                let reply = RequestVoteReply { from: me, vote_granted };

                let candidate_ref = peers
                    .get_mut(&request_vote_request.candidate_id)
                    .expect("all peers are known");
                let _ = candidate_ref.try_send(reply.into());
            }
            RaftMessage::AppendEntriesClientRequest(append_entries_client_request) => {
                tracing::trace!("redirecting the client to the leader");
                let _ = append_entries_client_request.reply_to.send(Err(leader_ref.clone()));
            }
            other => {
                tracing::trace!(unhandled = ?other);
            }
        }
    }
}
