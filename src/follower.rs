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

/// follower nodes receive AppendEntry messages from the leader and duplicate them
/// returns when no message is received from the leader after some time
pub async fn follower<AB, LogEntry>(
    cell: &mut AB,
    me: u32,
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<LogEntry>>>,
    common_state: &mut CommonState<LogEntry>,
    election_timeout: Range<Duration>,
) where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Clone + Send + 'static,
{
    let election_timeout = thread_rng().gen_range(election_timeout);
    let mut leader_ref: Option<ActorRef<RaftMessage<LogEntry>>> = None;

    loop {
        let Ok(message) = timeout(election_timeout, cell.recv()).await else {
            tracing::info!("election timeout");
            return;
        };

        let message = message.message().expect("raft runs indefinitely");
        tracing::trace!(message = ?message);

        match message {
            RaftMessage::AppendEntriesRequest(request) => {
                if request.term < common_state.current_term {
                    tracing::debug!("🚫 Received an AppendEntries message with an outdated term, ignoring");
                    let msg = AppendEntriesReply {
                        //TOASK: this is duplicated in the next if, should I make it into a function?
                        from: me,
                        term: common_state.current_term,
                        success: false,
                    };
                    let sender_ref = peers.get_mut(&request.leader_id).expect("all peers are known");
                    let _ = sender_ref.try_send(msg.into());
                    continue;
                }
                // +1 because if we have a log of length n, (indexes 1 to n), n+1 is valid
                if request.prev_log_index > common_state.log.len() as u64 + 1 { 
                    tracing::debug!(
                        "🚫 Received an AppendEntries message with an invalid prev_log_index (received: {}, log length: {}), ignoring",
                        request.prev_log_index,
                        common_state.log.len()
                    );
                    let msg = AppendEntriesReply {
                        from: me,
                        term: common_state.current_term,
                        success: false,
                    };
                    let sender_ref = peers.get_mut(&request.leader_id).expect("all peers are known");
                    let _ = sender_ref.try_send(msg.into());
                    continue;
                }

                if request.prev_log_index != common_state.log.len() as u64 + 1{   // Remove when log merge is implemented
                    tracing::warn!("Merge is not implemented yet");
                }

                let leader_commit:usize  = request.leader_commit.try_into().unwrap();
                if !request.entries.is_empty() {
                    tracing::debug!("AppendEntries has entries to append");
                    common_state.log.append(request.entries, request.term);
                }
                if leader_commit > common_state.commit_index {
                    tracing::debug!("leader commit is greater than follower commit, updating commit index");
                    let new_commit_index = min(leader_commit, common_state.log.len());
                    common_state.commit_index = new_commit_index;
                    common_state.commit(&mut None);
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
                tracing::trace!("Received a client message, redirecting the client to the leader");
                let _ = append_entries_client_request.reply_to.send(Err(leader_ref.clone()));
            }
            other => {
                tracing::trace!(unhandled = ?other);
            }
        }
    }
}
