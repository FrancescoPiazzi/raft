use actum::actor_bounds::ActorBounds;
use actum::actor_ref::ActorRef;
use rand::{thread_rng, Rng};
use std::cmp::{max, min};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::time::Duration;
use tokio::time::timeout;

use crate::common_state::CommonState;
use crate::messages::append_entries::AppendEntriesReply;
use crate::messages::request_vote::RequestVoteReply;
use crate::messages::*;

// follower nodes receive AppendEntry messages from the leader and duplicate them
// returns when no message is received from the leader after some time
pub async fn follower<AB, LogEntry>(
    cell: &mut AB,
    me: (u32, &mut ActorRef<RaftMessage<LogEntry>>),
    common_state: &mut CommonState<LogEntry>,
    election_timeout: Range<Duration>,
) where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Clone + Send + 'static,
{
    let election_timeout = thread_rng().gen_range(election_timeout);

    loop {
        let Ok(message) = timeout(election_timeout, cell.recv()).await else {
            tracing::info!("election timeout");
            return;
        };

        let message = message.message().expect("raft runs indefinitely");
        tracing::trace!(message = ?message);

        match message {
            RaftMessage::AppendEntriesRequest(mut request) => {
                if request.term < common_state.current_term {
                    todo!()
                }
                //
                else if request.prev_log_index > common_state.log.len() as u64 {
                    todo!()
                }
                //
                else if request.entries.is_empty() {
                    todo!()
                }
                //
                else {
                    let mut entries = request.entries.into_iter().map(|entry| (entry, request.term)).collect();
                    common_state.log.append(&mut entries);

                    if request.leader_commit > common_state.commit_index as u64 {
                        common_state.commit_index =
                            min(request.leader_commit, max(common_state.log.len() as i64 - 1, 0) as u64) as usize;
                    }

                    common_state.commit();

                    // todo: update leader

                    let reply = AppendEntriesReply {
                        from: me.0,
                        term: common_state.current_term,
                        success: true,
                    };
                }
            }
            RaftMessage::RequestVoteRequest(mut request_vote_request) => {
                let vote_granted = request_vote_request.term >= common_state.current_term
                    && (common_state.voted_for.is_none()
                        || *common_state.voted_for.as_ref().unwrap() == request_vote_request.candidate_id);
                let reply = RequestVoteReply {
                    from: 0,
                    vote_granted: false,
                };
                todo!()
            }
            RaftMessage::AppendEntriesClientRequest(mut append_entries_client_request) => {
                todo!()
            }
            other => {
                tracing::trace!(unhandled = ?other);
            }
        }
    }
}
