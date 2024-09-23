use std::cmp::{max, min};
use std::ops::Range;
use std::time::Duration;

use rand::{thread_rng, Rng};
use tokio::time::timeout;

use crate::raft::common::commit;
use crate::raft::common_state::CommonState;
use crate::raft::messages::*;
use actum::prelude::*;

// follower nodes receive AppendEntry messages from the leader and duplicate them
// returns when no message is received from the leader after some time
pub async fn follower<AB, LogEntry>(
    me: &ActorRef<RaftMessage<LogEntry>>,
    cell: &mut AB,
    common_data: &mut CommonState<LogEntry>,
    election_timeout: Range<Duration>,
) where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + 'static,
{
    let election_timeout = thread_rng().gen_range(election_timeout);

    let mut leader_ref: Option<ActorRef<RaftMessage<LogEntry>>> = None;

    loop {
        let Ok(message) = timeout(election_timeout, cell.recv()).await else {
            tracing::info!("election timeout");
            return;
        };

        let message = message.message().expect("raft runs indefinitely");

        match message {
            RaftMessage::AppendEntries(mut append_entries_rpc) => {
                if append_entries_rpc.term < common_data.current_term {
                    tracing::trace!("🚫 Received an AppendEntries message with an outdated term, ignoring");
                    let msg = RaftMessage::AppendEntryResponse(me.clone(), common_data.current_term, false);
                    let _ = append_entries_rpc.leader_ref.try_send(msg);
                    continue;
                }
                if append_entries_rpc.prev_log_index > common_data.log.len() as u64 {
                    tracing::trace!("🚫 Received an AppendEntries message with an invalid prev_log_index, ignoring");
                    let msg = RaftMessage::AppendEntryResponse(me.clone(), common_data.current_term, false);
                    let _ = append_entries_rpc.leader_ref.try_send(msg);
                    continue;
                }

                if append_entries_rpc.entries.is_empty() {
                    tracing::trace!("❤️ Received heartbeat");
                } else {
                    tracing::info!(
                        "✏️ Received an AppendEntries message with {} entries, adding them to the log",
                        append_entries_rpc.entries.len()
                    );
                }

                let mut entries = append_entries_rpc
                    .entries
                    .into_iter()
                    .map(|entry| (entry, append_entries_rpc.term))
                    .collect();
                common_data.log.append(&mut entries);

                if append_entries_rpc.leader_commit > common_data.commit_index as u64 {
                    common_data.commit_index = min(
                        append_entries_rpc.leader_commit,
                        max(common_data.log.len() as i64 - 1, 0) as u64,
                    ) as usize;
                }

                commit(common_data);

                leader_ref = Some(append_entries_rpc.leader_ref.clone());

                let msg = RaftMessage::AppendEntryResponse(me.clone(), common_data.current_term, true);
                let _ = append_entries_rpc.leader_ref.try_send(msg);
            }
            RaftMessage::RequestVote(mut request_vote_rpc) => {
                // grant the vote if the candidate's term is greater than or equal to the current term,
                // and either we haven't voted for anyone yet or we have voted for that candidate
                tracing::trace!("🗳️ Received a RequestVote message");
                let grant_vote = request_vote_rpc.term >= common_data.current_term
                    && (common_data.voted_for.is_none()
                        || *common_data.voted_for.as_ref().unwrap() == request_vote_rpc.candidate_ref.clone());
                let msg = RaftMessage::RequestVoteResponse(grant_vote);
                let _ = request_vote_rpc.candidate_ref.try_send(msg);
            }
            RaftMessage::AppendEntriesClient(mut append_entries_client_rpc) => {
                let msg = RaftMessage::AppendEntriesClientResponse(Err(leader_ref.clone()));
                let _ = append_entries_client_rpc.client_ref.try_send(msg);
            }
            _ => {
                tracing::trace!(unhandled = ?message);
            }
        }
    }
}
