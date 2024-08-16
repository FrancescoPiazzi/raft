use rand::{thread_rng, Rng};
use std::{ops::Range, time::Duration};
use tokio::time::timeout;

use actum::prelude::*;

use crate::raft::model::*;

// follower nodes receive AppendEntry messages from the leader and duplicate them
// returns when no message is received from the leader after some time
pub async fn follower<AB, LogEntry>(cell: &mut AB, common_data: &mut CommonData<LogEntry>)
where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + 'static,
{
    tracing::info!("👂 State is follower");

    pub const DEFAULT_ELECTION_TIMEOUT: Range<Duration> = Duration::from_millis(1500)..Duration::from_millis(3000);
    let election_timeout = thread_rng().gen_range(DEFAULT_ELECTION_TIMEOUT);

    let mut leader_ref: Option<ActorRef<RaftMessage<LogEntry>>> = None;

    loop {
        let wait_res = timeout(election_timeout, cell.recv()).await;

        let Ok(message) = wait_res else {
            tracing::info!("⏰ Timeout reached");
            return;
        };

        let message = message.message().expect("Received a None message, quitting");

        match message {
            RaftMessage::AppendEntries(mut append_entries_rpc) => {
                if append_entries_rpc.entries.is_empty() {
                    tracing::trace!("❤️ Received heartbeat");
                } else {
                    tracing::info!("✏️ Received an AppendEntries message, adding them to the log");
                }
                common_data.log.append(&mut append_entries_rpc.entries);
                leader_ref = Some(append_entries_rpc.leader_ref.clone());

                let msg = RaftMessage::AppendEntryResponse(common_data.current_term, true);
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
                tracing::warn!("Received an unexpected message: {:?}", message);
            }
        }
    }
}
