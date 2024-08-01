use rand::random;
use std::time::Duration;
use tokio::time::timeout;

use actum::prelude::*;

use crate::raft::model::*;

// follower nodes receive AppendEntry messages from the leader and execute them
// they ping the leader to see if it's still alive, if it isn't, they start an election
pub(crate) async fn follower<AB, LogEntry>(cell: &mut AB, common_data: &mut CommonData<LogEntry>) -> RaftState
where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + 'static,
{
    tracing::info!("👂 State is follower");

    let min_election_timeout_ms = 1500;
    let max_election_timeout_ms = 3000;

    let election_timeout = Duration::from_millis(
        random::<u64>() % (max_election_timeout_ms - min_election_timeout_ms) + min_election_timeout_ms,
    );

    let mut leader_ref: Option<ActorRef<RaftMessage<LogEntry>>> = None;

    loop {
        let wait_res = timeout(election_timeout, cell.recv()).await;

        let received_message = if let Ok(message) = wait_res {
            message
        } else {
            tracing::info!("⏰ Timeout reached, starting an election");
            return RaftState::Candidate;
        };

        let raftmessage = received_message.message().expect("Received a None message, quitting");

        match raftmessage {
            RaftMessage::AppendEntries(mut append_entries_rpc) => {
                if append_entries_rpc.entries.is_empty() {
                    tracing::trace!("❤️ Received heartbeat");
                } else {
                    tracing::info!("✏️ Received an AppendEntries message, adding them to the log");
                }
                common_data.log.append(&mut append_entries_rpc.entries);
                leader_ref = Some(append_entries_rpc.leader_ref.clone());

                // send append entry response
                let _ = append_entries_rpc
                    .leader_ref
                    .try_send(RaftMessage::AppendEntryResponse(common_data.current_term, true));
            }
            RaftMessage::RequestVote(mut request_vote_rpc) => {
                // check if the candidate log is at least as up-to-date as our log
                // if it is and we haven't voted for anyone yet, vote for the candidate
                if request_vote_rpc.term >= common_data.current_term
                    && (common_data.voted_for.is_none()
                        || *common_data.voted_for.as_ref().unwrap() == request_vote_rpc.candidate_ref)
                {
                    let _ = request_vote_rpc
                        .candidate_ref
                        .try_send(RaftMessage::RequestVoteResponse(true));
                    common_data.voted_for = Some(request_vote_rpc.candidate_ref);
                } else {
                    let _ = request_vote_rpc
                        .candidate_ref
                        .try_send(RaftMessage::RequestVoteResponse(false));
                }
            }
            RaftMessage::AppendEntriesClient(mut append_entries_client_rpc) => {
                let _ = append_entries_client_rpc
                    .client_ref
                    .try_send(RaftMessage::AppendEntriesClientResponse(Err(leader_ref.clone())));
            }
            _ => {}
        }
    }
}
