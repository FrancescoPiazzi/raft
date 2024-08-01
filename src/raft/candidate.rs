use rand::random;
use std::time::{Duration, Instant};
use tokio::time::timeout;

use actum::prelude::*;

use crate::raft::model::*;

pub async fn candidate<AB, LogEntry>(
    cell: &mut AB,
    me: &ActorRef<RaftMessage<LogEntry>>,
    common_data: &mut CommonData<LogEntry>,
    peer_refs: &mut [ActorRef<RaftMessage<LogEntry>>],
) -> RaftState
where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + 'static,
{
    tracing::info!("🤵 State is candidate");
    let min_timeout_ms = 150;
    let max_timeout_ms = 300;

    let mut votes = 1;
    let new_term = common_data.current_term + 1;

    for peer in peer_refs.iter_mut() {
        let _ = peer.try_send(RaftMessage::RequestVote(RequestVoteRPC {
            term: new_term,
            candidate_ref: me.clone(),
            last_log_index: common_data.last_applied,
            last_log_term: 0,
        }));
    }

    let mut time_left = Duration::from_millis(random::<u64>() % (max_timeout_ms - min_timeout_ms) + min_timeout_ms);

    loop {
        let start_wait_time = Instant::now();
        let wait_res = timeout(time_left, cell.recv()).await;

        let received_message = if let Ok(message) = wait_res {
            message
        } else {
            tracing::info!("⏰ Timeout reached, starting an election");
            return RaftState::Candidate;
        };

        let raftmessage = received_message.message().expect("Received a None message, quitting");

        match raftmessage {
            RaftMessage::RequestVoteResponse(vote_granted) => {
                if vote_granted {
                    tracing::trace!("Got a vote");
                    votes += 1;
                    if votes > peer_refs.len() / 2 + 1 {
                        return RaftState::Leader;
                    }
                }
            }
            RaftMessage::AppendEntries(append_entry_rpc) => {
                if append_entry_rpc.term >= common_data.current_term {
                    tracing::info!("There is another leader with an higher or equal term, going back to follower");
                    return RaftState::Follower;
                }
            }
            _ => {}
        }

        match time_left.checked_sub(start_wait_time.elapsed()) {
            Some(time) => time_left = time,
            None => return RaftState::Candidate,
        }
    }
}
