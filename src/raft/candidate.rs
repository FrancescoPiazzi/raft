use rand::{thread_rng, Rng};
use std::{
    ops::Range,
    time::{Duration, Instant},
};
use tokio::time::timeout;

use actum::prelude::*;

use crate::raft::model::*;

// candidate nodes start an election by sending RequestVote messages to the other nodes
// if they receive a majority of votes, they become the leader
// if they receive a message from a node with a higher term, they become a follower
// returns true if the election was won, false if it was lost
pub async fn candidate<AB, LogEntry>(
    cell: &mut AB,
    me: &ActorRef<RaftMessage<LogEntry>>,
    common_data: &mut CommonData<LogEntry>,
    peer_refs: &mut [ActorRef<RaftMessage<LogEntry>>],
) -> bool
where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + Clone + 'static,
{
    pub const DEFAULT_ELECTION_TIMEOUT: Range<Duration> = Duration::from_millis(150)..Duration::from_millis(300);

    let election_won;
    let mut votes = 1;
    let new_term = common_data.current_term + 1;

    let request_vote_msg = RaftMessage::RequestVote(RequestVoteRPC {
        term: new_term,
        candidate_ref: me.clone(),
        last_log_index: common_data.last_applied,
        last_log_term: 0,
    });
    for peer in peer_refs.iter_mut() {
        let _ = peer.try_send(request_vote_msg.clone());
    }

    let mut remaining_time_to_wait = thread_rng().gen_range(DEFAULT_ELECTION_TIMEOUT);

    'candidate: loop {
        tracing::info!("📦 Starting an election");

        'election: loop {
            let Ok(message) = timeout(remaining_time_to_wait, cell.recv()).await else {
                tracing::info!("election timeout");
                break 'election;
            };

            let message = message.message().expect("raft runs indefinitely");

            match message {
                RaftMessage::RequestVoteResponse(vote_granted) => {
                    if vote_granted {
                        tracing::trace!("Got a vote");
                        votes += 1;
                        if votes > peer_refs.len() / 2 + 1 {
                            tracing::info!("🟩 Election won");
                            election_won = true;
                            break 'candidate;
                        }
                    }
                }
                RaftMessage::AppendEntries(append_entry_rpc) => {
                    if append_entry_rpc.term >= common_data.current_term {
                        tracing::info!("🟥 There is another leader with an higher or equal term, election lost");
                        election_won = false;
                        break 'candidate;
                    }
                }
                _ => {
                    tracing::warn!("Received an unexpected message: {:?}", message);
                }
            }

            if let Some(new_remaining_time_to_wait) = remaining_time_to_wait.checked_sub(Instant::now().elapsed()) {
                remaining_time_to_wait = new_remaining_time_to_wait;
            } else {
                break 'election;
            }
        }
    }

    election_won
}
