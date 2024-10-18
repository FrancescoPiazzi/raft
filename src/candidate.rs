use std::collections::BTreeMap;
use std::ops::Range;
use std::time::{Duration, Instant};

use actum::actor_ref::ActorRef;
use actum::prelude::ActorBounds;
use rand::{thread_rng, Rng};
use tokio::time::timeout;

use crate::common_state::CommonState;
use crate::messages::request_vote::RequestVoteRequest;
use crate::messages::*;

// candidate nodes start an election by sending RequestVote messages to the other nodes
// if they receive a majority of votes, they become the leader
// if they receive a message from a node with a higher term, they become a follower
// returns true if the election was won, false if it was lost
pub async fn candidate<AB, SM, SMin, SMout>(
    cell: &mut AB,
    me: u32,
    common_data: &mut CommonState<SM, SMin, SMout>,
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin>>>,
    election_timeout: Range<Duration>,
) -> bool
where
    AB: ActorBounds<RaftMessage<SMin>>,
    SM: Send,
    SMin: Clone + Send + 'static,
    SMout: Send,
{
    let election_won;

    'candidate: loop {
        tracing::info!("📦 Starting an election");

        let mut votes = 1;

        let mut remaining_time_to_wait = thread_rng().gen_range(election_timeout.clone());

        common_data.current_term += 1;

        let request = RequestVoteRequest {
            term: common_data.current_term,
            candidate_id: me,
            last_log_index: 0,
            last_log_term: 0,
        };

        for peer in peers.values_mut() {
            let _ = peer.try_send(request.clone().into());
        }

        'election: loop {
            let Ok(message) = timeout(remaining_time_to_wait, cell.recv()).await else {
                tracing::info!("election timeout");
                break 'election;
            };

            let message = message.message().expect("raft runs indefinitely");
            tracing::info!(message = ?message);

            match message {
                RaftMessage::RequestVoteReply(request_vote_reply) => {
                    if request_vote_reply.vote_granted {
                        votes += 1;
                        if votes > peers.len() / 2 + 1 {
                            election_won = true;
                            break 'candidate;
                        }
                    }
                }
                RaftMessage::AppendEntriesRequest(append_entry_rpc) => {
                    if append_entry_rpc.term >= common_data.current_term {
                        election_won = false;
                        common_data.current_term = append_entry_rpc.term;
                        break 'candidate;
                    }
                }
                // TOASK: >= or > ?
                // reminder: candidates never vote for others, as they have already voted for themselves
                RaftMessage::RequestVoteRequest(request_vote_rpc) => {
                    if request_vote_rpc.term >= common_data.current_term {
                        election_won = false;
                        common_data.current_term = request_vote_rpc.term;
                        break 'candidate;
                    }
                }
                _ => {
                    tracing::trace!(unhandled = ?message);
                }
            }

            if let Some(new_remaining_time_to_wait) = remaining_time_to_wait.checked_sub(Instant::now().elapsed()) {
                remaining_time_to_wait = new_remaining_time_to_wait;
            } else {
                tracing::info!("election timeout");
                break 'election;
            }
        }
    }

    election_won
}
