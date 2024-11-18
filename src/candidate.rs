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

/// Behavior of the Raft server in candidate state.
///
/// Returns true if the candidate has won the election, false otherwise.
pub async fn candidate_behavior<AB, SM, SMin, SMout>(
    cell: &mut AB,
    me: u32,
    common_state: &mut CommonState<SM, SMin, SMout>,
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin>>>,
    election_timeout: Range<Duration>,
) -> bool
where
    AB: ActorBounds<RaftMessage<SMin>>,
    SM: Send,
    SMin: Clone + Send + 'static,
    SMout: Send,
{
    let mut election_won = false;

    common_state.voted_for = Some(me);

    let mut votes_from_others = BTreeMap::<u32, bool>::new();

    'candidate: loop {
        tracing::trace!("starting a new election");

        common_state.current_term += 1;

        votes_from_others.clear();

        let mut remaining_time_to_wait = thread_rng().gen_range(election_timeout.clone());

        let request = RequestVoteRequest {
            term: common_state.current_term,
            candidate_id: me,
            last_log_index: 0,
            last_log_term: 0,
        };

        for peer in peers.values_mut() {
            let _ = peer.try_send(request.clone().into());
        }

        'current_election: loop {
            let Ok(message) = timeout(remaining_time_to_wait, cell.recv()).await else {
                tracing::trace!("election timeout");
                break 'current_election;
            };

            let message = message.message().expect("raft runs indefinitely");
            tracing::trace!(message = ?message);

            match message {
                RaftMessage::RequestVoteReply(request_vote_reply) => {
                    votes_from_others.insert(request_vote_reply.from, request_vote_reply.vote_granted);
                    let n_granted_votes_including_self =
                        votes_from_others.values().filter(|granted| **granted).count() + 1;

                    if n_granted_votes_including_self > peers.len() / 2 + 1 {
                        election_won = true;
                        break 'candidate;
                    }
                }
                RaftMessage::AppendEntriesRequest(append_entry_rpc) => {
                    if append_entry_rpc.term >= common_state.current_term {
                        election_won = false;
                        common_state.current_term = append_entry_rpc.term;
                        break 'candidate;
                    }
                }
                RaftMessage::RequestVoteRequest(request_vote_rpc) => {
                    // reminder: candidates never vote for others, as they have already voted for themselves
                    if request_vote_rpc.term > common_state.current_term {
                        election_won = false;
                        common_state.current_term = request_vote_rpc.term;
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
                tracing::trace!("election timeout");
                break 'current_election;
            }
        }
    }

    election_won
}
