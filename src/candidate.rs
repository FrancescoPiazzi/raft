use std::collections::BTreeMap;
use std::ops::Range;
use std::time::{Duration, Instant};

use actum::actor_ref::ActorRef;
use actum::prelude::ActorBounds;
use rand::{thread_rng, Rng};
use tokio::time::{sleep, timeout};

use crate::common_state::CommonState;
use crate::messages::request_vote::RequestVoteRequest;
use crate::messages::*;
use crate::types::AppendEntriesClientResponse;

pub enum ElectionResult<SMin, SMout> {
    Won,
    Lost(Option<RaftMessage<SMin, SMout>>),
}

/// Behavior of the Raft server in candidate state.
pub async fn candidate_behavior<AB, SM, SMin, SMout>(
    cell: &mut AB,
    me: u32,
    common_state: &mut CommonState<SM, SMin, SMout>,
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin, SMout>>>,
    election_timeout: Range<Duration>,
) -> ElectionResult<SMin, SMout>
where
    AB: ActorBounds<RaftMessage<SMin, SMout>>,
    SM: Send,
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    let election_result;

    common_state.voted_for = Some(me);

    let mut votes_from_others = BTreeMap::<u32, bool>::new();

    'candidate: loop {
        tracing::debug!("starting a new election");

        common_state.current_term += 1;

        votes_from_others.clear();

        let mut remaining_time_to_wait = thread_rng().gen_range(election_timeout.clone());
        let last_applied = common_state.last_applied;

        let request = RequestVoteRequest {
            term: common_state.current_term,
            candidate_id: me,
            last_log_index: last_applied as u64,
            last_log_term: common_state.log.get_last_log_term(),
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

            if let Some(result) = handle_message_as_candidate(common_state, peers, &mut votes_from_others, message) {
                election_result = result;
                break 'candidate;
            }

            if let Some(new_remaining_time_to_wait) = remaining_time_to_wait.checked_sub(Instant::now().elapsed()) {
                remaining_time_to_wait = new_remaining_time_to_wait;
            } else {
                tracing::trace!("election timeout");
                break 'current_election;
            }
        }

        // if no winner is decleared by the end of the election, wait a random time to prevent split votes
        // from repeating forever
        let time_to_wait_before_new_election = thread_rng().gen_range(election_timeout.clone());
        sleep(time_to_wait_before_new_election).await;
    }

    election_result
}

/// Process a single message as the candidate
///
/// Returns Some(ElectionResult) if the election is over, None otherwise
fn handle_message_as_candidate<SM, SMin, SMout>(
    common_state: &mut CommonState<SM, SMin, SMout>,
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin, SMout>>>,
    votes_from_others: &mut BTreeMap<u32, bool>,
    message: RaftMessage<SMin, SMout>,
) -> Option<ElectionResult<SMin, SMout>> {
    tracing::trace!(message = ?message);

    match &message {
        RaftMessage::RequestVoteReply(reply) => {
            // formal specifications:310, don't count votes with terms different than the current
            if reply.term != common_state.current_term {
                return None;
            }
            if votes_from_others.contains_key(&reply.from) {
                tracing::error!("Candidate {} voted for me twice", reply.from);
                return None;
            }

            votes_from_others.insert(reply.from, reply.vote_granted);
            let n_granted_votes_including_self = votes_from_others.values().filter(|granted| **granted).count() + 1;
            let n_votes_against = votes_from_others.values().filter(|granted| !**granted).count();

            #[allow(clippy::int_plus_one)] // imo it's more clear with the +1
            if n_granted_votes_including_self >= peers.len() / 2 + 1 {
                Some(ElectionResult::Won)
            } else if n_votes_against >= peers.len() / 2 + 1 {
                // no need to process this message further since it was a RequestVoteReply, so I don't return it
                tracing::trace!("too many votes against, election lost");
                Some(ElectionResult::Lost(None))
            } else {
                None
            }
        }
        RaftMessage::AppendEntriesRequest(request) => {
            if common_state.update_term_stedile(request.term) {
                return Some(ElectionResult::Lost(Some(message)));
            }

            // request.term will never be > current_term, as we already checked that in update_term
            if request.term >= common_state.current_term {
                common_state.current_term = request.term;
                Some(ElectionResult::Lost(Some(message)))
            } else {
                None
            }
        }
        RaftMessage::RequestVoteRequest(request) => {
            if common_state.update_term_stedile(request.term) {
                return Some(ElectionResult::Lost(Some(message)));
            }
            
            // if the request had an higher term, we would have converted to follower already
            // so we can safely ignore it
            None
        }
        RaftMessage::AppendEntriesClientRequest(request) => {
            let _ = request.reply_to.try_send(AppendEntriesClientResponse(Err(None)));
            None
        }
        _ => {
            tracing::trace!(unhandled = ?message);
            None
        }
    }
}
