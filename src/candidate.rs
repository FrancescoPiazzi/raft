use std::collections::BTreeMap;
use std::ops::Range;
use std::time::{Duration, Instant};

use actum::actor_ref::ActorRef;
use actum::prelude::ActorBounds;
use either::Either;
use rand::{thread_rng, Rng};
use tokio::time::{sleep, timeout};

use crate::common_message_handling::{handle_append_entries_request, handle_vote_request, RaftState};
use crate::common_state::CommonState;
use crate::messages::append_entries::AppendEntriesRequest;
use crate::messages::request_vote::{RequestVoteReply, RequestVoteRequest};
use crate::messages::*;
use crate::state_machine::StateMachine;
use crate::types::AppendEntriesClientResponse;

pub enum ElectionResult<SMin> {
    Won,
    LostDueToHigherTerm(Either<AppendEntriesRequest<SMin>, RequestVoteRequest>),
    LostDueTooManyNegativeVotes,
}

/// Behavior of the Raft server in candidate state.
/// 
/// Returns `true` if the candidate won the election, `false` otherwise.
pub async fn candidate_behavior<AB, SM, SMin, SMout>(
    cell: &mut AB,
    me: u32,
    common_state: &mut CommonState<SM, SMin, SMout>,
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin, SMout>>>,
    election_timeout: Range<Duration>,
) -> bool
where
    AB: ActorBounds<RaftMessage<SMin, SMout>>,
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    common_state.voted_for = Some(me);

    let mut votes_from_others = BTreeMap::<u32, bool>::new();

    loop {
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
            let start_time = Instant::now();

            let Ok(message) = timeout(remaining_time_to_wait, cell.recv()).await else {
                tracing::trace!("election timeout");
                break 'current_election;
            };

            let message = message.message().expect("raft runs indefinitely");

            tracing::trace!(message = ?message);

            match message {
                RaftMessage::RequestVoteReply(reply) => {
                    let result = handle_request_vote_reply(common_state, peers, &mut votes_from_others, reply);
                    match result {
                        HandleRequestVoteReplyResult::Ongoing => {}
                        HandleRequestVoteReplyResult::Won => return true,
                        HandleRequestVoteReplyResult::Lost => return false
                    }
                }
                RaftMessage::AppendEntriesRequest(request) => {
                    let step_down = handle_append_entries_request(me, common_state, peers, RaftState::Candidate, request);
                    if step_down {
                        return false;
                    }
                }
                RaftMessage::RequestVoteRequest(request) => {
                    let step_down = handle_vote_request(me, common_state, peers, request);
                    if step_down {
                        return false;
                    }
                }
                RaftMessage::AppendEntriesClientRequest(request) => {
                    let response = AppendEntriesClientResponse(Err(None));
                    let _ = request.reply_to.try_send(response);
                }
                _ => {
                    tracing::trace!(unhandled = ?message);
                }
            }

            if let Some(new_remaining_time_to_wait) = remaining_time_to_wait.checked_sub(start_time.elapsed()) {
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
}

enum HandleRequestVoteReplyResult {
    Ongoing,
    Won,
    Lost,
}

#[tracing::instrument(level = "trace", skip_all)]
fn handle_request_vote_reply<SM, SMin, SMout>(
    common_state: &mut CommonState<SM, SMin, SMout>,
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin, SMout>>>,
    votes_from_others: &mut BTreeMap<u32, bool>,
    reply: RequestVoteReply,
) -> HandleRequestVoteReplyResult
where
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    // formal specifications:310, don't count votes with terms different than the current
    if reply.term != common_state.current_term {
        return HandleRequestVoteReplyResult::Ongoing;
    }

    if votes_from_others.contains_key(&reply.from) {
        tracing::error!("Candidate {} voted for me twice", reply.from);
        return HandleRequestVoteReplyResult::Ongoing;
    }

    votes_from_others.insert(reply.from, reply.vote_granted);
    let n_granted_votes_including_self = votes_from_others.values().filter(|granted| **granted).count() + 1;
    let n_votes_against = votes_from_others.values().filter(|granted| !**granted).count();

    #[allow(clippy::int_plus_one)]
    if n_granted_votes_including_self >= peers.len() / 2 + 1 {
        HandleRequestVoteReplyResult::Won
    } else if n_votes_against >= peers.len() / 2 + 1 {
        // no need to process this message further since it was a RequestVoteReply, so I don't return it
        tracing::trace!("too many votes against, election lost");
        HandleRequestVoteReplyResult::Lost
    } else {
        HandleRequestVoteReplyResult::Ongoing
    }
}
