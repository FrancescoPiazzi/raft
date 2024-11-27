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
        tracing::debug!("starting a new election");

        common_state.current_term += 1;

        votes_from_others.clear();

        let mut remaining_time_to_wait = thread_rng().gen_range(election_timeout.clone());
        let last_applied = common_state.last_applied;

        let request = RequestVoteRequest {
            term: common_state.current_term,
            candidate_id: me,
            last_log_index: last_applied as u64,
            last_log_term: if last_applied == 0 { 0 } else { common_state.log.get_term(last_applied) },
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
            
            if let Some(election_result) = handle_message_as_candidate(common_state,peers, &mut votes_from_others, message){
                election_won = election_result;
                break 'candidate;
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

/// Process a single message as the candidate
/// 
/// Returns Some(true) if election is won, Some(false) if it is lost, and None if it's still ongoing
fn handle_message_as_candidate<SM, SMin, SMout>(
    common_state: &mut CommonState<SM, SMin, SMout>,
    peers: &mut BTreeMap<u32, ActorRef<RaftMessage<SMin>>>,
    votes_from_others: &mut BTreeMap<u32, bool>,
    message: RaftMessage<SMin>
) -> Option<bool> 
{
    match message {
        RaftMessage::RequestVoteReply(reply) => {
            votes_from_others.insert(reply.from, reply.vote_granted);
            let n_granted_votes_including_self =
                votes_from_others.values().filter(|granted| **granted).count() + 1;
            
            // +1 before sub. or it always underflows 
            // TOTHINK: why always? It should only happen when everyone votes for me, which is usually not the case, 
            // since I will win the election before that happens
            tracing::trace!("others len: {}, n votes including self: {}", votes_from_others.len(), n_granted_votes_including_self);
            let n_votes_against = votes_from_others.len() + 1 - n_granted_votes_including_self;

            if n_granted_votes_including_self > peers.len() / 2 + 1 {
                Some(true)
            } else if n_votes_against > peers.len() / 2 + 1 {
                Some(false)
            } else {
                None
            }
        }
        RaftMessage::AppendEntriesRequest(request) => {
            if request.term >= common_state.current_term {
                common_state.current_term = request.term;
                Some(false)
            } else {
                None
            }
        }
        RaftMessage::RequestVoteRequest(request) => {
            // reminder: candidates never vote for others, as they have already voted for themselves
            // TOTHINK: do they? one would argue that they will vote for somone on a different term, so it doesn't count
            // but the formal specifications seem to imply they don't (it also seems to imply noone ever votes tough)
            if request.term > common_state.current_term {
                common_state.current_term = request.term;
                Some(false)
            } else {
                None
            }
        }
        _ => {
            tracing::trace!(unhandled = ?message);
            None
        }
    }
}