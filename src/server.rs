use std::ops::Range;
use std::time::Duration;

use actum::actor_bounds::ActorBounds;
use tracing::{info_span, Instrument};

use crate::candidate::{candidate_behavior, CandidateResult};
use crate::common_state::CommonState;
use crate::follower::{follower_behavior, FollowerResult};
use crate::leader::{leader_behavior, LeaderResult};
use crate::messages::*;
use crate::state_machine::StateMachine;

pub async fn raft_server<AB, SM, SMin, SMout>(
    mut cell: AB,
    me: u32,
    n_peers: usize,
    state_machine: SM,
    election_timeout: Range<Duration>,
    heartbeat_period: Duration,
) -> (AB, SM)
where
    AB: ActorBounds<RaftMessage<SMin, SMout>>,
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Send + Clone + 'static,
    SMout: Send + 'static,
{
    check_parameters(&election_timeout, &heartbeat_period);

    let mut common_state = CommonState::new(state_machine, me);
    let mut message_stash = Vec::<RaftMessage<SMin, SMout>>::new();

    tracing::trace!("obtaining peer references");

    while common_state.peers.len() < n_peers {
        let Some(message) = cell.recv().await.message() else {
            return (cell, common_state.state_machine);
        };

        match message {
            RaftMessage::AddPeer(peer) => {
                tracing::trace!(peer = ?peer);
                common_state.peers.insert(peer.peer_id, peer.peer_ref);
            }
            other => {
                tracing::trace!(stash = ?other);
                message_stash.push(other);
            }
        }
    }

    loop {
        let follower_result = follower_behavior(
            &mut cell,
            &mut common_state,
            election_timeout.clone(),
            &mut message_stash,
        )
        .instrument(info_span!("follower"))
        .await;

        match follower_result {
            FollowerResult::ElectionTimeout => {}
            FollowerResult::Stopped | FollowerResult::NoMoreSenders => break,
        }

        tracing::debug!("transition: follower → candidate");
        let candidate_result = candidate_behavior(&mut cell, &mut common_state, election_timeout.clone())
            .instrument(info_span!("candidate"))
            .await;

        match candidate_result {
            CandidateResult::ElectionWon => {
                tracing::debug!("⏩ transition: candidate → leader");
                let current_term = common_state.current_term;
                let leader_result = leader_behavior(&mut cell, &mut common_state, heartbeat_period)
                    .instrument(info_span!("leader👑()", term=current_term))
                    .await;

                match leader_result {
                    LeaderResult::Deposed => tracing::debug!("⏩ transition: leader → follower"),
                    LeaderResult::Stopped | LeaderResult::NoMoreSenders => break,
                }
            }
            CandidateResult::ElectionLost => tracing::debug!("⏩ transition: candidate → follower"),
            CandidateResult::Stopped | CandidateResult::NoMoreSenders => break,
        }
    }

    tracing::trace!("returning");
    (cell, common_state.state_machine)
}

fn check_parameters(election_timeout: &Range<Duration>, heartbeat_period: &Duration) {
    assert!(
        election_timeout.start <= election_timeout.end,
        "election_timeout start must be less than end"
    );
    assert!(
        *heartbeat_period > Duration::from_secs(0),
        "heartbeat_period must be greater than 0"
    );

    if election_timeout.start < *heartbeat_period {
        tracing::error!(
            "election_timeout start is less than heartbeat_period, 
            this will cause followers to always time out"
        );
    }
    if election_timeout.end < *heartbeat_period {
        tracing::warn!(
            "election_timeout end is less than heartbeat_period, 
            this may cause followers to time out even when the leader is working"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use actum::actum;
    use actum::prelude::*;
    use tracing::info_span;
    use tracing::Instrument;

    use crate::messages::append_entries::AppendEntriesRequest;
    use crate::messages::RaftMessage;
    use crate::server::add_peer::AddPeer;
    use crate::server::raft_server;
    use crate::state_machine::VoidStateMachine;

    #[tokio::test]
    async fn test_stash() {
        let void_state_machine_1 = VoidStateMachine::new();
        let void_state_machine_2 = void_state_machine_1.clone();
        let mut actor1 = actum::<RaftMessage<_, _>, _, _, _>(move |cell, _| async move {
            let actor = raft_server(
                cell,
                1,
                1,
                void_state_machine_1,
                Duration::from_millis(100)..Duration::from_millis(100),
                Duration::from_millis(50),
            )
            .await;
            actor
        });

        let actor2 = actum::<RaftMessage<_, _>, _, _, _>(move |cell, _| async move {
            let actor = raft_server(
                cell,
                1,
                1,
                void_state_machine_2,
                Duration::from_millis(100)..Duration::from_millis(100),
                Duration::from_millis(50),
            )
            .await;
            actor
        });

        let _handle1 = tokio::spawn(actor1.task.run_task().instrument(info_span!("test")));
        let _handle2 = tokio::spawn(actor2.task.run_task().instrument(info_span!("test")));

        let request = AppendEntriesRequest {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let _ = actor1.m_ref.try_send(request.into());

        let _ = actor1.m_ref.try_send(
            AddPeer {
                peer_id: 1,
                peer_ref: actor2.m_ref.clone(),
            }
            .into(),
        );

        // actor 1 should have sent the message to itself here
    }
}
