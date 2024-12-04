use std::collections::BTreeMap;
use std::ops::Range;
use std::time::Duration;

use actum::actor_bounds::ActorBounds;
use actum::actor_ref::ActorRef;
use tracing::{info_span, Instrument};

use crate::candidate::candidate_behavior;
use crate::common_state::CommonState;
use crate::follower::follower_behavior;
use crate::leader::leader_behavior;
use crate::messages::*;
use crate::state_machine::StateMachine;
use crate::candidate::ElectionResult;

pub async fn raft_server<AB, SM, SMin, SMout>(
    mut cell: AB,
    mut me: (u32, ActorRef<RaftMessage<SMin, SMout>>),
    n_peers: usize,
    state_machine: SM,
    election_timeout: Range<Duration>,
    heartbeat_period: Duration,
    replication_period: Duration,
) -> AB
where
    AB: ActorBounds<RaftMessage<SMin, SMout>>,
    SM: StateMachine<SMin, SMout> + Send,
    SMin: Send + Clone + 'static,
    SMout: Send + 'static,
{
    check_parameters(&election_timeout, &heartbeat_period, &replication_period);

    let mut peers = BTreeMap::<u32, ActorRef<RaftMessage<SMin, SMout>>>::new();
    let mut message_stash = Vec::<RaftMessage<SMin, SMout>>::new();

    tracing::trace!("obtaining peer references");

    while peers.len() < n_peers {
        match cell.recv().await.message().expect("raft runs indefinitely") {
            RaftMessage::AddPeer(peer) => {
                tracing::trace!(peer = ?peer);
                peers.insert(peer.peer_id, peer.peer_ref);
            }
            other => {
                tracing::trace!(stash = ?other);
                message_stash.push(other);
            }
        }
    }

    let mut common_state = CommonState::new(state_machine);

    for message in message_stash {
        match message {
            RaftMessage::AddPeer(_) => unreachable!(),
            _ => {
                let _ = me.1.try_send(message);
            }
        }
    }

    loop {
        follower_behavior(&mut cell, me.0, &mut peers, &mut common_state, election_timeout.clone())
            .instrument(info_span!("follower"))
            .await;

        tracing::debug!("transition: follower → candidate");
        let election_result = candidate_behavior(&mut cell, me.0, &mut common_state, &mut peers, election_timeout.clone())
            .instrument(info_span!("candidate"))
            .await;

        match election_result {
            ElectionResult::WON => {
                tracing::debug!("transition: candidate → leader");
                let message = leader_behavior(&mut cell, me.0, &mut common_state, &mut peers, heartbeat_period)
                    .instrument(info_span!("leader👑"))
                    .await;
                let _ = me.1.try_send(message);
            }
            ElectionResult::LOST(message) => {
                tracing::debug!("transition: candidate → follower");
                if let Some(message) = message {
                    let _ = me.1.try_send(message);
                }
            }
        }
    }
}

fn check_parameters(election_timeout: &Range<Duration>, heartbeat_period: &Duration, replication_period: &Duration) {
    assert!(
        election_timeout.start < election_timeout.end,
        "election_timeout start must be less than end"
    );
    assert!(
        *heartbeat_period > Duration::from_secs(0),
        "heartbeat_period must be greater than 0"
    );
    assert!(
        *replication_period > Duration::from_secs(0),
        "replication_period must be greater than 0"
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
    use crate::state_machine::StateMachine;

    #[derive(Clone)]
    struct VoidStateMachine;

    impl VoidStateMachine {
        fn new() -> Self {
            VoidStateMachine
        }
    }

    impl StateMachine<(), ()> for VoidStateMachine {
        fn apply(&mut self, _: &()) -> () {}
    }

    #[tokio::test]
    async fn test_stash() {
        let void_state_machine_1 = VoidStateMachine::new();
        let void_state_machine_2 = void_state_machine_1.clone();
        let mut actor1 = actum::<RaftMessage<_, _>, _, _>(move |cell, me| async move {
            let actor = raft_server(
                cell,
                (0, me),
                1,
                void_state_machine_1,
                Duration::from_millis(100)..Duration::from_millis(100),
                Duration::from_millis(50),
                Duration::from_millis(50),
            )
            .await;
            actor
        });

        let actor2 = actum::<RaftMessage<_, _>, _, _>(move |cell, me| async move {
            let actor = raft_server(
                cell,
                (1, me),
                1,
                void_state_machine_2,
                Duration::from_millis(100)..Duration::from_millis(100),
                Duration::from_millis(50),
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
