use std::ops::Range;
use std::time::Duration;
use tracing::{info_span, Instrument};

use crate::raft::candidate::candidate;
use crate::raft::common_state::CommonState;
use crate::raft::follower::follower;
use crate::raft::leader::leader;
use crate::raft::messages::*;

use actum::prelude::*;

pub async fn raft_actor<AB, LogEntry>(
    mut cell: AB,
    me: ActorRef<RaftMessage<LogEntry>>,
    election_timeout: Range<Duration>,
) -> AB
where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + Clone + 'static,
{
    let total_nodes = 5;

    let mut common_data = CommonState {
        current_term: 0,
        log: Vec::new(),
        commit_index: 0,
        last_applied: 0,
        voted_for: None,
    };

    let mut peer_refs = init(&mut cell, total_nodes).await;

    tracing::trace!("starting as follower");

    loop {
        follower(&mut cell, &mut common_data)
            .instrument(info_span!("follower"))
            .await;

        tracing::trace!("transition: follower → candidate");
        let election_won = candidate(
            &mut cell,
            &me,
            &mut common_data,
            &mut peer_refs,
            election_timeout.clone(),
        )
        .instrument(info_span!("candidate"))
        .await;

        if election_won {
            tracing::trace!("transition: candidate → leader");
            leader(&mut cell, &mut common_data, &mut peer_refs, &me)
                .instrument(info_span!("leader👑"))
                .await;
        } else {
            tracing::trace!("transition: candidate → follower");
        }
    }
}

// this function is not part of the raft protocol,
// however it is needed to receive the references of the other servers,
// since they are memory addresses, we can't know them in advance,
// when actum will switch to a different type of actor reference like a network address,
// this function can be made to read from a file the addresses of the other servers instead
async fn init<AB, LogEntry>(cell: &mut AB, total_nodes: usize) -> Vec<ActorRef<RaftMessage<LogEntry>>>
where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + 'static,
{
    let mut peers: Vec<ActorRef<RaftMessage<LogEntry>>> = Vec::with_capacity(total_nodes - 1);

    let mut npeers = 0;
    while npeers < total_nodes - 1 {
        let msg = cell.recv().await.message();
        match msg {
            Some(message) => {
                if let RaftMessage::AddPeer(peer) = message {
                    npeers += 1;
                    peers.push(peer);
                    tracing::trace!("🙆 Peer added, total: {}", npeers);
                }
            }
            None => {
                tracing::info!("Received a None message, quitting");
                panic!("Received a None message");
            }
        }
    }

    peers
}
