use crate::messages::*;
use actum::actor_bounds::ActorBounds;
use actum::actor_ref::ActorRef;
use std::collections::BTreeMap;
use std::ops::Range;
use std::time::Duration;
use tracing::{info_span, Instrument};

use crate::candidate::candidate;
use crate::common_state::CommonState;
use crate::follower::follower;
use crate::leader::leader;

pub async fn raft_server<AB, LogEntry>(
    mut cell: AB,
    me: (u32, ActorRef<RaftMessage<LogEntry>>),
    n_peers: usize,
    election_timeout: Range<Duration>,
    heartbeat_period: Duration,
    replication_period: Duration,
) -> AB
where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + Clone + 'static,
{
    let mut peers = BTreeMap::<u32, ActorRef<RaftMessage<LogEntry>>>::new();
    let mut message_stash = Vec::<RaftMessage<LogEntry>>::new();

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

    let mut common_state = CommonState::new();

    for message in message_stash {
        match message {
            RaftMessage::AddPeer(_) => unreachable!(),
            RaftMessage::AppendEntriesRequest(_) => {}
            RaftMessage::AppendEntriesReply(_) => {}
            RaftMessage::RequestVoteRequest(_) => {}
            RaftMessage::RequestVoteReply(_) => {}
            RaftMessage::AppendEntriesClientRequest(_) => {}
        }
    }

    loop {
        follower(&mut cell, me.0, &mut peers, &mut common_state, election_timeout.clone())
            .instrument(info_span!("follower"))
            .await;

        tracing::trace!("transition: follower → candidate");
        let election_won = candidate(&mut cell, me.0, &mut common_state, &mut peers, election_timeout.clone())
            .instrument(info_span!("candidate"))
            .await;

        if election_won {
            tracing::trace!("transition: candidate → leader");
            leader(
                &mut cell,
                me.0,
                &mut common_state,
                &mut peers,
                heartbeat_period,
                replication_period,
            )
            .instrument(info_span!("leader👑"))
            .await;
        } else {
            tracing::trace!("transition: candidate → follower");
        }
    }
}
