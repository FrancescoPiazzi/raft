use std::time::Duration;

use crate::config::{DEFAULT_ELECTION_TIMEOUT, REPLICATION_PERIOD};
use crate::messages::*;
use tracing::{info_span, Instrument};

use crate::candidate::candidate;
use crate::common_state::CommonState;
use crate::follower::follower;
use crate::leader::leader;

use actum::prelude::*;

pub async fn raft_server<AB, LogEntry>(
    mut cell: AB,
    me: ActorRef<RaftMessage<LogEntry>>,
    server_id: u32,
    n_servers: usize,
) -> AB
where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + Clone + 'static,
{
    let mut state = CommonState::new();

    let (mut peer_refs, peer_ids) = init(&mut cell, n_servers).await;

    let election_timeout = {
        #[cfg(test)]
        {
            Duration::from_millis(1000)..Duration::from_millis(1000) // TOASK: isn't this a magic number?
        }
        #[cfg(not(test))]
        {
            DEFAULT_ELECTION_TIMEOUT
        }
    };

    // worst case scenario, we send 3/4 heartbeats before followers time out,
    // meaning 2 can get lost without the follower thinking we are down
    // TOASK: is there a specific number of heartbeats/min_timeout in the paper?
    let hartbeat_period = election_timeout.start / 4;

    tracing::trace!("starting as follower");

    loop {
        follower(&me, server_id, &mut cell, &mut state, election_timeout.clone())
            .instrument(info_span!("follower"))
            .await;

        tracing::trace!("transition: follower → candidate");
        let election_won = candidate(&mut cell, &me, &mut state, &mut peer_refs, election_timeout.clone())
            .instrument(info_span!("candidate"))
            .await;

        if election_won {
            tracing::trace!("transition: candidate → leader");
            leader(
                &mut cell,
                &mut state,
                &peer_refs,
                &peer_ids,
                &me,
                hartbeat_period,
                REPLICATION_PERIOD,
            )
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
async fn init<AB, LogEntry>(cell: &mut AB, total_nodes: usize) -> (Vec<ActorRef<RaftMessage<LogEntry>>>, Vec<u32>)
where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + 'static,
{
    let mut peers: Vec<ActorRef<RaftMessage<LogEntry>>> = Vec::with_capacity(total_nodes - 1);
    let mut peer_ids: Vec<u32> = Vec::with_capacity(total_nodes - 1);

    let mut npeers = 0;
    while npeers < total_nodes - 1 {
        let msg = cell.recv().await.message();
        match msg {
            Some(message) => {
                if let RaftMessage::AddPeer(peer, id) = message {
                    npeers += 1;
                    peers.push(peer);
                    peer_ids.push(id);
                    tracing::trace!("🙆 Peer {} added", id);
                }
            }
            None => {
                tracing::info!("Received a None message, quitting");
                panic!("Received a None message");
            }
        }
    }

    (peers, peer_ids)
}