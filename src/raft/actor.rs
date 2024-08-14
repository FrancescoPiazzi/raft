use actum::prelude::*;

use crate::raft::model::*;

use crate::raft::candidate::candidate;
use crate::raft::follower::follower;
use crate::raft::leader::leader;

pub async fn raft_actor<AB, LogEntry>(mut cell: AB, me: ActorRef<RaftMessage<LogEntry>>) -> AB
where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + Clone + 'static,
{
    let total_nodes = 5;

    let mut common_data = CommonData {
        current_term: 0,
        log: Vec::new(),
        commit_index: 0,
        last_applied: 0,
        voted_for: None,
    };

    let mut peer_refs = init(&mut cell, total_nodes).await;

    tracing::trace!("initialization done");

    loop {
        follower(&mut cell, &mut common_data).await;
        let election_won = candidate(&mut cell, &me, &mut common_data, &mut peer_refs).await;
        if election_won {
            leader(&mut cell, &mut common_data, &mut peer_refs, &me).await;
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
            Some(message) => if let RaftMessage::AddPeer(peer) = message {
                npeers += 1;
                peers.push(peer);
                tracing::trace!("🙆 Peer added, total: {}", npeers);
            },
            None => {
                tracing::info!("Received a None message, quitting");
                panic!("Received a None message");
            }
        }
    }

    peers
}
