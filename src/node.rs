use actum::prelude::*;

pub enum RaftMessage {
    AddPeer(ActorRef<RaftMessage>),
    AppendEntry(String), // TODO: replace with any type that is Send + 'static
    AppendEntryResponse,
    RequestVote,
    RequestVoteResponse,
}

pub async fn raft_actor<AB>(mut cell: AB, _me: ActorRef<RaftMessage>)
where
    AB: ActorBounds<RaftMessage>,
{
    let total_nodes = 5;

    let peers = init(cell, total_nodes).await;
    tracing::info!("peers initialized");
}

// this function is not part of the raft protocol,
// however it is needed to receive the references of the other servers
// since they are memory addresses, we can't know them in advance,
// when actum will switch to a different type of actor reference like a network address,
// this function can be made to read from a file the addresses of the other servers instead
async fn init<AB>(mut cell: AB, total_nodes: usize) -> Vec<ActorRef<RaftMessage>>
where
    AB: ActorBounds<RaftMessage>,
{
    let mut peers: Vec<ActorRef<RaftMessage>> = Vec::with_capacity(total_nodes - 1);

    let mut npeers = 0;
    while npeers < total_nodes - 1 {
        let msg = cell.recv().await.message();
        match msg {
            Some(raftmessage) => match raftmessage {
                RaftMessage::AddPeer(peer) => {
                    npeers += 1;
                    peers.push(peer);
                    tracing::trace!("Peer added, total: {}", npeers);
                }
                _ => {
                    tracing::warn!("Received a message that is not AddPeer, ignoring, this should not happen");
                }
            },
            None => {
                tracing::info!("Received a None message, quitting");
                break;
            }
        }
    }

    peers
}

async fn follower() {}

async fn candidate() {}

async fn leader() {}
