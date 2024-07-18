use std::time::Duration;
use tokio::time::timeout;

use actum::prelude::*;

// TODO: replace with any type that is Send + 'static
type Message = String;

pub enum RaftMessage {
    AddPeer(ActorRef<RaftMessage>),

    AppendEntries(Vec<Message>),

    // only failure that we will answer is if we are not the leader, so we send the address of the leader back
    AppendEntryResponse(Result<(), ActorRef<RaftMessage>>),

    RequestVote,

    RequestVoteResponse,
}

pub async fn raft_actor<AB>(mut cell: AB, _me: ActorRef<RaftMessage>)
where
    AB: ActorBounds<RaftMessage>,
{
    let total_nodes = 5;

    let mut log: Vec<Message> = Vec::new();

    let mut commit_index = 0;
    let mut last_applied = 0;

    let mut current_term = 0;
    let mut voted_for: Option<ActorRef<RaftMessage>> = None;

    let peers = init(&mut cell, total_nodes).await;

    tracing::trace!("initialization done");

    follower(
        &mut cell,
        &mut log,
        &mut commit_index,
        &mut last_applied,
        &mut current_term,
        &mut voted_for,
    )
    .await;
}

// this function is not part of the raft protocol,
// however it is needed to receive the references of the other servers
// since they are memory addresses, we can't know them in advance,
// when actum will switch to a different type of actor reference like a network address,
// this function can be made to read from a file the addresses of the other servers instead
async fn init<AB>(cell: &mut AB, total_nodes: usize) -> Vec<ActorRef<RaftMessage>>
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
                    tracing::trace!("➕ Peer added, total: {}", npeers);
                }
                _ => {
                    tracing::warn!("❔ Received a message that is not AddPeer, ignoring, this should not happen");
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

// follower nodes receive AppendEntry messages from the leader and execute them
// they ping the leader to see if it's still alive, if it isn't, they start an election
async fn follower<AB>(
    cell: &mut AB,
    log: &mut Vec<Message>,
    commit_index: &mut usize,
    last_applied: &mut usize,
    current_term: &mut u64,
    voted_for: &mut Option<ActorRef<RaftMessage>>,
) where
    AB: ActorBounds<RaftMessage>,
{
    let max_time_before_election = Duration::from_secs(10);
    
    loop {
        let wait_res = timeout(max_time_before_election, cell.recv()).await;

        match wait_res {
            Ok(received_message) => match received_message.message() {
                Some(raftmessage) => match raftmessage {
                    RaftMessage::AppendEntries(mut entries) => {
                        tracing::info!("✏️ Received an AppendEntries message, adding them to the log");
                        log.append(&mut entries);
                    }
                    RaftMessage::RequestVote => {
                        // TODO: check if we can vote for the candidate
                    }
                    _ => {
                        tracing::warn!("❔ Received a message that is not AppendEntry or RequestVote while in follower mode, ignoring");
                    }
                },
                None => {
                    tracing::info!("Received a None message, quitting");
                    break;
                }
            },
            Err(_) => {
                tracing::info!("⏰ Timeout reached, starting an election (or at least pretending to)");
            }
        }
    }
}

async fn candidate() {}

async fn leader() {}
