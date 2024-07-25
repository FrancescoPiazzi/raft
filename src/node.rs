use std::time::Duration;
use tokio::time::timeout;

use actum::prelude::*;

// TODO: replace with any type that is Send + 'static
type Message = String;

pub enum RaftMessage {
    AddPeer(ActorRef<RaftMessage>),

    // update, vec of length 0 is sent as hartbeat
    AppendEntries(Vec<Message>),

    // only failure that we will answer is if we are not the leader, so we send the address of the leader back
    AppendEntryResponse(Result<(), ActorRef<RaftMessage>>),

    // the candidate that initiared the vote, stuff about the vote
    RequestVote(ActorRef<RaftMessage>, RequestVoteRPC),

    // true if the vote was granted, false otherwise
    RequestVoteResponse(bool),
}

#[derive(Debug)]
enum RaftState {
    Follower,
    Candidate,
    Leader,
}

struct RequestVoteRPC {
    term: u64,
    candidate_ref: ActorRef<RaftMessage>,
    last_log_index: usize,
    last_log_term: u64,
}

// data common to all states, used to avoid passing a million parameters to the state functions
struct CommonData {
    current_term: u64,
    log: Vec<Message>,
    commit_index: usize,
    last_applied: usize,
    voted_for: Option<ActorRef<RaftMessage>>,
}


pub async fn raft_actor<AB>(mut cell: AB, _me: ActorRef<RaftMessage>) -> AB
where
    AB: ActorBounds<RaftMessage>,
{
    let total_nodes = 5;

    let mut common_data = CommonData {
        current_term: 0,
        log: Vec::new(),
        commit_index: 0,
        last_applied: 0,
        voted_for: None,
    };

    let mut state: RaftState = RaftState::Follower;

    let peer_refs = init(&mut cell, total_nodes).await;

    tracing::trace!("initialization done");

    loop {
        state = match state {
            RaftState::Follower => follower(&mut cell, &mut common_data).await,
            RaftState::Candidate => candidate(&mut cell, &mut common_data).await,
            RaftState::Leader => leader(&mut cell, &mut common_data).await,
        };
    }

    #[allow(unreachable_code)]
    cell
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
                panic!("Received a None message");
            }
        }
    }

    peers
}

// follower nodes receive AppendEntry messages from the leader and execute them
// they ping the leader to see if it's still alive, if it isn't, they start an election
async fn follower<AB>(
    cell: &mut AB,
    common_data: &mut CommonData,
) -> RaftState where
    AB: ActorBounds<RaftMessage>,
{
    tracing::info!("👂 State is follower");
    let max_time_before_election = Duration::from_secs(5);

    loop {
        let wait_res = timeout(max_time_before_election, cell.recv()).await;

        match wait_res {
            Ok(received_message) => match received_message.message() {
                Some(raftmessage) => match raftmessage {
                    RaftMessage::AppendEntries(mut entries) => {
                        tracing::trace!("✏️ Received an AppendEntries message, adding them to the log");
                        common_data.log.append(&mut entries);
                    }
                    RaftMessage::RequestVote(mut candidate, request_vote_rpc) => {
                        // check if the candidate log is at least as up-to-date as our log
                        // if it is and we haven't voted for anyone yet, vote for the candidate
                        // also check the term of the candidate, if it's higher, update our term
                        // TODO: figure out how to compare two nodes using actum because partialEq is implemented but the compiler refuses to acknowledge it
                        if request_vote_rpc.term >= common_data.current_term &&
                        (common_data.voted_for.is_none() || std::ptr::eq(common_data.voted_for.as_ref().unwrap(), &request_vote_rpc.candidate_ref)) {
                            let _ = candidate.try_send(RaftMessage::RequestVoteResponse(true));
                            common_data.voted_for = Some(request_vote_rpc.candidate_ref);
                        } else {
                            let _ = candidate.try_send(RaftMessage::RequestVoteResponse(false));
                        }
                    }
                    _ => {
                        tracing::warn!("❔ Received a message that is not AppendEntry or RequestVote while in follower mode, ignoring");
                    }
                },
                None => {
                    tracing::info!("Received a None message, quitting");
                    panic!("Received a None message");
                }
            },
            Err(_) => {
                tracing::info!("⏰ Timeout reached, starting an election");
                break;
            }
        }
    }

    return RaftState::Candidate;
}

async fn candidate<AB>(
    cell: &mut AB,
    common_data: &mut CommonData,
) -> RaftState where
    AB: ActorBounds<RaftMessage>,
{
    tracing::info!("🤵 State is candidate");
    loop{

    }
}

async fn leader<AB>(
    cell: &mut AB,
    common_data: &mut CommonData,
) -> RaftState where
    AB: ActorBounds<RaftMessage>,
{
    tracing::info!("👑 State is leader");
    loop{

    }

    return RaftState::Follower;
}
