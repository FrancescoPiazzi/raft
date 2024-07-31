use rand::random;
use std::time::Duration;
use std::time::Instant;
use tokio::time::timeout;

use actum::prelude::*;

// trait LogEntry: Send + Clone + 'static {}

#[allow(private_interfaces)] // I need this to be public only to create a node of RaftMessage
pub enum RaftMessage<LogEntry> {
    AddPeer(ActorRef<RaftMessage<LogEntry>>),

    AppendEntries(AppendEntriesRPC<LogEntry>),

    AppendEntryResponse(u64, bool), // term, success

    // the candidate that initiared the vote, stuff about the vote
    RequestVote(RequestVoteRPC<LogEntry>),

    // true if the vote was granted, false otherwise
    RequestVoteResponse(bool),
}

#[derive(Debug)]
enum RaftState {
    Follower,
    Candidate,
    Leader,
}

struct RequestVoteRPC<LogEntry> {
    term: u64,
    candidate_ref: ActorRef<RaftMessage<LogEntry>>,
    last_log_index: usize,
    last_log_term: u64,
}

struct AppendEntriesRPC<LogEntry> {
    term: u64,                         // leader's term
    leader_ref: ActorRef<RaftMessage<LogEntry>>, // the leader's address, followers should store it to redirect clients that talk to them
    prev_log_index: u64,               // the index of the log entry immediately preceding the new ones
    prev_log_term: u64,                // the term of the entry at prev_log_index
    entries: Vec<LogEntry>,             // stuff to add, empty for heartbeat
    leader_commit: u64,                // the leader's commit index
}

// data common to all states, used to avoid passing a million parameters to the state functions
struct CommonData<LogEntry> {
    current_term: u64,
    log: Vec<LogEntry>,
    commit_index: usize,
    last_applied: usize,
    voted_for: Option<ActorRef<RaftMessage<LogEntry>>>,
}

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

    let mut state: RaftState = RaftState::Follower;

    let mut peer_refs = init(&mut cell, total_nodes).await;

    tracing::trace!("initialization done");

    // this should iterate not less than once per term
    loop {
        state = match state {
            RaftState::Follower => follower(&mut cell, &mut common_data).await,
            RaftState::Candidate => candidate(&mut cell, &me, &mut common_data, &mut peer_refs).await,
            RaftState::Leader => leader(&mut cell, &mut common_data, &mut peer_refs, &me).await,
        };
    }

    #[allow(unreachable_code)]
    cell
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
            Some(raftmessage) => match raftmessage {
                RaftMessage::AddPeer(peer) => {
                    npeers += 1;
                    peers.push(peer);
                    tracing::trace!("🙆 Peer added, total: {}", npeers);
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
// TODO: store leader address to redirect clients
async fn follower<AB, LogEntry>(cell: &mut AB, common_data: &mut CommonData<LogEntry>) -> RaftState
where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + 'static,
{
    tracing::info!("👂 State is follower");

    let min_election_timeout_ms = 1500;
    let max_election_timeout_ms = 3000;

    let election_timeout = Duration::from_millis(
        random::<u64>() % (max_election_timeout_ms - min_election_timeout_ms) + min_election_timeout_ms,
    );

    loop {
        let wait_res = timeout(election_timeout, cell.recv()).await;

        let received_message = if let Ok(message) = wait_res {
            message
        } else {
            tracing::info!("⏰ Timeout reached, starting an election");
            return RaftState::Candidate;
        };

        let raftmessage = if let Some(raftmessage) = received_message.message() {
            raftmessage
        } else {
            tracing::info!("Received a None message, quitting");
            panic!("Received a None message");
        };

        match raftmessage {
            RaftMessage::AppendEntries(mut append_entries_rpc) => {
                tracing::trace!("✏️ Received an AppendEntries message, adding them to the log");
                common_data.log.append(&mut append_entries_rpc.entries);
            }
            RaftMessage::RequestVote(mut request_vote_rpc) => {
                // check if the candidate log is at least as up-to-date as our log
                // if it is and we haven't voted for anyone yet, vote for the candidate
                if request_vote_rpc.term >= common_data.current_term
                    && (common_data.voted_for.is_none()
                        || *common_data.voted_for.as_ref().unwrap() == request_vote_rpc.candidate_ref)
                {
                    let _ = request_vote_rpc
                        .candidate_ref
                        .try_send(RaftMessage::RequestVoteResponse(true));
                    common_data.voted_for = Some(request_vote_rpc.candidate_ref);
                } else {
                    let _ = request_vote_rpc
                        .candidate_ref
                        .try_send(RaftMessage::RequestVoteResponse(false));
                }
            }
            _ => {}
        }
    }
}

async fn candidate<AB, LogEntry>(
    cell: &mut AB,
    me: &ActorRef<RaftMessage<LogEntry>>,
    common_data: &mut CommonData<LogEntry>,
    peer_refs: &mut Vec<ActorRef<RaftMessage<LogEntry>>>,
) -> RaftState
where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + 'static,
{
    tracing::info!("🤵 State is candidate");
    let min_timeout_ms = 150;
    let max_timeout_ms = 300;

    let mut votes = 1;
    let new_term = common_data.current_term + 1;

    for peer in peer_refs.iter_mut() {
        let _ = peer.try_send(RaftMessage::RequestVote(RequestVoteRPC {
            term: new_term,
            candidate_ref: me.clone(),
            last_log_index: common_data.last_applied,
            last_log_term: 0,
        }));
    }

    let mut time_left = Duration::from_millis(random::<u64>() % (max_timeout_ms - min_timeout_ms) + min_timeout_ms);

    loop {
        let start_wait_time = Instant::now();
        let wait_res = timeout(time_left, cell.recv()).await;

        let received_message = if let Ok(message) = wait_res {
            message
        } else {
            tracing::info!("⏰ Timeout reached, starting an election");
            return RaftState::Candidate;
        };

        let raftmessage = if let Some(raftmessage) = received_message.message() {
            raftmessage
        } else {
            tracing::info!("Received a None message, quitting");
            panic!("Received a None message");
        };

        match raftmessage {
            RaftMessage::RequestVoteResponse(vote_granted) => {
                if vote_granted {
                    tracing::trace!("Got a vote");
                    votes += 1;
                    if votes > peer_refs.len() / 2 + 1 {
                        return RaftState::Leader;
                    }
                }
            }
            RaftMessage::AppendEntries(append_entry_rpc) => {
                if append_entry_rpc.term >= common_data.current_term {
                    tracing::info!("There is another leader with an higher term, going back to follower");
                    return RaftState::Follower;
                }
            }
            _ => {}
        }

        match time_left.checked_sub(start_wait_time.elapsed()) {
            Some(time) => time_left = time,
            None => return RaftState::Candidate,
        }
    }

}

// the leader is the interface of the system to the external world
// clients send messages to the leader, which is responsible for replicating them to the other nodes
// after receiving confirmation from the majority of the nodes, the leader commits the message as agreed
async fn leader<AB, LogEntry>(
    cell: &mut AB,
    common_data: &mut CommonData<LogEntry>,
    peer_refs: &mut Vec<ActorRef<RaftMessage<LogEntry>>>,
    me: &ActorRef<RaftMessage<LogEntry>>,
) -> RaftState
where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + Clone + 'static,
{
    tracing::info!("👑 State is leader");
    // send heartbeat to everyone to make other candidates switch back to followers
    let empty_msg = Vec::new();
    for peer in peer_refs.iter_mut() {
        let _ = peer.try_send(RaftMessage::AppendEntries(AppendEntriesRPC {
            term: common_data.current_term,
            leader_ref: me.clone(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: empty_msg.clone(),
            leader_commit: 0,
        }));
    }

    let msg: Vec<LogEntry> = Vec::new();

    // remove when leader will listen to clients instead of sending random stuff
    let interval_between_messages = Duration::from_millis(1000);
    loop {
        // send random stuff to all peers
        // TODO: listen on something and replay instead of the random stuff

        // wait for the interval to pass before sending another message
        // pretend checking the queue took no time since this is temporary anyway
        tokio::time::sleep(interval_between_messages).await;

        for peer in peer_refs.iter_mut() {
            let _ = peer.try_send(RaftMessage::AppendEntries(AppendEntriesRPC {
                term: common_data.current_term,
                leader_ref: me.clone(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: msg.clone(),
                leader_commit: 0,
            }));
        }

        // check message queue for responses, don't wait too long since we're not responsive from the pov of the client
        // TODO: maybe figure out how to listen for messages from the client and the other raft nodes in parallel
        // or see if we can only check whether there is a message in the queue or not
        let wait_res = timeout(Duration::from_millis(10), cell.recv()).await;

        let received_message = if let Ok(message) = wait_res {
            message
        } else {
            continue;
        };

        let raftmessage = if let Some(raftmessage) = received_message.message() {
            raftmessage
        } else {
            tracing::info!("Received a None message, quitting");
            panic!("Received a None message");
        };

        match raftmessage {
            // TODO: condense AppendEntries and RequestVote cases in a function
            RaftMessage::AppendEntries(mut append_entries_rpc) => {
                tracing::trace!("Received an AppendEntries message as the leader, somone dared challenge me, are they right?");
                if append_entries_rpc.term >= common_data.current_term {
                    tracing::trace!("They are right, I'm stepping down");
                    let _ = append_entries_rpc
                        .leader_ref
                        .try_send(RaftMessage::RequestVoteResponse(true));
                    common_data.voted_for = Some(append_entries_rpc.leader_ref);
                    break;
                } else {
                    tracing::trace!("They are wrong, long live the king!");
                    let _ = append_entries_rpc
                        .leader_ref
                        .try_send(RaftMessage::RequestVoteResponse(false));
                }
            }
            RaftMessage::RequestVote(mut request_vote_rpc) => {
                tracing::trace!("Received a request vote message as the leader, somone dared challenge me, are they right?");
                if request_vote_rpc.term >= common_data.current_term {
                    tracing::trace!("They are right, I'm stepping down");
                    let _ = request_vote_rpc
                        .candidate_ref
                        .try_send(RaftMessage::RequestVoteResponse(true));
                    common_data.voted_for = Some(request_vote_rpc.candidate_ref);
                    break;
                } else {
                    tracing::trace!("They are wrong, long live the king!");
                    let _ = request_vote_rpc
                        .candidate_ref
                        .try_send(RaftMessage::RequestVoteResponse(false));
                }
            }
            RaftMessage::AppendEntryResponse(_term, _success) => {
                tracing::trace!("Received an AppendEntryResponse message");
            }
            _ => {}
        }
    }

    return RaftState::Follower;
}
