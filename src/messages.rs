use actum::prelude::ActorRef;
use std::fmt::{self, Debug, Formatter};

#[derive(Clone)]
pub enum RaftMessage<LogEntry> {
    AddPeer(ActorRef<RaftMessage<LogEntry>>, String),

    AppendEntries(AppendEntriesRPC<LogEntry>),
    AppendEntryResponse(String, u64, bool), // term, success

    RequestVote(RequestVoteRPC<LogEntry>),
    RequestVoteResponse(bool), // true if the vote was granted, false otherwise

    AppendEntriesClient(AppendEntriesClientRPC<LogEntry>),
    AppendEntriesClientResponse(Result<(), Option<ActorRef<RaftMessage<LogEntry>>>>),

    InitMessage(Vec<LogEntry>), // used only by the simulator to initialize the message the client will replay forever
}

#[derive(Clone)]
pub struct RequestVoteRPC<LogEntry> {
    pub term: u64,
    pub candidate_ref: ActorRef<RaftMessage<LogEntry>>,
    pub last_log_index: usize,
    pub last_log_term: u64,
}

#[derive(Clone)]
pub struct AppendEntriesRPC<LogEntry> {
    pub term: u64,                                   // leader's term
    pub leader_ref: ActorRef<RaftMessage<LogEntry>>, // the leader's address, followers should store it to redirect clients that talk to them
    pub prev_log_index: u64, // the index of the log entry immediately preceding the new ones
    pub prev_log_term: u64,  // the term of the entry at prev_log_index
    pub entries: Vec<LogEntry>, // stuff to add, empty for heartbeat
    pub leader_commit: u64,  // the leader's commit index
}

#[derive(Clone)]
pub struct AppendEntriesClientRPC<LogEntry> {
    pub client_ref: ActorRef<RaftMessage<LogEntry>>,
    pub entries: Vec<LogEntry>,
}

impl<LogEntry> Debug for RaftMessage<LogEntry> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::AddPeer(_, _) => "Add peer".fmt(f),
            Self::AppendEntries(_) => "Append entries".fmt(f),
            Self::AppendEntryResponse(_, _, _) => "Append entry response".fmt(f),
            Self::RequestVote(_) => "Request vote".fmt(f),
            Self::RequestVoteResponse(_) => "Request vote response".fmt(f),
            Self::AppendEntriesClient(_) => "Append entries client".fmt(f),
            Self::AppendEntriesClientResponse(_) => "Append entries client response".fmt(f),
            Self::InitMessage(_) => "Init message".fmt(f),
        }
    }
}
