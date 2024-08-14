#![allow(dead_code)]    // remove when algorithm is done and all fields should be used

use actum::prelude::ActorRef;

pub enum RaftMessage<LogEntry> {
    AddPeer(ActorRef<RaftMessage<LogEntry>>),

    AppendEntries(AppendEntriesRPC<LogEntry>),
    AppendEntryResponse(u64, bool), // term, success

    RequestVote(RequestVoteRPC<LogEntry>),
    RequestVoteResponse(bool), // true if the vote was granted, false otherwise

    AppendEntriesClient(AppendEntriesClientRPC<LogEntry>),
    AppendEntriesClientResponse(Result<(), Option<ActorRef<RaftMessage<LogEntry>>>>),

    InitMessage(Vec<LogEntry>), // used only by the simulator to initialize the message the client will replay forever
}

pub(crate) struct RequestVoteRPC<LogEntry> {
    pub(crate) term: u64,
    pub(crate) candidate_ref: ActorRef<RaftMessage<LogEntry>>,
    pub(crate) last_log_index: usize,
    pub(crate) last_log_term: u64,
}

pub(crate) struct AppendEntriesRPC<LogEntry> {
    pub(crate) term: u64,                                   // leader's term
    pub(crate) leader_ref: ActorRef<RaftMessage<LogEntry>>, // the leader's address, followers should store it to redirect clients that talk to them
    pub(crate) prev_log_index: u64, // the index of the log entry immediately preceding the new ones
    pub(crate) prev_log_term: u64,  // the term of the entry at prev_log_index
    pub(crate) entries: Vec<LogEntry>, // stuff to add, empty for heartbeat
    pub(crate) leader_commit: u64,  // the leader's commit index
}

// data common to all states, used to avoid passing a million parameters to the state functions
pub(crate) struct CommonData<LogEntry> {
    pub(crate) current_term: u64,
    pub(crate) log: Vec<LogEntry>,
    pub(crate) commit_index: usize,
    pub(crate) last_applied: usize,
    pub(crate) voted_for: Option<ActorRef<RaftMessage<LogEntry>>>,
}

pub(crate) struct AppendEntriesClientRPC<LogEntry> {
    pub(crate) client_ref: ActorRef<RaftMessage<LogEntry>>,
    pub(crate) entries: Vec<LogEntry>,
}
