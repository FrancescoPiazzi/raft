pub mod add_peer;
pub mod append_entries;
pub mod append_entries_client;
pub mod request_vote;

use std::fmt::{self, Debug, Formatter};

use crate::messages::add_peer::AddPeer;
use crate::messages::append_entries::{AppendEntriesReply, AppendEntriesRequest};
use crate::messages::append_entries_client::AppendEntriesClientRequest;
use crate::messages::request_vote::{RequestVoteReply, RequestVoteRequest};


pub enum RaftMessage<LogEntry> {
    AddPeer(AddPeer<LogEntry>),
    AppendEntriesRequest(AppendEntriesRequest<LogEntry>),
    AppendEntriesReply(AppendEntriesReply),
    RequestVoteRequest(RequestVoteRequest),
    RequestVoteReply(RequestVoteReply),
    AppendEntriesClientRequest(AppendEntriesClientRequest<LogEntry>),
}

impl<LogEntry> Debug for RaftMessage<LogEntry> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::AddPeer(_) => "Add peer".fmt(f),
            Self::AppendEntriesRequest(_) => "Append entries request".fmt(f),
            Self::AppendEntriesReply(_) => "Append entries reply".fmt(f),
            Self::RequestVoteRequest(_) => "Request vote request".fmt(f),
            Self::RequestVoteReply(_) => "Request vote reply".fmt(f),
            Self::AppendEntriesClientRequest(_) => "Append entries client request".fmt(f),
        }
    }
}

impl<LogEntry> From<AddPeer<LogEntry>> for RaftMessage<LogEntry> {
    fn from(value: AddPeer<LogEntry>) -> Self {
        Self::AddPeer(value)
    }
}

impl<LogEntry> From<AppendEntriesRequest<LogEntry>> for RaftMessage<LogEntry> {
    fn from(value: AppendEntriesRequest<LogEntry>) -> Self {
        Self::AppendEntriesRequest(value)
    }
}

impl<LogEntry> From<AppendEntriesReply> for RaftMessage<LogEntry> {
    fn from(value: AppendEntriesReply) -> Self {
        Self::AppendEntriesReply(value)
    }
}

impl<LogEntry> From<RequestVoteRequest> for RaftMessage<LogEntry> {
    fn from(value: RequestVoteRequest) -> Self {
        Self::RequestVoteRequest(value)
    }
}

impl<LogEntry> From<RequestVoteReply> for RaftMessage<LogEntry> {
    fn from(value: RequestVoteReply) -> Self {
        Self::RequestVoteReply(value)
    }
}

impl<LogEntry> From<AppendEntriesClientRequest<LogEntry>> for RaftMessage<LogEntry> {
    fn from(value: AppendEntriesClientRequest<LogEntry>) -> Self {
        Self::AppendEntriesClientRequest(value)
    }
}
