pub mod add_peer;
pub mod append_entries;
pub mod append_entries_client;
pub mod request_vote;

use std::fmt::{self, Debug, Formatter};

use crate::messages::add_peer::AddPeer;
use crate::messages::append_entries::{AppendEntriesReply, AppendEntriesRequest};
use crate::messages::append_entries_client::AppendEntriesClientRequest;
use crate::messages::request_vote::{RequestVoteReply, RequestVoteRequest};

pub enum RaftMessage<SMin, SMout> {
    AddPeer(AddPeer<SMin, SMout>),
    AppendEntriesRequest(AppendEntriesRequest<SMin>),
    AppendEntriesReply(AppendEntriesReply),
    RequestVoteRequest(RequestVoteRequest),
    RequestVoteReply(RequestVoteReply),
    AppendEntriesClientRequest(AppendEntriesClientRequest<SMin, SMout>),
}

impl<SMin, SMout> RaftMessage<SMin, SMout> {
    pub fn get_term(&self) -> Option<u64> {
        match self {
            Self::AddPeer(_) => None,
            Self::AppendEntriesClientRequest(_) => None,
            Self::AppendEntriesRequest(inner) => Some(inner.term),
            Self::AppendEntriesReply(inner) => Some(inner.term),
            Self::RequestVoteRequest(inner) => Some(inner.term),
            Self::RequestVoteReply(inner) => Some(inner.term),
        }
    }
}

impl<SMin, SMout> Debug for RaftMessage<SMin, SMout> {
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

impl<SMin, SMout> From<AddPeer<SMin, SMout>> for RaftMessage<SMin, SMout> {
    fn from(value: AddPeer<SMin, SMout>) -> Self {
        Self::AddPeer(value)
    }
}

impl<SMin, SMout> From<AppendEntriesRequest<SMin>> for RaftMessage<SMin, SMout> {
    fn from(value: AppendEntriesRequest<SMin>) -> Self {
        Self::AppendEntriesRequest(value)
    }
}

impl<SMin, SMout> From<AppendEntriesReply> for RaftMessage<SMin, SMout> {
    fn from(value: AppendEntriesReply) -> Self {
        Self::AppendEntriesReply(value)
    }
}

impl<SMin, SMout> From<RequestVoteRequest> for RaftMessage<SMin, SMout> {
    fn from(value: RequestVoteRequest) -> Self {
        Self::RequestVoteRequest(value)
    }
}

impl<SMin, SMout> From<RequestVoteReply> for RaftMessage<SMin, SMout> {
    fn from(value: RequestVoteReply) -> Self {
        Self::RequestVoteReply(value)
    }
}

impl<SMin, SMout> From<AppendEntriesClientRequest<SMin, SMout>> for RaftMessage<SMin, SMout> {
    fn from(value: AppendEntriesClientRequest<SMin, SMout>) -> Self {
        Self::AppendEntriesClientRequest(value)
    }
}
