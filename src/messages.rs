pub mod add_peer;
pub mod append_entries;
pub mod append_entries_client;
pub mod request_vote;

use std::fmt::{self, Debug, Formatter};

use crate::messages::add_peer::AddPeer;
use crate::messages::append_entries::{AppendEntriesReply, AppendEntriesRequest};
use crate::messages::append_entries_client::AppendEntriesClientRequest;
use crate::messages::request_vote::{RequestVoteReply, RequestVoteRequest};

/// Every message exchanged that strictly belongs to the Raft algorithm has a term
/// this trait is used to get the term of a message I don't know the type of
/// the optional wrapper allows for types that aren't exactly part of the algorithm
/// like AddPeer and AppendEntriesClientRequest to return cleanly
pub(crate) trait TermProvider{
    fn get_term(&self) -> Option<u64>;
}

pub enum RaftMessage<SMin> {
    AddPeer(AddPeer<SMin>),
    AppendEntriesRequest(AppendEntriesRequest<SMin>),
    AppendEntriesReply(AppendEntriesReply),
    RequestVoteRequest(RequestVoteRequest),
    RequestVoteReply(RequestVoteReply),
    AppendEntriesClientRequest(AppendEntriesClientRequest<SMin>),
}

impl<SMin> Debug for RaftMessage<SMin> {
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

impl<SMin> From<AddPeer<SMin>> for RaftMessage<SMin> {
    fn from(value: AddPeer<SMin>) -> Self {
        Self::AddPeer(value)
    }
}

impl<SMin> From<AppendEntriesRequest<SMin>> for RaftMessage<SMin> {
    fn from(value: AppendEntriesRequest<SMin>) -> Self {
        Self::AppendEntriesRequest(value)
    }
}

impl<SMin> From<AppendEntriesReply> for RaftMessage<SMin> {
    fn from(value: AppendEntriesReply) -> Self {
        Self::AppendEntriesReply(value)
    }
}

impl<SMin> From<RequestVoteRequest> for RaftMessage<SMin> {
    fn from(value: RequestVoteRequest) -> Self {
        Self::RequestVoteRequest(value)
    }
}

impl<SMin> From<RequestVoteReply> for RaftMessage<SMin> {
    fn from(value: RequestVoteReply) -> Self {
        Self::RequestVoteReply(value)
    }
}

impl<SMin> From<AppendEntriesClientRequest<SMin>> for RaftMessage<SMin> {
    fn from(value: AppendEntriesClientRequest<SMin>) -> Self {
        Self::AppendEntriesClientRequest(value)
    }
}

impl<SMin> TermProvider for RaftMessage<SMin> {
    fn get_term(&self) -> Option<u64> {
        match &self {
            RaftMessage::AddPeer(_) => {None}
            RaftMessage::AppendEntriesClientRequest(_) => None,
            RaftMessage::AppendEntriesReply(msg) => Some(msg.term),
            RaftMessage::AppendEntriesRequest(msg) => Some(msg.term),
            RaftMessage::RequestVoteRequest(msg) => Some(msg.term),
            RaftMessage::RequestVoteReply(msg) => Some(msg.term),
        }
    }
}