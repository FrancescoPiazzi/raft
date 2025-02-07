pub mod add_peer;
pub mod append_entries;
pub mod append_entries_client;
pub mod poll_state;
pub mod request_vote;

use std::fmt::{self, Debug, Formatter};

use crate::messages::add_peer::AddPeer;
use crate::messages::append_entries::{AppendEntriesReply, AppendEntriesRequest};
use crate::messages::append_entries_client::AppendEntriesClientRequest;
use crate::messages::request_vote::{RequestVoteReply, RequestVoteRequest};

use crate::messages::poll_state::PollStateRequest;

pub enum RaftMessage<SMin, SMout> {
    AddPeer(AddPeer<SMin, SMout>),
    AppendEntriesRequest(AppendEntriesRequest<SMin>),
    AppendEntriesReply(AppendEntriesReply),
    RequestVoteRequest(RequestVoteRequest),
    RequestVoteReply(RequestVoteReply),
    AppendEntriesClientRequest(AppendEntriesClientRequest<SMin, SMout>),

    PollState(PollStateRequest),
}

impl<SMin, SMout> RaftMessage<SMin, SMout> {
    pub fn unwrap_add_peer(self) -> AddPeer<SMin, SMout> {
        match self {
            RaftMessage::AddPeer(inner) => inner,
            other => panic!("called `RaftMessage::unwrap_add_peer()` on a `{:?}` value", other),
        }
    }

    pub fn unwrap_append_entries_request(self) -> AppendEntriesRequest<SMin> {
        match self {
            RaftMessage::AppendEntriesRequest(inner) => inner,
            other => panic!("called `RaftMessage::unwrap_append_entries_request()` on a `{:?}` value", other),
        }
    }

    pub fn unwrap_append_entries_reply(self) -> AppendEntriesReply {
        match self {
            RaftMessage::AppendEntriesReply(inner) => inner,
            other => panic!("called `RaftMessage::unwrap_append_entries_reply()` on a `{:?}` value", other),
        }
    }

    pub fn unwrap_request_vote_request(self) -> RequestVoteRequest {
        match self {
            RaftMessage::RequestVoteRequest(inner) => inner,
            other => panic!("called `RaftMessage::unwrap_request_vote_request()` on a `{:?}` value", other),
        }
    }

    pub fn unwrap_request_vote_reply(self) -> RequestVoteReply {
        match self {
            RaftMessage::RequestVoteReply(inner) => inner,
            other => panic!("called `RaftMessage::unwrap_request_vote_reply()` on a `{:?}` value", other),
        }
    }

    pub fn unwrap_append_entries_client_request(self) -> AppendEntriesClientRequest<SMin, SMout> {
        match self {
            RaftMessage::AppendEntriesClientRequest(inner) => inner,
            other => panic!("called `RaftMessage::unwrap_append_entries_client_request()` on a `{:?}` value", other),
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

            Self::PollState(_) => "Poll state".fmt(f),
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

// TODO: generics here
impl From<PollStateRequest> for RaftMessage<u64, usize> {
    fn from(value: PollStateRequest) -> Self {
        Self::PollState(value)
    }
}
