use std::fmt::Debug;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RequestVoteRequest {
    pub term: u64,
    pub candidate_id: u32,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Eq, PartialEq)]
pub struct RequestVoteReply {
    pub from: u32,
    pub term: u64,
    pub vote_granted: bool,
}
