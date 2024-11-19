use std::fmt::{Debug, Formatter};

pub struct AppendEntriesRequest<SMin> {
    pub term: u64,
    pub leader_id: u32,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<SMin>,
    pub leader_commit: u64,
}

impl<SMin> Debug for AppendEntriesRequest<SMin> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.entries.is_empty() {
            f.debug_struct("AppendEntriesRequest ♥")
        } else {
            f.debug_struct("AppendEntriesRequest")
        }
        .field("term", &self.term)
        .field("leader_id", &self.leader_id)
        .field("prev_log_index", &self.prev_log_index)
        .field("prev_log_term", &self.prev_log_term)
        .field("leader_commit", &self.leader_commit)
        .finish_non_exhaustive()
    }
}

impl<SMin> AppendEntriesRequest<SMin> {
    pub fn is_heartbeat(&self) -> bool {
        self.entries.is_empty()
    }
}

#[derive(Debug)]
pub struct AppendEntriesReply {
    pub from: u32,
    pub term: u64,
    pub success: bool,
}
