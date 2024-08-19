use crate::raft::model::RaftMessage;
use actum::actor_ref::ActorRef;
use std::fmt;
use std::fmt::{Debug, Formatter};

// data common to all states, used to avoid passing a million parameters to the state functions
#[derive(Clone)]
pub(crate) struct CommonState<LogEntry> {
    pub(crate) current_term: u64,
    pub(crate) log: Vec<LogEntry>,
    pub(crate) commit_index: usize,
    pub(crate) last_applied: usize,
    pub(crate) voted_for: Option<ActorRef<RaftMessage<LogEntry>>>,
}

impl<LogEntry> Debug for CommonState<LogEntry> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommonState")
            .field("current_term", &self.current_term)
            .field("log length", &self.log.len())
            .field("commit_index", &self.commit_index)
            .field("last_applied", &self.last_applied)
            .field(
                "voted_for",
                if self.voted_for.is_some() {
                    &"Somebody"
                } else {
                    &"Nobody"
                },
            )
            .finish()
    }
}
