use crate::messages::RaftMessage;
use actum::actor_ref::ActorRef;
use std::fmt::{Debug, Formatter, Result};

#[derive(Clone)]
pub struct CommonState<LogEntry> {
    pub current_term: u64,
    pub log: Vec<(LogEntry, u64)>,
    pub commit_index: usize,
    pub last_applied: usize,
    pub voted_for: Option<ActorRef<RaftMessage<LogEntry>>>,
}

impl<LogEntry> Debug for CommonState<LogEntry> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
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
