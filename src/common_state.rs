use std::fmt::{Debug, Formatter, Result};

pub struct CommonState<LogEntry> {
    pub current_term: u64,
    pub log: Vec<(LogEntry, u64)>,
    pub commit_index: usize,
    pub last_applied: usize,
    pub voted_for: Option<u32>,
}

impl<LogEntry> CommonState<LogEntry> {
    pub fn new() -> Self {
        Self {
            current_term: 0,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            voted_for: None,
        }
    }

    // commit log entries up to the leader's commit index
    // the entire common_data object is taken even if for now only the commit_index and last_applied are used
    // because in the future I will want to access the log entries to actually apply them
    pub fn commit(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
        }
    }
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
