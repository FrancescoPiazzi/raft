use std::fmt::{Debug, Formatter, Result};


pub struct CommonState<LogEntry> {
    pub current_term: u64,
    pub log: Vec<(LogEntry, u64)>,
    pub commit_index: Option<usize>,
    pub last_applied: Option<usize>,
    pub voted_for: Option<u32>,
}

impl<LogEntry> CommonState<LogEntry> {
    pub const fn new() -> Self {
        Self {
            current_term: 0,
            log: Vec::new(),
            commit_index: None,
            last_applied: None,
            voted_for: None,
        }
    }

    // commit log entries up to the leader's commit index
    // the entire common_data object is taken even if for now only the commit_index and last_applied are used
    // because in the future I will want to access the log entries to actually apply them
    pub fn commit(
        &mut self,
        newly_committed_entries: &mut Option<Vec<usize>>
    ) {
        let start = self.last_applied.map_or(0, |index| index + 1);
        let end = match self.commit_index {
            Some(index) => index,
            None => return, // the leader has not committed anything yet
        };

        if start <= end {
            for i in start..=end {
                tracing::info!("Applying log entry {}", i);
                if let Some(inner) = newly_committed_entries {
                    inner.push(i);
                }
            }
            self.last_applied = Some(end);
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

impl<LogEntry> Default for CommonState<LogEntry> {
    fn default() -> Self {
        Self::new()
    }
}
