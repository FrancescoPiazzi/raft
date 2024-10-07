use std::{collections::BTreeMap, fmt::{Debug, Formatter, Result}};

use tokio::sync::mpsc;

use crate::types::AppendEntriesClientResponse;

pub struct CommonState<LogEntry> {
    pub current_term: u64,
    pub log: Vec<(LogEntry, u64)>,
    pub commit_index: usize,
    pub last_applied: usize,
    pub voted_for: Option<u32>,
}

impl<LogEntry> CommonState<LogEntry> {
    pub const fn new() -> Self {
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
    // TOASK: I don't thing there 2 mut are needed
    pub fn commit(&mut self, mut client_per_entry_group: Option<&mut BTreeMap<usize, mpsc::Sender<AppendEntriesClientResponse<LogEntry>>>>) {
        while self.last_applied < self.commit_index {
            tracing::info!("Applying log entry {}", self.last_applied);
            self.last_applied += 1;

            if let Some(map) = &mut client_per_entry_group {
                if let Some(sender) = map.remove(&self.last_applied) {
                    let _ = sender.try_send(AppendEntriesClientResponse::Ok(()));
                }
            }
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