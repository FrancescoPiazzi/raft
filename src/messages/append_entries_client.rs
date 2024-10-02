use std::fmt::{Debug, Formatter};

pub struct AppendEntriesClientRequest<LogEntry> {
    pub entries_to_replicate: Vec<LogEntry>,
}

impl<LogEntry> Debug for AppendEntriesClientRequest<LogEntry> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppendEntriesClientRequest").finish_non_exhaustive()
    }
}
