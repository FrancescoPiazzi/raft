use actum::prelude::ActorRef;
use std::fmt::{Debug, Formatter};
use tokio::sync::oneshot;

use super::RaftMessage;

pub struct AppendEntriesClientRequest<LogEntry> {
    pub reply_to: oneshot::Sender<Result<(), Option<ActorRef<RaftMessage<LogEntry>>>>>,
    pub entries_to_replicate: Vec<LogEntry>,
}

impl<LogEntry> Debug for AppendEntriesClientRequest<LogEntry> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppendEntriesClientRequest").finish_non_exhaustive()
    }
}
