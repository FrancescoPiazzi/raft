use actum::actor_ref::ActorRef;
use std::fmt::{Debug, Formatter};

pub struct AddPeer<LogEntry> {
    pub peer_id: u32,
    pub peer_ref: ActorRef<LogEntry>,
}

impl<LogEntry> Debug for AddPeer<LogEntry> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AddPeer")
            .field("peer_id", &self.peer_id)
            .finish_non_exhaustive()
    }
}
