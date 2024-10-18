use std::fmt::{Debug, Formatter};

use actum::actor_ref::ActorRef;

use crate::messages::RaftMessage;

pub struct AddPeer<SMin> {
    pub peer_id: u32,
    pub peer_ref: ActorRef<RaftMessage<SMin>>,
}

impl<SMin> Debug for AddPeer<SMin> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AddPeer")
            .field("peer_id", &self.peer_id)
            .finish_non_exhaustive()
    }
}
