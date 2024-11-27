use actum::prelude::ActorRef;

use crate::messages::RaftMessage;

pub struct AppendEntriesClientResponse<SMin>(pub Result<(), Option<ActorRef<RaftMessage<SMin>>>>);

impl<SMin> std::ops::Deref for AppendEntriesClientResponse<SMin> {
    type Target = Result<(), Option<ActorRef<RaftMessage<SMin>>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
