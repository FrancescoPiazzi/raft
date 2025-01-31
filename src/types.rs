use actum::prelude::ActorRef;
use actum::drop_guard::ActorDropGuard;
use tokio::task::JoinHandle;

use crate::messages::RaftMessage;

pub struct AppendEntriesClientResponse<SMin, SMout>(pub Result<SMout, Option<ActorRef<RaftMessage<SMin, SMout>>>>);

impl<SMin, SMout> std::ops::Deref for AppendEntriesClientResponse<SMin, SMout> {
    type Target = Result<SMout, Option<ActorRef<RaftMessage<SMin, SMout>>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub type SplitServers<SM, SMin, SMout> = (
    Vec<ActorRef<RaftMessage<SMin, SMout>>>, 
    Vec<u32>, 
    Vec<JoinHandle<SM>>, 
    Vec<ActorDropGuard>
);