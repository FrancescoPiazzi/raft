use actum::prelude::ActorRef;

use crate::messages::RaftMessage;

pub type AppendEntriesClientResponse<SMin> = Result<(), Option<ActorRef<RaftMessage<SMin>>>>;
