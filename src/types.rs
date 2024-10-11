use actum::prelude::ActorRef;

use crate::messages::RaftMessage;

pub type AppendEntriesClientResponse<LogEntry> = Result<(), Option<ActorRef<RaftMessage<LogEntry>>>>;
