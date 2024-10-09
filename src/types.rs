use crate::messages::RaftMessage;

use actum::prelude::ActorRef;

pub type AppendEntriesClientResponse<LogEntry> = Result<(), Option<ActorRef<RaftMessage<LogEntry>>>>;
