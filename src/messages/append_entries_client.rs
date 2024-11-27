use std::fmt::{Debug, Formatter};

use tokio::sync::oneshot;

use crate::types::AppendEntriesClientResponse;

pub struct AppendEntriesClientRequest<SMin, SMout> {
    pub reply_to: oneshot::Sender<AppendEntriesClientResponse<SMin, SMout>>,
    pub entries_to_replicate: Vec<SMin>,
}

impl<SMin, SMout> Debug for AppendEntriesClientRequest<SMin, SMout> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppendEntriesClientRequest").finish_non_exhaustive()
    }
}
