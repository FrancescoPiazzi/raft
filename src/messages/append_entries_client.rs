use std::fmt::{Debug, Formatter};

use tokio::sync::oneshot;

use crate::types::AppendEntriesClientResponse;

pub struct AppendEntriesClientRequest<SMin> {
    pub reply_to: oneshot::Sender<AppendEntriesClientResponse<SMin>>,
    pub entries_to_replicate: Vec<SMin>,
}

impl<SMin> Debug for AppendEntriesClientRequest<SMin> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppendEntriesClientRequest").finish_non_exhaustive()
    }
}
