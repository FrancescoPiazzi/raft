use std::{
    fmt::{Debug, Formatter, Result},
    marker::PhantomData,
};

use crate::log::Log;
use crate::state_machine::StateMachine;

pub struct CommonState<SM, SMin, SMout> {
    pub current_term: u64,
    pub log: Log<SMin>,
    pub state_machine: SM,
    pub commit_index: usize,
    pub last_applied: usize,
    pub voted_for: Option<u32>,
    _phantom: PhantomData<SMout>,
}

impl<SM, SMin, SMout> CommonState<SM, SMin, SMout> {
    pub const fn new(state_machine: SM) -> Self {
        Self {
            current_term: 0,
            log: Log::new(),
            state_machine,
            commit_index: 0,
            last_applied: 0,
            voted_for: None,
            _phantom: PhantomData,
        }
    }

    /// Commit the log entries up to the leader's commit index.
    ///
    /// Newly commited entries are appended in the `newly_committed_entries_out` buffer.
    /// It is responsibility of the caller to clear the buffer beforehand.
    #[tracing::instrument(level = "trace", skip(self, newly_committed_entries_buf), fields(self.last_applied = %self.last_applied, self.commit_index = %self.commit_index))]
    pub fn commit_log_entries_up_to_commit_index(&mut self, newly_committed_entries_buf: &mut Option<Vec<usize>>)
    where
        SM: StateMachine<SMin, SMout> + Send,
    {
        for i in (self.last_applied + 1)..=self.commit_index {
            tracing::info!("Applying log entry {}", i);
            self.state_machine.apply(&self.log[i]);
            if let Some(inner) = newly_committed_entries_buf {
                inner.push(i);
            }
        }
        self.last_applied = self.commit_index;
    }
}

impl<SM, SMin, SMout> Debug for CommonState<SM, SMin, SMout> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("CommonState")
            .field("current_term", &self.current_term)
            .field("log length", &self.log.len())
            .field("commit_index", &self.commit_index)
            .field("last_applied", &self.last_applied)
            .field(
                "voted_for",
                if self.voted_for.is_some() {
                    &"Somebody"
                } else {
                    &"Nobody"
                },
            )
            .finish()
    }
}
