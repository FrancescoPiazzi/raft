use std::{fmt::{Debug, Formatter, Result}, marker::PhantomData};

use crate::{log::Log, state_machine};
use crate::state_machine::StateMachine;

pub struct CommonState<LogEntry, SM, StateMachineResult> 
{
    pub current_term: u64,
    pub log: Log<LogEntry>,
    pub state_machine: SM,
    pub commit_index: usize,
    pub last_applied: usize,
    pub voted_for: Option<u32>,
    _phantom: PhantomData<StateMachineResult>,
}

impl<LogEntry, SM, StateMachineResult> CommonState<LogEntry, SM, StateMachineResult> 
where SM: StateMachine<LogEntry, StateMachineResult>,
{
    pub const fn new(state_machine: SM) -> Self {
        Self {
            current_term: 0,
            log: Log::new(),
            state_machine: state_machine,
            commit_index: 0,
            last_applied: 0,
            voted_for: None,
            _phantom: PhantomData,
        }
    }

    /// commit log entries up to the leader's commit index
    /// the entire common_data object is taken even if for now only the commit_index and last_applied are used
    /// because in the future I will want to access the log entries to actually apply them
    pub fn commit(&mut self, newly_committed_entries: &mut Option<Vec<usize>>) {
        for i in (self.last_applied + 1)..=self.commit_index {
            tracing::info!("Applying log entry {}", i);
            self.state_machine.apply(&self.log[i]);
            if let Some(inner) = newly_committed_entries {
                inner.push(i);
            }
        }
        self.last_applied = self.commit_index;
    }
}

impl<LogEntry, SM, StateMachineResult> Debug for CommonState<LogEntry, SM, StateMachineResult> {
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
