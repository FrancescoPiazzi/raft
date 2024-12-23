use std::{
    fmt::{Debug, Formatter, Result},
    marker::PhantomData,
};

use crate::log::Log;
use crate::messages::request_vote::RequestVoteRequest;
use crate::state_machine::StateMachine;

pub struct CommonState<SM, SMin, SMout> {
    pub current_term: u64,
    pub leader_id: Option<u32>,
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
            leader_id: None,
            log: Log::new(),
            state_machine,
            commit_index: 0,
            last_applied: 0,
            voted_for: None,
            _phantom: PhantomData,
        }
    }

    /// Commit the log entries up to the leader's commit index.
    #[tracing::instrument(level = "trace", skip(self), fields(self.last_applied = %self.last_applied, self.commit_index = %self.commit_index))]
    pub fn commit_log_entries_up_to_commit_index(&mut self)
    where
        SM: StateMachine<SMin, SMout> + Send,
    {
        for i in (self.last_applied + 1)..=self.commit_index {
            tracing::trace!("Applying log entry {}", i);
            let _ = self.state_machine.apply(&self.log[i]);
        }

        self.last_applied = self.commit_index;
    }

    /// Commit the log entries up to the leader's commit index.
    ///
    /// Newly commited entries are appended to `newly_committed_entries_buf` buffer.
    /// This is done for optimization purposes, instead of returning a new vector containing the newly commited entries.
    /// Note that the buffer is cleared by the function.
    #[tracing::instrument(level = "trace", skip(self, newly_committed_entries_buf), fields(self.last_applied = %self.last_applied, self.commit_index = %self.commit_index))]
    pub fn commit_log_entries_up_to_commit_index_buf(&mut self, newly_committed_entries_buf: &mut Vec<SMout>)
    where
        SM: StateMachine<SMin, SMout> + Send,
    {
        newly_committed_entries_buf.clear();

        for i in (self.last_applied + 1)..=self.commit_index {
            tracing::trace!("Applying log entry {}", i);
            let state_machine_output = self.state_machine.apply(&self.log[i]);
            newly_committed_entries_buf.push(state_machine_output);
        }

        self.last_applied = self.commit_index;
    }

    /// If the new term is greater than the current term, enters a new term by updating the current term
    /// to the new term and resetting the id of the candidate for which the vote is granted in the new term.
    ///
    /// Returns true if a new term is entered, as an indication to a candidate or leader to revert to follower,
    /// false otherwise.
    #[must_use]
    pub fn update_term(&mut self, new_term: u64) -> bool {
        if new_term > self.current_term {
            self.current_term = new_term;
            self.voted_for = None;
            true
        } else {
            false
        }
    }

    /// Returns true if the server can vote for the candidate with the given id, false otherwise.
    pub fn can_grant_vote(&self, request: &RequestVoteRequest) -> bool {
        self.log.is_log_ok(request)
            && (self.voted_for.is_none() || self.voted_for.is_some_and(|id| id == request.candidate_id))
    }

    pub fn check_validity(&self) {
        if self.commit_index > self.log.len() {
            panic!("commit index is greater than log length");
        }
        if self.last_applied > self.commit_index {
            panic!("last applied is greater than commit index");
        }

        for i in 1..=self.log.len() - 1 {
            if self.log.get_term(i) > self.log.get_term(i + 1) {
                panic!(
                    "log term [{}] = {} is greater than term at [{}] = {}, \
                    log terms should increase monotonically",
                    i,
                    self.log.get_term(i),
                    i + 1,
                    self.log.get_term(i + 1)
                );
            }
        }

        if self.current_term < self.log.get_term(self.commit_index) {
            panic!(
                "current term ({}) is less than term at commit index ({})",
                self.current_term,
                self.log.get_term(self.commit_index)
            );
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_machine::*;

    #[test]
    fn test_commit_log_entries_up_to_commit_index() {
        let mut common_state = CommonState::new(VoidStateMachine);
        common_state.log.append(vec![()], 1);
        common_state.log.append(vec![()], 2);
        common_state.log.append(vec![()], 3);
        common_state.commit_index = 2;

        let mut newly_committed_entries_buf = Vec::new();
        common_state.commit_log_entries_up_to_commit_index_buf(&mut newly_committed_entries_buf);

        assert_eq!(common_state.last_applied, 2);
        assert_eq!(newly_committed_entries_buf, vec![(), ()]);
    }

    #[test]
    fn test_check_validity() {
        let mut common_state = CommonState::<_, (), ()>::new(VoidStateMachine);

        common_state.log.append(vec![()], 1);
        common_state.log.append(vec![()], 2);
        common_state.log.append(vec![()], 3);
        common_state.current_term = 3;
        common_state.commit_index = 2;
        common_state.last_applied = 2;

        common_state.check_validity();
    }

    #[test]
    #[should_panic]
    fn test_check_validity_invalid_commit_index() {
        let mut common_state = CommonState::<_, (), ()>::new(VoidStateMachine);

        common_state.log.append(vec![()], 1);
        common_state.log.append(vec![()], 2);
        common_state.log.append(vec![()], 3);
        common_state.current_term = 3;
        common_state.commit_index = 4;
        common_state.last_applied = 2;

        common_state.check_validity();
    }

    #[test]
    #[should_panic]
    fn test_check_validity_invalid_last_applied() {
        let mut common_state = CommonState::<_, (), ()>::new(VoidStateMachine);

        common_state.log.append(vec![()], 1);
        common_state.log.append(vec![()], 2);
        common_state.log.append(vec![()], 3);
        common_state.current_term = 3;
        common_state.commit_index = 2;
        common_state.last_applied = 3;

        common_state.check_validity();
    }

    #[test]
    #[should_panic]
    fn test_check_validity_invalid_log_terms() {
        let mut common_state = CommonState::<_, (), ()>::new(VoidStateMachine);

        common_state.log.append(vec![()], 1);
        common_state.log.append(vec![()], 2);
        common_state.log.append(vec![()], 1);
        common_state.current_term = 3;
        common_state.commit_index = 2;
        common_state.last_applied = 2;

        common_state.check_validity();
    }
}
