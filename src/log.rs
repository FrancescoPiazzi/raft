use std::fmt::{Debug, Formatter};
use std::iter::repeat;
use std::ops::{Index, IndexMut, RangeFrom};

use crate::messages::request_vote::RequestVoteRequest;

fn check_log_index(index: usize) {
    if index == 0 {
        panic!("attempted to access the log at index 0");
    }
}

/// 1-indexed log to store log entries and their term.
///
/// # Panic
///
/// Panics at the attempt to access the log at index 0.
pub struct Log<SMin> {
    log: Vec<SMin>,
    terms: Vec<u64>,
}

impl<SMin> Debug for Log<SMin> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Log")
            .field("length", &self.log.len())
            .field("terms", &self.terms)
            .finish()
    }
}

impl<SMin> Index<usize> for Log<SMin> {
    type Output = SMin;

    fn index(&self, index: usize) -> &Self::Output {
        check_log_index(index);
        &self.log[index - 1]
    }
}

impl<SMin> Index<RangeFrom<usize>> for Log<SMin>
where
    SMin: Clone,
{
    type Output = [SMin];

    fn index(&self, index: RangeFrom<usize>) -> &Self::Output {
        if index.start == 0 {
            &[]
        } else {
            &self.log[index.start - 1..]
        }
    }
}

impl<SMin> IndexMut<usize> for Log<SMin> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        check_log_index(index);
        &mut self.log[index - 1]
    }
}

impl<SMin> Log<SMin> {
    pub const fn new() -> Self {
        Self {
            log: Vec::new(),    // TLA: 153
            terms: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.log.len()
    }

    pub fn is_empty(&self) -> bool {
        self.log.is_empty()
    }

    pub fn get_term(&self, index: usize) -> u64 {
        check_log_index(index);
        self.terms[index - 1]
    }

    pub fn get_last_log_term(&self) -> u64 {
        self.terms.last().copied().unwrap_or(0)
    }

    pub fn append(&mut self, entries: Vec<SMin>, term: u64) {
        self.insert(entries, self.len() as u64, term);
    }

    // https://github.com/logcabin/logcabin/blob/ee6c55ae9744b82b451becd9707d26c7c1b6bbfb/Server/RaftConsensus.cc#L1536
    pub fn is_log_ok(&self, request: &RequestVoteRequest) -> bool {
        request.last_log_term > self.get_last_log_term()
            || (request.last_log_term == self.get_last_log_term() && request.last_log_index >= self.len() as u64)
    }

    #[tracing::instrument(level = "trace", skip(entries))]
    pub fn insert(&mut self, mut entries: Vec<SMin>, prev_log_index: u64, term: u64) {
        assert!(prev_log_index as usize <= self.log.len(), "Raft logs cannot have holes");

        let insert_index = prev_log_index as usize;
        let n_new_entries = entries.len();

        // Remove entries that will be overwritten
        self.log.truncate(insert_index);
        self.terms.truncate(insert_index);

        // Insert new entries
        self.log.append(&mut entries);
        self.terms.extend(repeat(term).take(n_new_entries));

        assert_eq!(
            self.log.len(),
            self.terms.len(),
            "Log and term vectors must have the same length"
        );
    }
}

impl<SMin> Default for Log<SMin> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_indexing() {
        let mut log = Log::<u32>::new();
        log.append(vec![1, 2, 3], 1);

        assert_eq!(log[1], 1);
        assert_eq!(log[2], 2);
        assert_eq!(log[3], 3);
    }

    #[test]
    #[should_panic(expected = "attempted to access the log at index 0")]
    fn test_log_indexing_0() {
        let log = Log::<u32>::new();
        let _ = log[0];
    }

    #[test]
    fn test_log_indexing_range() {
        let mut log = Log::<u32>::new();
        log.append(vec![1, 2, 3], 1);

        assert_eq!(&log[1..], &[1, 2, 3]);
        assert_eq!(&log[2..], &[2, 3]);
        assert_eq!(&log[3..], &[3]);
    }

    #[test]
    fn test_log_indexing_range_empty() {
        let log = Log::<u32>::new();
        assert_eq!(&log[1..], &[]);
    }

    #[test]
    fn test_log_indexing_mut() {
        let mut log = Log::<u32>::new();
        log.append(vec![1, 2, 3], 1);

        log[1] = 4;
        log[2] = 5;
        log[3] = 6;

        assert_eq!(log[1], 4);
        assert_eq!(log[2], 5);
        assert_eq!(log[3], 6);
    }

    #[test]
    #[should_panic(expected = "attempted to access the log at index 0")]
    fn test_log_indexing_mut_0() {
        let mut log = Log::<u32>::new();
        log[0] = 1;
    }

    #[test]
    fn test_is_log_ok() {
        let mut log = Log::<u32>::new();
        log.append(vec![1, 2, 3], 1);

        let request = RequestVoteRequest {
            term: 1,
            candidate_id: 1,
            last_log_index: 3,
            last_log_term: 1,
        };
        assert!(log.is_log_ok(&request));

        // log of requester is shorter than ours
        let request = RequestVoteRequest {
            term: 1,
            candidate_id: 1,
            last_log_index: 2,
            last_log_term: 1,
        };
        assert!(!log.is_log_ok(&request));

        // log of requester is shorter than ours, but out term is higher
        let request = RequestVoteRequest {
            term: 1,
            candidate_id: 1,
            last_log_index: 1,
            last_log_term: 2,
        };

        assert!(log.is_log_ok(&request));
    }

    #[test]
    fn test_insert_normal() {
        let mut log = Log::<u32>::new();
        log.append(vec![1, 2, 3], 1);
        log.insert(vec![4, 5], 3, 1);

        assert_eq!(&log[1..], &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_insert_overwrite() {
        let mut log = Log::<u32>::new();
        log.append(vec![1, 2, 3], 1);

        log.insert(vec![6, 7], 2, 2);
        assert_eq!(&log[1..], &[1, 2, 6, 7]);

        log.insert(vec![8], 0, 3);
        assert_eq!(&log[1..], &[8]);
    }

    #[test]
    #[should_panic(expected = "Raft logs cannot have holes")]
    fn test_insert_hole() {
        let mut log = Log::<u32>::new();
        log.append(vec![1, 2, 3], 1);
        log.insert(vec![4, 5], 4, 1);
    }
}
