use std::iter::repeat;
use std::ops::{Index, IndexMut, RangeFrom};

const LOG_INDEX_STARTS_AT_1: &str = "Log index starts at 1";

/// 1-indexed log to store log entries along with the term they were added in
pub struct Log<LogEntry> {
    log: Vec<LogEntry>,
    terms: Vec<u64>,
}

impl<LogEntry> Index<usize> for Log<LogEntry> {
    type Output = LogEntry;

    fn index(&self, index: usize) -> &Self::Output {
        if index == 0 {
            panic!("{}", LOG_INDEX_STARTS_AT_1);
        }
        &self.log[index - 1]
    }
}

impl<LogEntry> Index<RangeFrom<usize>> for Log<LogEntry>
where
    LogEntry: Clone,
{
    type Output = [LogEntry];

    fn index(&self, index: RangeFrom<usize>) -> &Self::Output {
        if index.start == 0 {
            &[]
        } else {
            &self.log[index.start - 1..]
        }
    }
}

impl<LogEntry> IndexMut<usize> for Log<LogEntry> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        if index == 0 {
            panic!("{}", LOG_INDEX_STARTS_AT_1);
        }
        &mut self.log[index - 1]
    }
}

impl<LogEntry> Log<LogEntry> {
    pub const fn new() -> Self {
        Self {
            log: Vec::new(),
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
        if index == 0 {
            panic!("{}", LOG_INDEX_STARTS_AT_1);
        }
        self.terms[index - 1]
    }

    pub fn append(&mut self, entries: Vec<LogEntry>, term: u64) {
        self.insert(entries, self.len() as u64, term);
    }

    pub fn insert(&mut self, mut entries: Vec<LogEntry>, prev_log_index: u64, term: u64) {
        assert!(prev_log_index as usize <= self.log.len(), "Raft logs cannot have holes");

        let insert_index = prev_log_index as usize;
        let n_new_entries = entries.len();

        // Remove entries that will be overwritten
        self.log.truncate(insert_index);
        self.terms.truncate(insert_index);

        // Insert new entries
        self.log.append(&mut entries);
        self.terms.extend(repeat(term).take(n_new_entries));

        assert!(
            self.log.len() == self.terms.len(),
            "Log and term vectors must have the same length"
        );
    }
}

impl<LogEntry> Default for Log<LogEntry> {
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
    #[should_panic(expected = "Log index starts at 1")]
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
    #[should_panic(expected = "Log index starts at 1")]
    fn test_log_indexing_mut_0() {
        let mut log = Log::<u32>::new();
        log[0] = 1;
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
