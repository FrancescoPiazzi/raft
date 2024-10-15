use std::ops::{Index, IndexMut, RangeFrom};
use std::iter::repeat;

const LOG_INDEX_STARTS_AT_1: &str = "Log index starts at 1";

/// 1-indexed log to store log entries along with the term they were added in
pub struct Log<LogEntry> {
    log: Vec<LogEntry>,
    terms: Vec<u64>,
}

impl<LogEntry> Index<usize> for Log<LogEntry> {
    type Output = LogEntry;

    fn index(&self, index: usize) -> &Self::Output {
        if index <= 0 {
            panic!("{}", LOG_INDEX_STARTS_AT_1);
        }
        &self.log[index - 1]
    }
}

impl<LogEntry> Index<RangeFrom<usize>> for Log<LogEntry> 
where LogEntry: Clone
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
        if index <= 0 {
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

    // TODO: maybe return an Option
    pub fn get_term(&self, index: usize) -> u64 {
        if index <= 0 {
            panic!("{}", LOG_INDEX_STARTS_AT_1);
        }
        self.terms[index - 1]
    }

    // TODO: entries should  not simply be added,
    // they should be compared to the current log and only added if they are not already present
    pub fn append(&mut self, mut entries: Vec<LogEntry>, term: u64) {
        let n_new_entries = entries.len();
        self.log.append(&mut entries);
        self.terms.extend(repeat(term).take(n_new_entries));

        assert!(self.log.len() == self.terms.len());
    }
}