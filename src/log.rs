/// accessible with vector like syntax, the vector is 1-indexed
struct Log<LogEntry> {
    log: Vec<(LogEntry, u64)>,
}

impl Index for Log {
    type Output = LogEntry;

    fn index(&self, index: usize) -> &Self::Output {
        if index == 0 {
            panic!("Log index starts at 1");
        }
        &self.log[index-1]
    }
}

impl IndexMut for Log {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        if index == 0 {
            panic!("Log index starts at 1");
        }
        &mut self.log[index-1]
    }
}

impl Log {
    pub fn new() -> Self {
        Self {
            log: Vec::new(),
        }
    }

    pub fn get_log_entry_at(&self, index: usize) -> Option<&(LogEntry, u64)> {
        self.log[index-1];
    }
}