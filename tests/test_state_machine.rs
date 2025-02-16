use std::collections::HashSet as Set;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;

use oxidized_float::state_machine::StateMachine;

#[derive(Clone)]
pub struct TestStateMachine {
    pub set: Set<u64>,
}

impl TestStateMachine {
    pub fn new() -> Self {
        TestStateMachine { set: Set::new() }
    }
}

impl StateMachine<u64, usize> for TestStateMachine {
    fn apply(&mut self, entry: &u64) -> usize {
        self.set.insert(*entry);
        self.set.len()
    }
}

impl PartialEq for TestStateMachine {
    fn eq(&self, other: &Self) -> bool {
        self.set == other.set
    }
}

impl Debug for TestStateMachine {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.set)
    }
}