use oxidized_float::state_machine::StateMachine;
use std::collections::HashSet as Set;

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
