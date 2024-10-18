pub trait StateMachine<LogEntry, StateMachineResult> {
    fn apply(&mut self, entry: &LogEntry) -> StateMachineResult;
}