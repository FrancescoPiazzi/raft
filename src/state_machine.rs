pub trait StateMachine<SMin, SMout> {
    fn apply(&mut self, entry: &SMin) -> SMout;
}