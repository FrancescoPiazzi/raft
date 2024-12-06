pub trait StateMachine<SMin, SMout> {
    #[must_use]
    fn apply(&mut self, entry: &SMin) -> SMout;
}
