pub trait StateMachine<SMin, SMout> {
    #[must_use]
    fn apply(&mut self, entry: &SMin) -> SMout;
}

// #[cfg(test)]
#[derive(Clone)]
pub(crate) struct VoidStateMachine;

// #[cfg(test)]
impl StateMachine<(), ()> for VoidStateMachine {
    fn apply(&mut self, _entry: &()) -> () {
        ()
    }
}

// #[cfg(test)]
impl VoidStateMachine {
    pub fn new() -> Self {
        Self
    }
}