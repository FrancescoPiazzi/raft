use crate::common_state::CommonState;

pub fn update_term<SMin, SMout, SM>(common_state: &mut CommonState<SMin, SMout, SM>, term: u64) -> bool {
    if term > common_state.current_term {
        tracing::trace!("previous term = {}, new term = {}",
            common_state.current_term, term);

        common_state.current_term = term;
        common_state.voted_for = None;
        true
    } else {
        false
    }
}
