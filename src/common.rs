use crate::common_state::CommonState;
use crate::messages::TermProvider;

/// Updates current term if necessary, returns true if we have to become a follower, false otherwise
pub fn update_term<TP, SM, SMin, SMout>(common_state: &mut CommonState<SM, SMin, SMout>, msg: &TP) -> bool
where 
TP: TermProvider
{
    if let Some(inner) = msg.get_term(){
        if inner > common_state.current_term {
            common_state.current_term = inner;
            return true;
        } else {
            return false;
        }
    }
    false
}