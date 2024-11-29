use crate::{common_state::CommonState, messages::RaftMessage};

/// Updates current term if necessary, returns true if we have to become a follower, false otherwise
pub fn update_term<SM, SMin, SMout>(
    common_state: &mut CommonState<SM, SMin, SMout>, 
    msg: &RaftMessage<SMin, SMout>
) -> bool 
{
    if let Some(inner) = msg.get_term(){
        if inner > common_state.current_term {
            common_state.new_term(inner);
            return true;
        } else {
            return false;
        }
    }
    false
}