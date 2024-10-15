use std::collections::VecDeque as Queue;

pub(crate) struct PeerState{
    pub(crate) next_index: u64,
    pub(crate) match_index: u64,

    // Track the length of log entries sent to each follower but not yet acknowledged.
    // This is used to calculate next_index. If an AppendEntriesResponse is lost, the follower
    // will correct us if next_index is too high, or overwrite logs if it's too low.
    // The queue handles multiple messages sent before receiving a response, assuming
    // replies are generally in order. If not, the same correction logic applies.
    pub(crate) messages_len: Queue<usize>
}

impl PeerState{
    pub(crate) fn new(initial_next_index: u64) -> PeerState{
        PeerState{
            next_index: initial_next_index,
            match_index: 0,
            messages_len: Queue::new()
        }
    }
}

impl Default for PeerState{
    fn default() -> Self {
        Self::new(0)
    }
}