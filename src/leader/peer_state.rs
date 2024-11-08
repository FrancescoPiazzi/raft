use std::collections::VecDeque;

pub struct PeerState {
    pub next_index: u64,
    pub match_index: u64,

    // Track the length of log entries sent to each follower but not yet acknowledged.
    // This is used to calculate next_index. If an AppendEntriesResponse is lost, the follower
    // will correct us if next_index is too high, or overwrite logs if it's too low.
    // The queue handles multiple messages sent before receiving a response, assuming
    // replies are generally in order. If not, the same correction logic applies.
    pub messages_len: VecDeque<usize>,
}

impl PeerState {
    pub const fn new(initial_next_index: u64) -> Self {
        Self {
            next_index: initial_next_index,
            match_index: 0,
            messages_len: VecDeque::new(),
        }
    }
}

impl Default for PeerState {
    fn default() -> Self {
        Self::new(0)
    }
}
