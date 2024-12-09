pub struct PeerState {
    pub next_index: u64,
    pub match_index: u64,
}

impl PeerState {
    pub const fn new(initial_next_index: u64) -> Self {
        Self {
            next_index: initial_next_index,
            match_index: 0,
        }
    }
}

impl Default for PeerState {
    fn default() -> Self {
        Self::new(0)
    }
}
