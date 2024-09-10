use std::ops::Range;
use std::time::Duration;

// TOASK: is this the right place? Or should this hold only the configuration of one specific node?
pub const N_NODES: usize = 5;

#[allow(dead_code)]
pub const DEFAULT_ELECTION_TIMEOUT: Range<Duration> = Duration::from_millis(150)..Duration::from_millis(300);
