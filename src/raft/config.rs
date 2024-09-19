use std::ops::Range;
use std::time::Duration;

// TOASK: is this the right place? Or should this hold only the configuration of one specific node?
pub const N_NODES: usize = 5;

#[allow(dead_code)]
pub const DEFAULT_ELECTION_TIMEOUT: Range<Duration> = Duration::from_millis(150)..Duration::from_millis(300);

// leader will resend the AppendEntries message to each follower 
// if it doesn't receive a response in this time
pub const REPLICATION_PERIOD: Duration = Duration::from_millis(250);