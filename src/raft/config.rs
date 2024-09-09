use std::ops::Range;
use std::time::Duration;

#[allow(dead_code)]
pub const DEFAULT_ELECTION_TIMEOUT: Range<Duration> = Duration::from_millis(150)..Duration::from_millis(300);
