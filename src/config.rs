use std::ops::Range;
use std::time::Duration;

pub const DEFAULT_ELECTION_TIMEOUT: Range<Duration> = Duration::from_millis(200)..Duration::from_millis(350);

pub const DEFAULT_HEARTBEAT_PERIOD: Duration = Duration::from_millis(100);
