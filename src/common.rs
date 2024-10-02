use crate::common_state::CommonState;

// commit log entries up to the leader's commit index
// the entire common_data object is taken even if for now only the commit_index and last_applied are used
// because in the future I will want to access the log entries to actually apply them
pub fn commit<LogEntry>(common_data: &mut CommonState<LogEntry>) {
    while common_data.last_applied < common_data.commit_index {
        common_data.last_applied += 1;
        tracing::info!("🎉 Applied log entry {}", common_data.last_applied);
    }
}
