use std::time::Duration;

use tokio::task::JoinSet;

use crate::raft::common_state::CommonState;
use crate::raft::messages::*;
use actum::prelude::*;

#[derive(Clone)]
struct Follower<LogEntry>{
    actor_ref: ActorRef<RaftMessage<LogEntry>>,
    next_index: u64,
    match_index: u64,
}

// the leader is the interface of the cluster to the external world
// clients send messages to the leader, which is responsible for replicating them to the other nodes
// after receiving confirmation from the majority of the nodes, the leader commits the message as agreed
// returns when another leader or candidate with a higher term is detected
pub async fn leader<AB, LogEntry>(
    cell: &mut AB,
    common_data: &mut CommonState<LogEntry>,
    peer_refs: &mut [ActorRef<RaftMessage<LogEntry>>],
    me: &ActorRef<RaftMessage<LogEntry>>,
    heartbeat_period: Duration,
    _replication_period: Duration,
) where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + Clone + 'static,
{
    let mut followers: Vec<Follower<LogEntry>> = peer_refs
        .into_iter()
        .map(|peer| Follower{actor_ref: peer.clone(), next_index: 0, match_index: 0})
        .collect();

    let mut followers_timeouts = JoinSet::new();
    followers.into_iter().for_each(|follower| {
        followers_timeouts.spawn(wait_and_return(follower, heartbeat_period));
    });

    loop {
        tokio::select! {
            message = cell.recv() => {
                let message = message.message().expect("raft runs indefinitely");
                let become_follow = handle_message(common_data, peer_refs, me, message, followers);
                if become_follow {
                    break;
                }
            },
            timed_out_follower = followers_timeouts.join_next() => {
                let mut timed_out_follower = timed_out_follower.expect("leaders must have at least one follower").unwrap();
                update_follower(common_data, &mut timed_out_follower, me);
                followers_timeouts.spawn(wait_and_return(timed_out_follower, heartbeat_period));
            },
        }
    }
}

// returns the given object after some time, useful to keep track of follower timeouts
async fn wait_and_return<T>(x: T, heartbeat_period: Duration) -> T 
{
    tokio::time::sleep(heartbeat_period).await;
    x
}

// send an appendentriesRPC to a single follower
fn update_follower<LogEntry>(
    common_data: &mut CommonState<LogEntry>,
    follower: &mut Follower<LogEntry>,
    me: &ActorRef<RaftMessage<LogEntry>>,
) where
    LogEntry: Send + Clone + 'static,
{
    // TODO: maybe try avoiding adding the term after each logentry before so I don't have to strip them here
    let stuff_to_send = common_data.log[follower.next_index as usize..]
        .iter()
        .map(|x| {x.0.clone()})
        .collect();

    // TODO
    let append_entries_msg = RaftMessage::AppendEntries(AppendEntriesRPC {
        term: common_data.current_term,
        leader_ref: me.clone(),
        prev_log_index: follower.next_index,
        prev_log_term: 0,
        entries: stuff_to_send,
        leader_commit: 0,
    });

    let _ = follower.actor_ref.try_send(append_entries_msg);
}


// handles one message as leader
// returns true if we have to go back to a follower state, false otherwise
fn handle_message<LogEntry>(
    common_data: &mut CommonState<LogEntry>,
    peer_refs: &mut [ActorRef<RaftMessage<LogEntry>>],
    me: &ActorRef<RaftMessage<LogEntry>>,
    message: RaftMessage<LogEntry>,
    follower_states: Vec<Follower<LogEntry>>
)-> bool
where
    LogEntry: Send + Clone + 'static,
{
    match message {
        RaftMessage::AppendEntriesClient(mut append_entries_client_rpc) => {
            tracing::info!("⏭️ Received a message from a client, replicating it to the other nodes");

            let mut entries = append_entries_client_rpc.entries.clone().into_iter().map(|entry| (entry, common_data.current_term)).collect();
            common_data.log.append(&mut entries);

            let append_entries_msg = RaftMessage::AppendEntries(AppendEntriesRPC {
                term: common_data.current_term,
                leader_ref: me.clone(),
                prev_log_index: common_data.log.len() as u64,   // log entries are 1-indexed, 0 means no log entries yet
                prev_log_term: 0,
                entries: append_entries_client_rpc.entries.clone(),
                leader_commit: 0,
            });

            for peer in peer_refs.iter_mut() {
                let _ = peer.try_send(append_entries_msg.clone());
            }

            // TODO: send the confirmation when it's committed instead of here
            let msg = RaftMessage::AppendEntriesClientResponse(Ok(()));
            let _ = append_entries_client_rpc.client_ref.try_send(msg);
        }
        RaftMessage::AppendEntries(append_entries_rpc) => {
            tracing::trace!("Received an AppendEntries message as the leader, somone challenged me");
            // TOASK: should it be >= ?
            if append_entries_rpc.term > common_data.current_term {
                tracing::trace!("They are right, I'm stepping down");
                return true;
            } else {
                tracing::trace!("They are wrong, long live the king!");
            }
        }
        RaftMessage::RequestVote(mut request_vote_rpc) => {
            tracing::trace!("Received a request vote message as the leader, somone challenged me");
            // TOASK: should it be >= ?
            let step_down_from_lead = request_vote_rpc.term > common_data.current_term;
            let msg = RaftMessage::RequestVoteResponse(step_down_from_lead);
            let _ = request_vote_rpc.candidate_ref.try_send(msg);
            if step_down_from_lead {
                tracing::trace!("They are right, granted vote and stepping down");
                common_data.voted_for = Some(request_vote_rpc.candidate_ref);
                return true;
            } else {
                tracing::trace!("They are wrong, long live the king!");
            }
        }
        RaftMessage::AppendEntryResponse(_term, _success) => {
            tracing::trace!("✔️ Received an AppendEntryResponse message");
            // TODO: check if we have a majority of the nodes and commit the message if so
            // otherwise increment the number of nodes that have added the entry
        }
        // normal to recieve some extra votes if we just got elected but we don't care
        RaftMessage::RequestVoteResponse(_) => {}
        _ => {
            tracing::trace!(unhandled = ?message);
        }
    }
    false
}


// sends a message to a peer at a constant rate forever, used to replicate log entries, must be canceled from the outside
async fn send_log_entry<LogEntry>(mut peer: ActorRef<RaftMessage<LogEntry>>, message: RaftMessage<LogEntry>, period: Duration) 
where LogEntry: Send + Clone + 'static
{
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        let _ = peer.try_send(message.clone());
    }
}