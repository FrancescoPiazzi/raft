use std::collections::VecDeque as Queue;
use std::time::Duration;

use tokio::task::{JoinHandle, JoinSet};

use crate::raft::common_state::CommonState;
use crate::raft::messages::*;
use actum::prelude::*;

// wrapper struct to enforce some messages to be AppendEntriesRPCs
#[derive(Clone)]
struct AppendEntriesMessage<LogEntry>(AppendEntriesRPC<LogEntry>);

impl<LogEntry> From<AppendEntriesMessage<LogEntry>> for RaftMessage<LogEntry> {
    fn from(msg: AppendEntriesMessage<LogEntry>) -> Self {
        RaftMessage::AppendEntries(msg.0)
    }
}

impl<LogEntry> From<RaftMessage<LogEntry>> for AppendEntriesMessage<LogEntry> {
    fn from(msg: RaftMessage<LogEntry>) -> Self {
        match msg {
            RaftMessage::AppendEntries(append_entries_rpc) => AppendEntriesMessage(append_entries_rpc),
            _ => panic!("tried to convert a Raft non-AppendEntries message to an AppendEntriesMessage"),
        }
    }
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
    replication_period: Duration,
) where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + Clone + 'static,
{
    let mut peer_timeouts = JoinSet::new();
    peer_refs.iter_mut().for_each(|peer| {
        peer_timeouts.spawn(wait_and_return(peer.clone(), heartbeat_period));
    });

    loop {
        tokio::select! {
            message = cell.recv() => {
                let message = message.message().expect("raft runs indefinitely");
                let become_follow = handle_message(common_data, peer_refs, me, message, replication_period);
                if become_follow {
                    break;
                }
            },
            peer = peer_timeouts.join_next() => {
                let mut peer = peer.expect("leaders must have at least one follower").unwrap();
                send_heartbeat(common_data, &mut peer, me);
                peer_timeouts.spawn(wait_and_return(peer, heartbeat_period));
            },
        }
    }
}

// returns the given ActorRef after some time, used to send heartbeats to followers
async fn wait_and_return<LogEntry>(
    peer: ActorRef<RaftMessage<LogEntry>>,
    heartbeat_period: Duration,
) -> ActorRef<RaftMessage<LogEntry>>
where
    LogEntry: Send + Clone + 'static,
{
    tokio::time::sleep(heartbeat_period).await;
    peer
}

// send a heartbeat to a single follower
fn send_heartbeat<LogEntry>(
    common_data: &CommonState<LogEntry>,
    peer: &mut ActorRef<RaftMessage<LogEntry>>,
    me: &ActorRef<RaftMessage<LogEntry>>,
) where
    LogEntry: Send + Clone + 'static,
{
    let heartbeat_msg = RaftMessage::AppendEntries(AppendEntriesRPC {
        term: common_data.current_term,
        leader_ref: me.clone(),
        prev_log_index: 0,
        prev_log_term: 0,
        entries: Vec::new(),
        leader_commit: 0,
    });
    let _ = peer.try_send(heartbeat_msg);
}

async fn send_hearbeat_loop<LogEntry, AB>(
    common_data_server: &mut ActorRef<RaftMessage<LogEntry>>,
    peer_ref: &mut ActorRef<RaftMessage<LogEntry>>,
    me: &ActorRef<RaftMessage<LogEntry>>,
    cell: &mut AB,
    period: Duration,
) where
    LogEntry: Send + Clone + 'static,
    AB: ActorBounds<RaftMessage<LogEntry>>,
{
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        let common_data = get_common_data(me, cell, common_data_server).await;
        send_heartbeat(&common_data, peer_ref, me);
    }
}

// helper function to interrogate the common data server
async fn get_common_data<LogEntry, AB>(
    me: &ActorRef<RaftMessage<LogEntry>>,
    cell: &mut AB,
    common_data_server: &mut ActorRef<RaftMessage<LogEntry>>,
) -> CommonState<LogEntry>
where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + Clone + 'static,
{
    common_data_server.try_send(RaftMessage::GetCommonData(me.clone()));
    let common_data: RaftMessage<LogEntry> = cell.recv().await.message().expect("raft runs indefinitely");

    match common_data {
        RaftMessage::SetCommonData(common_data) => common_data,
        _ => {
            tracing::error!("Expected common data, got {:?}", common_data);
            panic!("Expected common data, got {:?}", common_data);
        }
    }
}

// handles one message as leader
// returns true if we have to go back to a follower state, false otherwise
fn handle_message<LogEntry>(
    common_data: &mut CommonState<LogEntry>,
    peer_refs: &mut [ActorRef<RaftMessage<LogEntry>>],
    me: &ActorRef<RaftMessage<LogEntry>>,
    message: RaftMessage<LogEntry>,
    replication_period: Duration,
) -> bool
where
    LogEntry: Send + Clone + 'static,
{
    match message {
        RaftMessage::AppendEntriesClient(mut append_entries_client_rpc) => {
            tracing::info!("⏭️ Received a message from a client, replicating it to the other nodes");

            let mut entries = append_entries_client_rpc
                .entries
                .clone()
                .into_iter()
                .map(|entry| (entry, common_data.current_term))
                .collect();
            common_data.log.append(&mut entries);

            let append_entries_msg = RaftMessage::AppendEntries(AppendEntriesRPC {
                term: common_data.current_term,
                leader_ref: me.clone(),
                prev_log_index: common_data.log.len() as u64, // log entries are 1-indexed, 0 means no log entries yet
                prev_log_term: 0,
                entries: append_entries_client_rpc.entries,
                leader_commit: 0,
            });

            for peer in peer_refs {}

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
async fn send_log_entry<LogEntry>(
    mut peer: ActorRef<RaftMessage<LogEntry>>,
    message: AppendEntriesMessage<LogEntry>,
    period: Duration,
) where
    LogEntry: Send + Clone + 'static,
{
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        let _ = peer.try_send(message.clone().into());
    }
}

pub async fn common_data_server<AB, LogEntry>(mut cell: AB) -> AB
where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + Clone + 'static,
{
    let mut common_data = None;
    loop {
        let message = cell.recv().await.message().expect("raft runs indefinitely");
        match message {
            RaftMessage::SetCommonData(new_common_data) => {
                common_data = Some(new_common_data);
            }
            RaftMessage::GetCommonData(mut sender) => {
                let _ = sender.try_send(
                    RaftMessage::SetCommonData(common_data
                        .clone()
                        .expect("common data requested before being set")
                        .clone()),
                );
            }
            _ => {
                tracing::trace!(unhandled = ?message);
            }
        }
    }
}

// this is an actum node that manages one peer, it replays the AppendEntriesRPCs from the leader until
// it recieves a response, which is then replayed back to the leader, it also keeps track of when was the last time
// the peer recieved a message, and sends an heartbeat if needed, this considerably lightens the load of the leader
pub async fn peer_manager<AB, LogEntry>(
    common_data_server: &'static mut ActorRef<RaftMessage<LogEntry>>,
    mut cell: AB,
    me: &'static ActorRef<RaftMessage<LogEntry>>,
    peer: &'static mut ActorRef<RaftMessage<LogEntry>>,
    replication_period: Duration,
) -> AB
where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + Clone + 'static,
{
    let mut message_queue: Queue<AppendEntriesMessage<LogEntry>> = Queue::new();
    let mut message_sender_handle: Option<JoinHandle<()>> = None;
    let mut heartbeat_sender_handle: Option<JoinHandle<()>> = Some(tokio::task::spawn(send_hearbeat_loop(
        common_data_server,
        peer,
        me,
        &mut cell,
        replication_period,
    )));
    let mut last_message_time = tokio::time::Instant::now();

    loop {
        tokio::select! {
            message = cell.recv() => {
                let message = message.message().expect("raft runs indefinitely");
                match message {
                    RaftMessage::AppendEntries(append_entries_rpc) => {
                        message_queue.push_back(AppendEntriesMessage(append_entries_rpc));
                    }
                    RaftMessage::AppendEntryResponse(_term, _success) => {
                    }
                    _ => {
                        tracing::trace!(unhandled = ?message);
                    }
                }
            },
            handle = async {
                if let Some(sender) = message_sender_handle.take() {
                    sender.await;
                }
            }, if message_sender_handle.is_some() => {

            }
        }
    }
}
