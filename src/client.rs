use actum::prelude::*;

use crate::raft::messages::{AppendEntriesClientRPC, RaftMessage};

// example of a raft client requesting the replication of entries
// the client will first recieve a message from the simulator, then replay that to the raft nodes forever
// this allows to not make any assumptions about the type of the message,
// at the expense of the client not being decoupled from the simulator
// creating a client that sends messages of a specific type i.e. ActorRef<RaftMessage<String>> will make the compiler
// complain because it's a different type from ActorRef<RaftMessage<LogEntry>> that is used in the raft_nodes
// TOASK: is there a way to get it to work? String repects all the traits of LogEntry
pub async fn client<AB, LogEntry>(mut cell: AB, me: ActorRef<RaftMessage<LogEntry>>, server_count: usize) -> AB
where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Clone + Send + 'static,
{
    // TODO: these are a bit magic numbers, but also the client should't necessarly be a raft node anyway
    let initial_wait = std::time::Duration::from_secs(2);
    let interval_between_messages = std::time::Duration::from_millis(2345);

    tokio::time::sleep(initial_wait).await;

    let raft_nodes = init_raft_nodes(&mut cell, server_count).await;
    let message_to_send = get_message_to_send(&mut cell).await;
    let mut leader = raft_nodes[0].clone();

    tracing::trace!("Client ready to send messages");

    loop {
        tokio::time::sleep(interval_between_messages).await;

        let msg = RaftMessage::AppendEntriesClient(AppendEntriesClientRPC {
            entries: message_to_send.clone(),
            client_ref: me.clone(),
        });
        let _ = leader.try_send(msg);

        let message = cell.recv().await.message().expect("Received a None message, quitting");

        match message {
            RaftMessage::AppendEntriesClientResponse(response) => {
                if let Err(Some(leader_ref)) = response {
                    tracing::trace!("Sent a message to the wrong node, updating leader");
                    leader = leader_ref.clone();
                }
            }
            _ => {
                tracing::warn!("Client received unexpected message");
            }
        }
    }
}

// recieve the messages from the simulator to get the raft nodes refs
async fn init_raft_nodes<AB, LogEntry>(cell: &mut AB, n_nodes: usize) -> Vec<ActorRef<RaftMessage<LogEntry>>>
where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + 'static,
{
    let mut raft_nodes: Vec<ActorRef<RaftMessage<LogEntry>>> = Vec::new();

    while raft_nodes.len() < n_nodes {
        let message = cell.recv().await.message().expect("Received a None message, quitting");
        if let RaftMessage::AddPeer(peer) = message {
            raft_nodes.push(peer);
        }
    }

    raft_nodes
}

// used to get the message that the client will replay forever from the upper node
// this is because we can't instantiate the message here, as we can't do any assumptions about the message type
async fn get_message_to_send<AB, LogEntry>(cell: &mut AB) -> Vec<LogEntry>
where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Clone + Send + 'static,
{
    let RaftMessage::InitMessage(message) = cell.recv().await.message().expect("Received a None message, quitting")
    else {
        panic!("Received an unexpected message");
    };

    message
}
