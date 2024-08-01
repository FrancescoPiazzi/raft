use actum::prelude::*;

use crate::node::RaftMessage;
use crate::node::AppendEntriesClientRPC;

pub async fn client<AB, LogEntry>(
    mut cell: AB,
    me: ActorRef<RaftMessage<LogEntry>>,
) -> AB
where
    AB: ActorBounds<RaftMessage<LogEntry>>,
    LogEntry: Send + 'static,
{
    let n_raft_nodes = 5;

    let initial_wait = std::time::Duration::from_secs(5);
    let interval_between_messages = std::time::Duration::from_millis(500);

    tokio::time::sleep(initial_wait).await;

    let raft_nodes = init_raft_nodes(&mut cell, n_raft_nodes).await;
    let mut leader = raft_nodes[0].clone();

    loop {
        tokio::time::sleep(interval_between_messages).await;

        let _ = leader.try_send(RaftMessage::AppendEntriesClient(AppendEntriesClientRPC {
            entries: vec![],
            client_ref: me.clone(),
        }));

        let raftmessage = if let Some(raftmessage) = cell.recv().await.message() {
            raftmessage
        } else {
            tracing::info!("Received a None message, quitting");
            panic!("Received a None message");
        };
        
        match raftmessage{
            RaftMessage::AppendEntriesClientResponse(response) => {
                if let Err(Some(leader_ref)) = response {
                    tracing::info!("Sent a message to the wrong node, updating leader");
                    leader = leader_ref.clone();
                }
            }
            _ => {
                tracing::info!("Client received unexpected message");
            }
        }
    }
}


async fn init_raft_nodes<AB, LogEntry>(cell: &mut AB, n_nodes: usize) -> Vec<ActorRef<RaftMessage<LogEntry>>> 
where 
AB: ActorBounds<RaftMessage<LogEntry>>,
LogEntry: Send + 'static,
{
    let mut raft_nodes: Vec<ActorRef<RaftMessage<LogEntry>>> = Vec::new();

    while raft_nodes.len() < n_nodes {
        let raftmessage = if let Some(raftmessage) = cell.recv().await.message() {
            raftmessage
        } else {
            tracing::info!("Received a None message, quitting");
            panic!("Received a None message");
        };
        match raftmessage {
            RaftMessage::AddPeer(peer) => {
                raft_nodes.push(peer);
            }
            _ => { }
        }
    }

    raft_nodes
}