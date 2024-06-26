use tracing::Instrument;

use actum::prelude::*;

pub enum RaftMessage {
    AddPeer(ActorRef<RaftMessage>),
    RemovePeer(ActorRef<RaftMessage>),
    AppendEntry(String), // TODO: replace with any type that is Send + 'static
    AppendEntryResponse,
    RequestVote,
    RequestVoteResponse,
}

async fn raft_actor<AB>(mut cell: AB, _me: ActorRef<RaftMessage>)
where
    AB: ActorBounds<RaftMessage>,
{
    tracing::trace!("Raft actor running");
    let mut npeers = 0;
    loop {
        let msg = cell.recv().await.message();
        match msg {
            Some(raftmessage) => match raftmessage {
                RaftMessage::AddPeer(_peer) => {
                    npeers += 1;
                    tracing::info!("Peer added, total: {}", npeers);
                }
                _ => {
                    tracing::info!("Received a message");
                }
            },
            None => {
                tracing::info!("Received a None message, quitting");
                break;
            }
        }
    }
}


// works, but servers are in normal variables
#[allow(dead_code)]
async fn raft_1() {
    // create a simulated environment
    let main_node = actum(move |mut cell, _me: ActorRef<RaftMessage>| async move {
        let server1 = cell.spawn(raft_actor).await.unwrap();
        let server2 = cell.spawn(raft_actor).await.unwrap();
        let server3 = cell.spawn(raft_actor).await.unwrap();

        // store the m_ref attribute of the servers in a vector
        // I need it because I can't do this because of borrowing rules:
        // it shouldn't be causing any problem but it's one of the very few differences from the previous implementation
        // for server in servers_ref.iter_mut() {
        //     for other_server in servers_ref.iter() {
        //         if !std::ptr::eq(server, other_server) {
        //              let _ = server.try_send(RaftMessage::AddPeer(other_server.clone()));
        let servers_ref = vec![
            server1.m_ref.clone(),
            server2.m_ref.clone(),
            server3.m_ref.clone(),
        ];

        tracing::info!("Servers initialized");

        let handles = vec![
            tokio::spawn(
                server1
                    .task
                    .run_task()
                    .instrument(tracing::info_span!("server1")),
            ),
            tokio::spawn(
                server2
                    .task
                    .run_task()
                    .instrument(tracing::info_span!("server2")),
            ),
            tokio::spawn(
                server3
                    .task
                    .run_task()
                    .instrument(tracing::info_span!("server3")),
            ),
        ];

        for server_ref in &servers_ref {
            for other_server_ref in &servers_ref {
                if !std::ptr::eq(server_ref, other_server_ref) {
                    let mut server1 = server_ref.clone();
                    let server2 = other_server_ref.clone();
                    let _ = server1.try_send(RaftMessage::AddPeer(server2));
                }
            }
        }
        tracing::info!("Neighbors initialized");

        for handle in handles {
            handle.await.unwrap();
        }
    });

    let handle = tokio::spawn(
        main_node
            .task
            .run_task()
            .instrument(tracing::info_span!("actum")),
    );
    handle.await.unwrap();

    tracing::info!("Main node has finished");
}


// doesn't work already
async fn raft_2() {
    // create a simulated environment
    let main_node = actum(move |mut cell, _me: ActorRef<RaftMessage>| async move {
        let server1 = cell.spawn(raft_actor).await.unwrap();
        let server2 = cell.spawn(raft_actor).await.unwrap();
        let server3 = cell.spawn(raft_actor).await.unwrap();

        let servers = vec![server1, server2, server3];

        let servers_ref = servers
            .iter()
            .map(|server| server.m_ref.clone())
            .collect::<Vec<_>>();

        tracing::info!("Servers initialized");

        let handles = servers
            .into_iter()
            .map(|server| {
                tokio::spawn(
                    server
                        .task
                        .run_task()
                        .instrument(tracing::info_span!("server")),
                )
            })
            .collect::<Vec<_>>();

        for server_ref in &servers_ref {
            for other_server_ref in &servers_ref {
                if !std::ptr::eq(server_ref, other_server_ref) {
                    let mut server1 = server_ref.clone();
                    let server2 = other_server_ref.clone();
                    let _ = server1.try_send(RaftMessage::AddPeer(server2));
                }
            }
        }
        tracing::info!("Neighbors initialized");

        for handle in handles {
            handle.await.unwrap();
        }
    });

    let handle = tokio::spawn(
        main_node
            .task
            .run_task()
            .instrument(tracing::info_span!("actum")),
    );
    handle.await.unwrap();

    tracing::info!("Main node has finished");
}

#[tokio::main]
async fn main() {
    // initialize program logger
    tracing_subscriber::fmt()
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::NEW
                | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        )
        .with_target(false)
        .with_line_number(true)
        .with_max_level(tracing::Level::INFO)
        .init();

    raft_2().await;
}
