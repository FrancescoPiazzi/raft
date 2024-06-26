use futures::StreamExt;
use tracing::Instrument;
use std::{any::Any, iter::Rev, thread::{sleep, JoinHandle}};

use actum::prelude::*;


pub enum RaftMessage {
    AddPeer(ActorRef<RaftMessage>),
    RemovePeer(ActorRef<RaftMessage>),
    AppendEntry(String),    // TODO: replace with any type that is Send + 'static
    AppendEntryResponse,
    RequestVote,
    RequestVoteResponse,
}


async fn raft_actor<AB>(mut cell: AB, me: ActorRef<RaftMessage>) 
where AB: ActorBounds<RaftMessage>
{
    tracing::trace!("Raft actor running");
    let mut npeers = 0;
    loop{
        let msg = cell.recv().await.message().unwrap();
        match msg {
            RaftMessage::AddPeer(peer) => {
                npeers += 1;
                tracing::info!("Peer added, total: {}", npeers);
            },
            _ => {
                tracing::info!("Received a message");
            }
        }
    }
}


// this works, but nodes aren't in a vector
async fn raft_1(){
    // create a simulated environment
    let main_node = actum(move | mut cell, _me: ActorRef<RaftMessage> | async move {     
        let mut server1 = cell.spawn(raft_actor).await.unwrap();
        let mut server2 = cell.spawn(raft_actor).await.unwrap();
        let mut server3 = cell.spawn(raft_actor).await.unwrap();

        tracing::info!("Servers initialized");
        sleep(std::time::Duration::from_secs(3));

        let handles = vec![
            tokio::spawn(server1.task.run_task().instrument(tracing::info_span!("server1"))),
            tokio::spawn(server2.task.run_task().instrument(tracing::info_span!("server2"))),
            tokio::spawn(server3.task.run_task().instrument(tracing::info_span!("server3"))),
        ];

        tracing::info!("Handles initialized");
        sleep(std::time::Duration::from_secs(3));

        let _ = server1.m_ref.try_send(RaftMessage::AddPeer(server2.m_ref.clone()));
        let _ = server1.m_ref.try_send(RaftMessage::AddPeer(server3.m_ref.clone()));
        let _ = server2.m_ref.try_send(RaftMessage::AddPeer(server1.m_ref.clone()));
        let _ = server2.m_ref.try_send(RaftMessage::AddPeer(server3.m_ref.clone()));
        let _ = server3.m_ref.try_send(RaftMessage::AddPeer(server1.m_ref.clone()));
        let _ = server3.m_ref.try_send(RaftMessage::AddPeer(server2.m_ref.clone()));
        
        tracing::info!("Neighbors initialized");
        
        sleep(std::time::Duration::from_secs(3));

        for handle in handles {
            handle.await.unwrap();
        }
    });

    let handle = tokio::spawn(main_node.task.run_task().instrument(tracing::info_span!("actum")));
    handle.await.unwrap();

    tracing::info!("Main node has finished");
}


// this doesn't work, the first call of recv() in the raft_actor immediately returns None
async fn raft_2(){
    // create a simulated environment
    let main_node = actum(move | mut cell, _me: ActorRef<RaftMessage> | async move {     
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
            server3.m_ref.clone()
        ];

        tracing::info!("Servers initialized");

        let handles = vec![
            tokio::spawn(server1.task.run_task().instrument(tracing::info_span!("server1"))),
            tokio::spawn(server2.task.run_task().instrument(tracing::info_span!("server2"))),
            tokio::spawn(server3.task.run_task().instrument(tracing::info_span!("server3"))),
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

    let handle = tokio::spawn(main_node.task.run_task().instrument(tracing::info_span!("actum")));
    handle.await.unwrap();

    tracing::info!("Main node has finished");
}


// minimal code to reproduce the issue, recv() immediately returns None, even if no message has been sent
async fn raft_3(){
    let main_node = actum(move | mut cell, _me: ActorRef<RaftMessage> | async move {     
        let mut servers = Vec::new();

        for _ in 0..5 {
            let server = cell.spawn(raft_actor).await.unwrap();
            servers.push(server);
        }

        tracing::info!("Servers initialized");
        sleep(std::time::Duration::from_secs(3));

        let mut handles = Vec::new();
        for server in servers {
            let handle = tokio::spawn(server.task.run_task().instrument(tracing::info_span!("server")));
            handles.push(handle);
        }

        tracing::info!("Handles initialized");
        sleep(std::time::Duration::from_secs(3));

        // get all the servers to know each other
        // for i in 0..servers_ref.len() {
        //     for j in 0..servers_ref.len() {
        //         if i!=j {
        //             // must clone the references otherwise I'd have to borrow the vector once mutably and once immutably
        //             let server = &mut servers_ref[i].clone();
        //             let other_server = &servers_ref[j].clone();
        //             if !std::ptr::eq(server, other_server) {
        //                 let _ = server.try_send(RaftMessage::AddPeer(other_server.clone()));
        //             }
        //         }
        //     }
        // }
        
        tracing::info!("Neighbors initialized");
        
        sleep(std::time::Duration::from_secs(3));

        for handle in handles {
            handle.await.unwrap();
        }
    });

    let handle = tokio::spawn(main_node.task.run_task().instrument(tracing::info_span!("actum")));
    handle.await.unwrap();

    tracing::info!("Main node has finished");
}


#[tokio::main]
async fn main() {
    // initialize program logger
    tracing_subscriber::fmt()
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::NEW | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        )
        .with_target(false)
        .with_line_number(true)
        .with_max_level(tracing::Level::TRACE)
        .init();


    // raft_1().await;
    raft_2().await;
}

