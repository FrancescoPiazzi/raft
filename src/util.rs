use actum::drop_guard::ActorDropGuard;
use actum::prelude::*;
use tokio::task::JoinHandle;
use tracing::{info_span, Instrument};

use crate::config::{DEFAULT_ELECTION_TIMEOUT, DEFAULT_HEARTBEAT_PERIOD, DEFAULT_REPLICATION_PERIOD};
use crate::messages::add_peer::AddPeer;
use crate::messages::RaftMessage;
use crate::server::raft_server;
use crate::state_machine::StateMachine;

pub struct Server<SM, SMin, SMout> {
    pub server_id: u32,
    pub server_ref: ActorRef<RaftMessage<SMin, SMout>>,
    #[allow(dead_code)] // guard is not used but must remain in scope or the actors are dropped as well
    guard: ActorDropGuard,
    pub handle: JoinHandle<SM>,
}

pub fn spawn_raft_servers<SM, SMin, SMout>(n_servers: usize, state_machine: SM) -> Vec<Server<SM, SMin, SMout>>
where
    SM: StateMachine<SMin, SMout> + Send + Clone + 'static,
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    let mut servers = Vec::with_capacity(n_servers);

    for id in 0..n_servers {
        let state_machine = state_machine.clone();
        let actor = actum::<RaftMessage<SMin, SMout>, _, _, SM>(move |cell, me| async move {
            let me = (id as u32, me);
            raft_server(
                cell,
                me,
                n_servers - 1,
                state_machine,
                DEFAULT_ELECTION_TIMEOUT,
                DEFAULT_HEARTBEAT_PERIOD,
                DEFAULT_REPLICATION_PERIOD,
            )
            .await
        });
        let handle = tokio::spawn(actor.task.run_task().instrument(info_span!("server", id)));
        servers.push(Server {
            server_id: id as u32,
            server_ref: actor.m_ref,
            guard: actor.guard,
            handle,
        });
    }
    servers
}

pub fn send_peer_refs<SM, SMin, SMout>(servers: &[Server<SM, SMin, SMout>])
where
    SMin: Send + 'static,
    SMout: Send + 'static,
{
    for i in 0..servers.len() {
        let (server_id, mut server_ref) = {
            let server = &servers[i];
            (server.server_id, server.server_ref.clone())
        };

        for other_server in servers {
            if server_id != other_server.server_id {
                let _ = server_ref.try_send(RaftMessage::AddPeer(AddPeer {
                    peer_id: other_server.server_id,
                    peer_ref: other_server.server_ref.clone(),
                }));
            }
        }
    }
}
