use actum::drop_guard::ActorDropGuard;
use actum::prelude::*;
use actum::testkit::testkit;
use tokio::task::JoinHandle;
use tracing::{info_span, Instrument, subscriber};
use tracing_subscriber::{fmt, layer::SubscriberExt, EnvFilter, Layer, Registry};
use tracing_appender::rolling::{RollingFileAppender, Rotation};

use crate::config::{DEFAULT_ELECTION_TIMEOUT, DEFAULT_HEARTBEAT_PERIOD};
use crate::messages::add_peer::AddPeer;
use crate::messages::RaftMessage;
use crate::server::raft_server;
use crate::state_machine::{StateMachine, VoidStateMachine};
use crate::types::SplitServers;

pub struct Server<SM, SMin, SMout> {
    pub server_id: u32,
    pub server_ref: ActorRef<RaftMessage<SMin, SMout>>,
    #[allow(dead_code)] // guard is not used but must remain in scope or the actors are dropped as well
    pub guard: ActorDropGuard,
    pub handle: JoinHandle<SM>,
}

/// Initialize a set of Raft servers with a given state machine and exchanges their references
pub fn init<SM, SMin, SMout>(n_servers: usize, state_machine: SM) -> Vec<Server<SM, SMin, SMout>>
where
    SM: StateMachine<SMin, SMout> + Send + Clone + 'static,
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    let (refs, ids, handles, guards) = spawn_raft_servers(n_servers, state_machine);
    send_peer_refs::<SM, SMin, SMout>(&refs, &ids);

    // zip together refs, ids, handles, and guards into a vector of Server structs
    refs.into_iter().zip(ids).zip(handles).zip(guards)
        .map(|(((server_ref, server_id), handle), guard)| Server {
            server_id,
            server_ref,
            guard,
            handle,
        })
        .collect()
}


pub fn init_split_servers<SM, SMin, SMout>(n_servers: usize, state_machine: SM) 
    -> SplitServers<SM, SMin, SMout>
where
    SM: StateMachine<SMin, SMout> + Send + Clone + 'static,
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    let (refs, ids, handles, guards) = spawn_raft_servers(n_servers, state_machine);
    send_peer_refs::<SM, SMin, SMout>(&refs, &ids);
    (refs, ids, handles, guards)
}


pub fn spawn_raft_servers<SM, SMin, SMout>(n_servers: usize, state_machine: SM) -> SplitServers<SM, SMin, SMout>
where
    SM: StateMachine<SMin, SMout> + Send + Clone + 'static,
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    let mut refs = Vec::with_capacity(n_servers);
    let mut ids = Vec::with_capacity(n_servers);
    let mut handles = Vec::with_capacity(n_servers);
    let mut guards = Vec::with_capacity(n_servers);

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
            )
            .await
        });
        let handle = tokio::spawn(actor.task.run_task().instrument(info_span!("server", id)));
        refs.push(actor.m_ref);
        ids.push(id as u32);
        handles.push(handle);
        guards.push(actor.guard);
    }
    (refs, ids, handles, guards)
}


pub fn spawn_raft_servers_testkit() {
    for id in 0..5 {
        let state_machine = VoidStateMachine::new();
        let (actor, tk) = testkit::<RaftMessage<(), ()>, _, _, VoidStateMachine>(move |cell, me| async move {
            let me = (id as u32, me);
            raft_server(
                cell,
                me,
                4,
                state_machine,
                DEFAULT_ELECTION_TIMEOUT,
                DEFAULT_HEARTBEAT_PERIOD,
            )
            .await
        });
        let handle = tokio::spawn(actor.task.run_task().instrument(info_span!("server", id)));
    }
}


pub fn send_peer_refs<SM, SMin, SMout>(refs: &[ActorRef<RaftMessage<SMin, SMout>>], ids: &[u32])
where
    SMin: Send + 'static,
    SMout: Send + 'static,
{
    for i in 0..refs.len() {
        for j in 0..refs.len() {
            if i != j {
                let _ = refs[i].clone().try_send(RaftMessage::AddPeer(AddPeer {
                    peer_id: ids[j],
                    peer_ref: refs[j].clone(),
                }));
            }
        }
    }
}

/// Set the global subscriber to a composite layer that writes logs to different files for each server
pub async fn split_file_logs(n_servers: usize, guards: &mut Vec<tracing_appender::non_blocking::WorkerGuard>) {
    if n_servers == 0 {
        return;
    }

    let composite_layer = {
        let mut layers: Option<Box<dyn Layer<Registry> + Send + Sync + 'static>> = None;

        for i in 0..n_servers {
            let file_appender = RollingFileAppender::new(Rotation::NEVER, "log", format!("server{}.log", i));
            let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
            guards.push(guard);

            let filter = EnvFilter::new(format!("[server{{id={}}}]", i));

            let fmt_layer = fmt::Layer::new()
                .with_writer(non_blocking)
                .with_filter(filter)
                .boxed();

            layers = match layers {
                Some(layer) => Some(layer.and_then(fmt_layer).boxed()),
                None => Some(fmt_layer),
            };
        }

        layers
    };

    if let Some(inner) = composite_layer {
        let subscriber = Registry::default().with(inner);
        if subscriber::set_global_default(subscriber).is_err(){
            tracing::error!("Unable to set global subscriber");
        }
    } else {
        tracing::error!("No layers were created for the composite layer");
    }
}