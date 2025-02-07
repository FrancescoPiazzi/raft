use std::ops::Range;

use crate::config::{DEFAULT_ELECTION_TIMEOUT, DEFAULT_HEARTBEAT_PERIOD};
use crate::messages::add_peer::AddPeer;
use crate::messages::RaftMessage;
use crate::server::raft_server;
use crate::state_machine::StateMachine;
use actum::drop_guard::ActorDropGuard;
use actum::prelude::*;
use actum::testkit::{testkit, Testkit};
use itertools::izip;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tracing::{info_span, subscriber, Instrument};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{fmt, layer::SubscriberExt, EnvFilter, Layer, Registry};

pub struct Server<SM, SMin, SMout> {
    pub server_id: u32,
    pub server_ref: ActorRef<RaftMessage<SMin, SMout>>,
    #[allow(dead_code)]
    /// Although the actor guard is never used, it must remain in scope or the actor is otherwise dropped.
    pub guard: ActorDropGuard,
    pub handle: JoinHandle<SM>,
}

impl<SM, SMin, SMout> Server<SM, SMin, SMout> {
    pub fn new(
        server_id: u32,
        server_ref: ActorRef<RaftMessage<SMin, SMout>>,
        guard: ActorDropGuard,
        handle: JoinHandle<SM>,
    ) -> Self {
        Self {
            server_id,
            server_ref,
            guard,
            handle,
        }
    }
}

pub fn init_servers_split<SM, SMin, SMout>(
    n_servers: usize,
    state_machine: SM,
    election_timeout: Option<Range<Duration>>,
    heartbeat_period: Option<Duration>,
) -> SplitServers<SM, SMin, SMout>
where
    SM: StateMachine<SMin, SMout> + Send + Clone + 'static,
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    let split_servers = spawn_raft_servers(n_servers, state_machine, election_timeout, heartbeat_period);
    send_peer_refs::<SM, SMin, SMout>(&split_servers.server_ref_vec, &split_servers.server_id_vec);
    split_servers
}

pub fn spawn_raft_servers<SM, SMin, SMout>(
    n_servers: usize,
    state_machine: SM,
    election_timeout: Option<Range<Duration>>,
    heartbeat_period: Option<Duration>,
) -> SplitServers<SM, SMin, SMout>
where
    SM: StateMachine<SMin, SMout> + Send + Clone + 'static,
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    let mut server_ref_vec = Vec::with_capacity(n_servers);
    let mut server_id_vec = Vec::with_capacity(n_servers);
    let mut handle_vec = Vec::with_capacity(n_servers);
    let mut guard_vec = Vec::with_capacity(n_servers);

    for id in 0..n_servers {
        let state_machine = state_machine.clone();
        let election_timeout = election_timeout.clone();
        let actor = actum::<RaftMessage<SMin, SMout>, _, _, SM>(move |cell, _| async move {
            raft_server(
                cell,
                id as u32,
                n_servers - 1,
                state_machine,
                election_timeout.unwrap_or(DEFAULT_ELECTION_TIMEOUT),
                heartbeat_period.unwrap_or(DEFAULT_HEARTBEAT_PERIOD),
            )
            .await
        });
        let handle = tokio::spawn(actor.task.run_task().instrument(info_span!("server", id)));
        server_ref_vec.push(actor.m_ref);
        server_id_vec.push(id as u32);
        handle_vec.push(handle);
        guard_vec.push(actor.guard);
    }

    SplitServers {
        server_ref_vec,
        server_id_vec,
        handle_vec,
        guard_vec,
    }
}

/// Initializes a set of Raft servers, returns parallel vectors containing all the information about them
/// Parameters:
/// - `n_servers`: the number of servers to spawn
/// - `state_machine`: the state machine to use for each server
/// - `election_timeout`: the range of election timeouts to use for each server, None to use the default
/// - `heartbeat_period`: the heartbeat period to use for each server, None to use the default
/// - `n_servers_total`: the total number of servers in the cluster, None to use `n_servers`
pub fn spawn_raft_servers_testkit<SM, SMin, SMout>(
    n_servers: usize,
    state_machine: SM,
    election_timeout: Option<Range<Duration>>,
    heartbeat_period: Option<Duration>,
    n_servers_total: Option<usize>,
) -> (SplitServers<SM, SMin, SMout>, Vec<Testkit<RaftMessage<SMin, SMout>>>)
where
    SM: StateMachine<SMin, SMout> + Send + Clone + 'static,
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    let mut server_ref_vec = Vec::with_capacity(n_servers);
    let mut server_id_vec = Vec::with_capacity(n_servers);
    let mut handle_vec = Vec::with_capacity(n_servers);
    let mut guard_vec = Vec::with_capacity(n_servers);
    let mut testkit_vec = Vec::with_capacity(n_servers);

    for id in 0..n_servers {
        let state_machine = state_machine.clone();
        let election_timeout = election_timeout.clone();
        let (actor, tk) = testkit::<RaftMessage<SMin, SMout>, _, _, SM>(move |cell, _| async move {
            raft_server(
                cell,
                id as u32,
                n_servers_total.unwrap_or(n_servers) - 1,
                state_machine,
                election_timeout.unwrap_or(DEFAULT_ELECTION_TIMEOUT),
                heartbeat_period.unwrap_or(DEFAULT_HEARTBEAT_PERIOD),
            )
            .await
        });
        let handle = tokio::spawn(actor.task.run_task().instrument(info_span!("server", id)));
        server_ref_vec.push(actor.m_ref);
        server_id_vec.push(id as u32);
        handle_vec.push(handle);
        guard_vec.push(actor.guard);
        testkit_vec.push(tk);
    }

    let split_servers = SplitServers {
        server_ref_vec,
        server_id_vec,
        handle_vec,
        guard_vec,
    };
    (split_servers, testkit_vec)
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

            let fmt_layer = fmt::Layer::new().with_writer(non_blocking).with_filter(filter).boxed();

            layers = match layers {
                Some(layer) => Some(layer.and_then(fmt_layer).boxed()),
                None => Some(fmt_layer),
            };
        }

        layers
    };

    if let Some(inner) = composite_layer {
        let subscriber = Registry::default().with(inner);
        if subscriber::set_global_default(subscriber).is_err() {
            tracing::error!("Unable to set global subscriber");
        }
    } else {
        tracing::error!("No layers were created for the composite layer");
    }
}

pub struct SplitServers<SM, SMin, SMout> {
    pub server_id_vec: Vec<u32>,
    pub server_ref_vec: Vec<ActorRef<RaftMessage<SMin, SMout>>>,
    pub guard_vec: Vec<ActorDropGuard>,
    pub handle_vec: Vec<JoinHandle<SM>>,
}

impl<SM, SMin, SMout> SplitServers<SM, SMin, SMout> {
    pub fn into_server_vec(self) -> Vec<Server<SM, SMin, SMout>> {
        let zip = izip!(self.server_ref_vec, self.server_id_vec, self.handle_vec, self.guard_vec);
        zip.map(|(server_ref, server_id, handle, guard)| Server::new(server_id, server_ref, guard, handle))
            .collect()
    }
}
