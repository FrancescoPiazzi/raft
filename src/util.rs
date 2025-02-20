use std::ops::Range;
use std::fmt::Debug;

use crate::config::{DEFAULT_ELECTION_TIMEOUT, DEFAULT_HEARTBEAT_PERIOD};
use crate::messages::add_peer::AddPeer;
use crate::messages::RaftMessage;
use crate::server::raft_server;
use crate::state_machine::StateMachine;
use crate::types::AppendEntriesClientResponse;
use crate::prelude::AppendEntriesClientRequest;
use actum::drop_guard::ActorDropGuard;
use actum::effect::Effect;
use actum::prelude::*;
use actum::testkit::{testkit, Testkit};
use itertools::izip;
use rand::prelude::IteratorRandom;
use rand::Rng;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tokio::sync::mpsc;
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

pub struct ServerWithTestkit<SM, SMin, SMout> {
    pub server: Server<SM, SMin, SMout>,
    pub testkit: Testkit<RaftMessage<SMin, SMout>>,
}

impl<SM, SMin, SMout> ServerWithTestkit<SM, SMin, SMout> {
    pub fn new(
        server_id: u32,
        server_ref: ActorRef<RaftMessage<SMin, SMout>>,
        guard: ActorDropGuard,
        handle: JoinHandle<SM>,
        testkit: Testkit<RaftMessage<SMin, SMout>>,
    ) -> Self {
        let server = Server::new(server_id, server_ref, guard, handle);
        Self { server, testkit }
    }
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
) -> SplitServersWithTestkit<SM, SMin, SMout>
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

    SplitServersWithTestkit {
        server_ref_vec,
        server_id_vec,
        handle_vec,
        guard_vec,
        testkit_vec,
    }
}

pub fn send_peer_refs<SMin, SMout>(refs: &[ActorRef<RaftMessage<SMin, SMout>>], ids: &[u32])
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
pub fn split_file_logs(n_servers: usize) -> Vec<tracing_appender::non_blocking::WorkerGuard> {
    let mut guards = Vec::new();

    let composite_layer = {
        let mut layers: Box<dyn Layer<Registry> + Send + Sync + 'static> = Box::new(fmt::Layer::new());

        for i in 0..n_servers {
            let file_appender = RollingFileAppender::new(Rotation::NEVER, "log", format!("server{}.log", i));
            let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
            guards.push(guard);

            let filter = EnvFilter::new(format!("[server{{id={}}}]", i));

            let fmt_layer = fmt::Layer::new().with_writer(non_blocking).with_filter(filter).boxed();

            layers = layers.and_then(fmt_layer).boxed();
        }

        layers
    };

    let subscriber = Registry::default().with(composite_layer);
    subscriber::set_global_default(subscriber).expect("failed to set global default subscriber");

    guards
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

pub struct SplitServersWithTestkit<SM, SMin, SMout> {
    pub server_id_vec: Vec<u32>,
    pub server_ref_vec: Vec<ActorRef<RaftMessage<SMin, SMout>>>,
    pub guard_vec: Vec<ActorDropGuard>,
    pub handle_vec: Vec<JoinHandle<SM>>,
    pub testkit_vec: Vec<Testkit<RaftMessage<SMin, SMout>>>,
}

impl<SM, SMin, SMout> SplitServersWithTestkit<SM, SMin, SMout> {
    pub fn into_server_with_testkt_vec(self) -> Vec<ServerWithTestkit<SM, SMin, SMout>> {
        let zip = izip!(
            self.server_ref_vec,
            self.server_id_vec,
            self.handle_vec,
            self.guard_vec,
            self.testkit_vec
        );
        zip.map(|(server_ref, server_id, handle, guard, testkit)| {
            ServerWithTestkit::new(server_id, server_ref, guard, handle, testkit)
        })
        .collect()
    }
}

pub async fn run_testkit_until_actor_returns<SMin, SMout>(mut testkit: Testkit<RaftMessage<SMin, SMout>>)
where
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    loop {
        match testkit.test_next_effect(|_| {}).await {
            None => break,
            Some((returned, ())) => {
                testkit = returned;
            }
        }
    }
}

pub async fn run_testkit_until_actor_returns_and_drop_messages<SMin, SMout>(
    mut testkit: Testkit<RaftMessage<SMin, SMout>>, 
    message_drop_probability: f64
) where
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    loop {
        match testkit.test_next_effect(|effect| {
            if let Effect::Recv(mut inner) = effect{
                match inner.recv {
                    Recv::Message(msg) => {
                        match msg {
                            // let add peer messages through or servers will never start
                            RaftMessage::AddPeer(_) => {}
                            // Temporarily let client requests as well
                            RaftMessage::AppendEntriesClientRequest(_) => {}
                            message => {
                                if rand::random::<f64>() < message_drop_probability {
                                    tracing::trace!("🔥 dropping message {:?}", message);
                                    inner.discard();
                                }
                            }
                        }
                    },
                    _ => {}
                }
            }
        }).await {
            None => break,
            Some((returned, ())) => {
                testkit = returned;
            }
        }
    }
}

pub async fn request_entry_replication<SM, SMin, SMout>(
    server_refs: &Vec<ActorRef<RaftMessage<SMin, SMout>>>,
    entries: Vec<SMin>,
    request_timeout: Duration,
) -> Vec<SMout>
where
    SMin: Clone + Send + 'static,
    SMout: Send + Debug + 'static,
{
    let mut output_vec;
    let mut idx = 0;
    let mut leader = server_refs[idx].clone();
    
    'outer: loop{
        tracing::debug!("sending entries to replicate to server {}", idx);
        output_vec = Vec::with_capacity(entries.len());
        
        // channel to receive the outputs of the state machine.
        let (tx, mut rx) = mpsc::channel::<AppendEntriesClientResponse<SMin, SMout>>(10);
        
        let request = AppendEntriesClientRequest {
            entries_to_replicate: entries.clone(),
            reply_to: tx,
        };
        let _ = leader.try_send(request.into());

        'inner: while output_vec.len() < entries.len() {
            match tokio::time::timeout(request_timeout, rx.recv()).await {
                Ok(Some(AppendEntriesClientResponse(Ok(output)))) => {
                    tracing::debug!("✅ entry has been successfully replicated");
                    output_vec.push(output);
                }
                Ok(Some(AppendEntriesClientResponse(Err(Some(new_leader_ref))))) => {
                    tracing::debug!("this server is not the leader: switching to the provided leader");
                    leader = new_leader_ref;
                    idx = server_refs.iter().position(|x| x == &leader).unwrap();
                    break 'inner;
                }
                Ok(Some(AppendEntriesClientResponse(Err(None)))) | Err(_) | Ok(None)=> {
                    tracing::debug!("this server did not answer or does not know who the leader is: \
                        switching to another server");
                    idx = (idx+1)%server_refs.len();
                    leader = server_refs[idx].clone();
                    break 'inner;
                }
            }
        }
        if output_vec.len() == entries.len() {
            tracing::debug!("all entries have been successfully replicated");
            break 'outer;
        }
        tokio::time::sleep(Duration::from_millis(1000)).await;
    }

    output_vec
}