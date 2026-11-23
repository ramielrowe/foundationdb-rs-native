//! [`ClusterMonitor`]: the coordinator long poll (C++'s `monitorProxies`).

use std::fmt;
use std::io;
use std::sync::{Arc, Mutex, PoisonError};

use fdb_native_schema::domain::{ClientDbInfo, Hostname, OpenDatabaseCoord};
use fdb_native_schema::{Request, SchemaVersion};
use fdb_native_transport::connection::well_known;
use fdb_native_transport::peer::{PeerHandle, PeerManager, PeerRef, StreamWrapper};
use fdb_native_transport::rpc::RequestError;
use fdb_native_transport::runtime::{Runtime, TaskHandle};
use fdb_native_wire::{EncodeError, NetworkAddress, ProtocolVersion, Uid};

use super::config::ClusterMonitorConfig;
use super::error::{MonitorClosed, MonitorError};
use super::info::{ClusterInfo, CommitProxyEndpoints, GrvProxyEndpoints, ProxySampler};
use super::record::ConnectionRecord;
use super::rng::Rng;
use super::watch::{ClusterInfoWatch, Shared};
use crate::cluster_file::{ClusterConnectionString, Coordinator, ResolvedCoordinator};

/// The issue a request carries while the cluster file disagrees with the connection string
/// in memory.
const INCORRECT_CLUSTER_FILE_CONTENTS: &[u8] = b"incorrect_cluster_file_contents";

/// A snapshot of what the monitor is doing, for diagnostics. See [`ClusterMonitor::status`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct MonitorStatus {
    /// The connection string whose coordinators are being asked: the record's, or the one a
    /// coordinator forwarded to.
    pub connection_string: ClusterConnectionString,
    /// The record's connection string (what the cluster file holds, or is to hold). It
    /// differs from [`connection_string`](Self::connection_string) between a forward and
    /// the first reply from the new coordinators.
    pub record_connection_string: ClusterConnectionString,
    /// The coordinator asked last (or being asked).
    pub coordinator: Option<ResolvedCoordinator>,
    /// The generation: 1, plus one for every switch to another connection string.
    pub generation: u64,
    /// Whether a coordinator has answered since the last switch.
    pub has_connected: bool,
    /// How many times asking a coordinator failed, in all.
    pub failures: u64,
    /// Why asking a coordinator failed last.
    pub last_error: Option<MonitorError>,
    /// Why writing the cluster file failed last, if it did (C++ only logs it).
    pub persist_error: Option<String>,
}

/// Keeps a client's `ClientDBInfo` current by long-polling the coordinators, as C++'s
/// `monitorProxies` does, and publishes each one as a [`ClusterInfo`]. See the [module
/// documentation](super) for the protocol.
///
/// The monitor runs on its own task over the runtime. It uses the [`PeerManager`] it is
/// given (shared with the rest of the client) and holds references to the coordinators'
/// peers while it runs. [`shutdown`](ClusterMonitor::shutdown), or dropping it, stops it.
pub struct ClusterMonitor<R: Runtime> {
    shared: Arc<Shared>,
    task: Mutex<Option<R::Task<()>>>,
}

impl<R: Runtime> fmt::Debug for ClusterMonitor<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterMonitor")
            .field("shared", &self.shared)
            .finish_non_exhaustive()
    }
}

impl<R: Runtime> ClusterMonitor<R> {
    /// Starts monitoring the cluster of `record` over `peers`. Call it inside the executor
    /// (a task is spawned on `rt`).
    pub fn start<W>(
        rt: R,
        peers: PeerManager<R, W>,
        record: ConnectionRecord,
        config: ClusterMonitorConfig,
    ) -> ClusterMonitor<R>
    where
        W: StreamWrapper<R::Stream>,
    {
        let status = MonitorStatus {
            connection_string: record.connection_string.clone(),
            record_connection_string: record.connection_string.clone(),
            coordinator: None,
            generation: 1,
            has_connected: false,
            failures: 0,
            last_error: None,
            persist_error: None,
        };
        let shared = Arc::new(Shared::new(status));
        let task = MonitorTask {
            rt: rt.clone(),
            peers,
            shared: shared.clone(),
            rng: Rng::new(config.seed),
            config,
            record,
        };
        let task = rt.spawn(task.run());
        ClusterMonitor {
            shared,
            task: Mutex::new(Some(task)),
        }
    }

    /// The latest info, if a coordinator has sent one.
    pub fn current(&self) -> Option<Arc<ClusterInfo>> {
        self.shared.lock().info.clone()
    }

    /// The schema set of the latest info: the cluster's version.
    pub fn schema_version(&self) -> Option<SchemaVersion> {
        self.shared.lock().info.as_ref().map(|info| info.schema)
    }

    /// A receiver that has seen nothing yet: its first
    /// [`changed`](ClusterInfoWatch::changed) returns the current info, if there is one.
    pub fn watch(&self) -> ClusterInfoWatch {
        ClusterInfoWatch::new(self.shared.clone(), 0)
    }

    /// Waits for the next info published after this call.
    pub async fn changed(&self) -> Result<Arc<ClusterInfo>, MonitorClosed> {
        let seen = self.shared.lock().sequence;
        ClusterInfoWatch::new(self.shared.clone(), seen)
            .changed()
            .await
    }

    /// Resolves with `f`'s first `Some`, evaluated on the current info and on every new one.
    /// `f` runs under the monitor's lock: keep it short, and do not call the monitor from it.
    pub async fn wait_for<T>(
        &self,
        mut f: impl FnMut(&Arc<ClusterInfo>) -> Option<T> + Unpin,
    ) -> Result<T, MonitorClosed> {
        self.shared
            .wait(move |state| state.info.as_ref().and_then(&mut f))
            .await
    }

    /// The current info, or the first one to come.
    pub async fn ready(&self) -> Result<Arc<ClusterInfo>, MonitorClosed> {
        self.wait_for(|info| Some(info.clone())).await
    }

    /// The current info if it has usable GRV and commit proxies, or the first one that does
    /// ([`ClusterInfo::has_proxies`]): what a transaction waits for during a recovery
    /// (`onProxiesChanged`).
    pub async fn proxies(&self, use_provisional: bool) -> Result<Arc<ClusterInfo>, MonitorClosed> {
        self.wait_for(|info| info.has_proxies(use_provisional).then(|| info.clone()))
            .await
    }

    /// What the monitor is doing.
    pub fn status(&self) -> MonitorStatus {
        self.shared.lock().status.clone()
    }

    /// The connection string whose coordinators are being asked (it changes when the cluster
    /// moves).
    pub fn connection_string(&self) -> ClusterConnectionString {
        self.shared.lock().status.connection_string.clone()
    }

    /// Whether the monitor was shut down.
    pub fn is_closed(&self) -> bool {
        self.shared.lock().closed
    }

    /// Stops the monitor and waits for its task to end: the outstanding request is cancelled,
    /// the coordinator references are released, and waiters get [`MonitorClosed`]. The last
    /// info stays readable. Idempotent.
    pub async fn shutdown(&self) {
        let task = self
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(task) = task {
            task.abort();
            // Aborted (or panicked): either way it has ended.
            let _ = task.await;
        }
        self.shared.close();
    }
}

impl<R: Runtime> Drop for ClusterMonitor<R> {
    /// Stops the task without waiting for it.
    fn drop(&mut self) {
        let task = self
            .task
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(task) = task {
            task.abort();
        }
        self.shared.close();
    }
}

/// Closes the shared state when the task ends, however it ends.
struct CloseOnDrop(Arc<Shared>);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// C++'s `MonitorLeaderInfo`: what carries over from one generation to the next. (Its
/// `hasConnected` only chooses a trace event in C++; here it is
/// [`MonitorStatus::has_connected`].)
#[derive(Debug, Default)]
struct GenerationInfo {
    /// `intermediateConnRecord`, when it is not the record: the connection string a
    /// coordinator forwarded to, not yet persisted.
    intermediate: Option<ClusterConnectionString>,
}

/// A decoded reply and the version of the connection it came on.
struct Reply {
    info: ClientDbInfo,
    schema: SchemaVersion,
    version: ProtocolVersion,
}

struct MonitorTask<R: Runtime, W> {
    rt: R,
    peers: PeerManager<R, W>,
    shared: Arc<Shared>,
    config: ClusterMonitorConfig,
    record: ConnectionRecord,
    rng: Rng,
}

impl<R: Runtime, W: StreamWrapper<R::Stream>> MonitorTask<R, W> {
    /// `monitorProxies`.
    async fn run(mut self) {
        let _close = CloseOnDrop(self.shared.clone());
        let mut info = GenerationInfo::default();
        loop {
            info = self.one_generation(info).await;
            let connection_string = info
                .intermediate
                .clone()
                .unwrap_or_else(|| self.record.connection_string.clone());
            let record_connection_string = self.record.connection_string.clone();
            self.update_status(|status| {
                status.generation += 1;
                status.connection_string = connection_string;
                status.record_connection_string = record_connection_string;
                status.has_connected = false;
            });
        }
    }

    /// `monitorProxiesOneGeneration`: asks the coordinators of one connection string until
    /// the cluster moves.
    async fn one_generation(&mut self, mut generation: GenerationInfo) -> GenerationInfo {
        let cs = generation
            .intermediate
            .clone()
            .unwrap_or_else(|| self.record.connection_string.clone());
        let mut coordinators = cs.coordinators().to_vec();
        self.rng.shuffle(&mut coordinators);
        let n = coordinators.len();
        // Like the generation's `ClientLeaderRegInterface`s: the address coordinators now,
        // each hostname's address once resolved.
        let mut references: Vec<Option<PeerRef>> = coordinators
            .iter()
            .map(|c| {
                c.resolved()
                    .map(|r| self.peers.peer(&network_address(r)).reference())
            })
            .collect();
        let mut sampler = ProxySampler::default();
        let mut index = 0;
        let mut success_index = 0;
        let mut all_connections_failed = false;

        loop {
            let known = self
                .shared
                .lock()
                .info
                .as_ref()
                .map_or(Uid::default(), |info| info.id());
            let mut request = self.request(&cs, known);

            if let Some(stored) = self.record.check_file() {
                match stored {
                    Some(stored) if all_connections_failed => {
                        // Every coordinator failed and the file says something else: someone
                        // else may have followed the cluster to new coordinators.
                        let result = self.record.set_and_persist(stored);
                        self.note_persist(result);
                        generation.intermediate = None;
                        return generation;
                    }
                    _ => request
                        .issues
                        .push(INCORRECT_CLUSTER_FILE_CONTENTS.to_vec()),
                }
            }

            let result = match self.resolve(&coordinators[index]).await {
                Err(error) => Err(error),
                Ok(coordinator) => {
                    let peer = self.peers.peer(&network_address(coordinator));
                    if coordinators[index].is_host() {
                        references[index] = Some(peer.reference());
                    }
                    self.update_status(|status| status.coordinator = Some(coordinator));
                    match open_database(&peer, request).await {
                        Err(error) => Err(MonitorError::Request { coordinator, error }),
                        Ok(reply) => match &reply.info.forward {
                            None => Ok((coordinator, reply)),
                            Some(forward) => match ClusterConnectionString::parse_bytes(forward) {
                                Ok(forwarded) => {
                                    // The cluster moved: the next generation asks the new
                                    // coordinators, and the file is rewritten once one answers.
                                    generation.intermediate = Some(forwarded);
                                    return generation;
                                }
                                Err(error) => Err(MonitorError::InvalidForward {
                                    coordinator,
                                    forward: forward.clone(),
                                    error,
                                }),
                            },
                        },
                    }
                }
            };

            match result {
                Ok((coordinator, reply)) => {
                    if let Some(forwarded) = generation.intermediate.take() {
                        let result = self.record.set_and_persist(forwarded);
                        self.note_persist(result);
                    }
                    let result = self.record.notify_connected();
                    self.note_persist(result);

                    let (grv, commit) = sampler.sample(
                        &reply.info,
                        self.config.max_grv_proxy_connections,
                        self.config.max_commit_proxy_connections,
                        &mut self.rng,
                    );
                    let record_connection_string = self.record.connection_string.clone();
                    self.update_status(|status| {
                        status.has_connected = true;
                        status.record_connection_string = record_connection_string;
                    });
                    self.shared.publish(ClusterInfo {
                        client_info: reply.info,
                        schema: reply.schema,
                        protocol_version: reply.version,
                        coordinator,
                        connection_string: cs.clone(),
                        grv_proxies: grv.into_iter().map(GrvProxyEndpoints::new).collect(),
                        commit_proxies: commit.into_iter().map(CommitProxyEndpoints::new).collect(),
                        sequence: 0,
                    });
                    success_index = index;
                    all_connections_failed = false;
                }
                Err(error) => {
                    self.update_status(|status| {
                        status.failures += 1;
                        status.last_error = Some(error);
                    });
                    index = (index + 1) % n;
                    if index == success_index {
                        all_connections_failed = true;
                        self.rt
                            .sleep(self.config.coordinator_reconnection_delay)
                            .await;
                    }
                }
            }
        }
    }

    /// The `OpenDatabaseCoordRequest` for the coordinators of `cs`.
    fn request(&self, cs: &ClusterConnectionString, known: Uid) -> OpenDatabaseCoord {
        let mut coordinators = Vec::new();
        let mut hostnames = Vec::new();
        for c in cs.coordinators() {
            match c {
                Coordinator::Addr { .. } => {
                    coordinators.extend(c.resolved().map(network_address));
                }
                Coordinator::Host { name, port, tls } => hostnames.push(Hostname {
                    host: name.as_bytes().to_vec(),
                    service: port.to_string().into_bytes(),
                    is_tls: *tls,
                }),
            }
        }
        OpenDatabaseCoord {
            issues: self.config.issues.clone(),
            supported_versions: self.config.supported_versions.clone(),
            trace_log_group: self.config.trace_log_group.clone(),
            known_client_info_id: known,
            cluster_key: cs.key().into_bytes(),
            coordinators,
            hostnames,
            internal: self.config.internal,
        }
    }

    /// `Hostname::resolve` for a hostname coordinator (one of its addresses, at random); the
    /// address of an address coordinator.
    async fn resolve(
        &mut self,
        coordinator: &Coordinator,
    ) -> Result<ResolvedCoordinator, MonitorError> {
        let Some((host, port)) = coordinator.host() else {
            return Ok(coordinator
                .resolved()
                .expect("an address coordinator has an address"));
        };
        let error = match self.rt.resolve(host, port).await {
            Ok(addresses) if !addresses.is_empty() => {
                let pick = addresses[self.rng.below(addresses.len())];
                return Ok(coordinator.with_resolved(pick));
            }
            Ok(_) => "no addresses".to_owned(),
            Err(e) => e.to_string(),
        };
        Err(MonitorError::Resolve {
            host: host.to_owned(),
            port,
            error,
        })
    }

    fn note_persist(&self, result: io::Result<()>) {
        if let Err(e) = result {
            let path = self
                .record
                .path()
                .map(|p| p.display().to_string())
                .unwrap_or_default();
            self.update_status(|status| {
                status.persist_error = Some(format!("cannot write cluster file `{path}`: {e}"));
            });
        }
    }

    /// Changes the status, which nothing waits on.
    fn update_status(&self, f: impl FnOnce(&mut MonitorStatus)) {
        f(&mut self.shared.lock().status);
    }
}

/// The transport's address for a resolved coordinator: public, its TLS flag, and
/// `fromHostname` when it came from a hostname.
pub(crate) fn network_address(coordinator: ResolvedCoordinator) -> NetworkAddress {
    let mut address = NetworkAddress::new(
        coordinator.addr.ip().into(),
        coordinator.addr.port(),
        true,
        coordinator.tls,
    );
    address.from_hostname = coordinator.from_hostname;
    address
}

/// One `tryGetReply` of an `OpenDatabaseCoordRequest` to the coordinator at `peer`, encoded
/// and decoded with the schema set of the connection it goes out on.
async fn open_database(
    peer: &PeerHandle,
    request: OpenDatabaseCoord,
) -> Result<Reply, RequestError> {
    // The version the request was encoded for is the version of the connection it was sent
    // on, which is the one its reply comes on.
    let negotiated = Arc::new(Mutex::new(None));
    let slot = negotiated.clone();
    let payload = peer
        .request(
            well_known::CLIENTLEADERREG_OPENDATABASE,
            move |reply, version, out| {
                let schema = SchemaVersion::for_protocol_version(version).ok_or(
                    EncodeError::Invalid("no schema set for the coordinator's protocol version"),
                )?;
                *slot.lock().unwrap_or_else(PoisonError::into_inner) = Some((schema, version));
                request.encode_request_into(schema, reply, out)
            },
        )
        .await?;
    let (schema, version) = negotiated
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take()
        // A reply cannot come for a request that was never encoded.
        .ok_or(RequestError::MaybeDelivered(None))?;
    match OpenDatabaseCoord::decode_reply(schema, &payload) {
        Ok(Ok(info)) => Ok(Reply {
            info,
            schema,
            version,
        }),
        Ok(Err(error)) => Err(RequestError::from_reply_error(error)),
        Err(error) => Err(RequestError::Decode(error)),
    }
}
