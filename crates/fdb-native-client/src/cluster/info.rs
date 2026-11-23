//! What the monitor publishes: a `ClientDBInfo` with the proxies to use and their endpoints.

use fdb_native_schema::SchemaVersion;
use fdb_native_schema::domain::{ClientDbInfo, CommitProxy, GrvProxy};
use fdb_native_schema::streams::{CommitProxyStreams, GrvProxyStreams};
use fdb_native_wire::{Endpoint, ProtocolVersion, Uid};

use super::rng::Rng;
use crate::cluster_file::{ClusterConnectionString, ResolvedCoordinator};

/// A GRV proxy the client uses, with the endpoints of all its streams.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrvProxyEndpoints {
    /// The proxy as `ClientDBInfo` lists it.
    pub proxy: GrvProxy,
    /// Its streams ([`GrvProxy::endpoints`]); `get_consistent_read_version` takes
    /// `GetReadVersionRequest`.
    pub streams: GrvProxyStreams<Endpoint>,
}

impl GrvProxyEndpoints {
    /// The proxy and its derived endpoints.
    pub fn new(proxy: GrvProxy) -> GrvProxyEndpoints {
        let streams = proxy.endpoints();
        GrvProxyEndpoints { proxy, streams }
    }
}

/// A commit proxy the client uses, with the endpoints of all its streams, at the indices of
/// the version the proxy was decoded with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitProxyEndpoints {
    /// The proxy as `ClientDBInfo` lists it.
    pub proxy: CommitProxy,
    /// Its streams ([`CommitProxy::endpoints`]); `commit` takes `CommitTransactionRequest`,
    /// `get_key_servers_locations` `GetKeyServerLocationsRequest`.
    pub streams: CommitProxyStreams<Endpoint>,
}

impl CommitProxyEndpoints {
    /// The proxy and its derived endpoints.
    pub fn new(proxy: CommitProxy) -> CommitProxyEndpoints {
        let streams = proxy.endpoints();
        CommitProxyEndpoints { proxy, streams }
    }
}

/// One `ClientDBInfo` from a coordinator, as a [`ClusterMonitor`](super::ClusterMonitor)
/// publishes it.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ClusterInfo {
    /// The info exactly as the coordinator sent it (all proxies, not only the sample in
    /// [`grv_proxies`](Self::grv_proxies) and [`commit_proxies`](Self::commit_proxies)).
    /// `client_info.commit_proxies[0]` is C++'s `firstCommitProxy`.
    pub client_info: ClientDbInfo,
    /// The schema set the info was decoded with: the cluster's version. Requests to the
    /// proxies are encoded with the schema set of the connection they go out on, which is
    /// normally this one.
    pub schema: SchemaVersion,
    /// The protocol version of the connection the info came on.
    pub protocol_version: ProtocolVersion,
    /// The coordinator that sent it.
    pub coordinator: ResolvedCoordinator,
    /// The connection string whose coordinators are being asked.
    pub connection_string: ClusterConnectionString,
    /// The GRV proxies to use: all of them, or a random sample of
    /// [`max_grv_proxy_connections`](super::ClusterMonitorConfig::max_grv_proxy_connections)
    /// kept while the proxy set is unchanged (`shrinkProxyList`).
    pub grv_proxies: Vec<GrvProxyEndpoints>,
    /// The commit proxies to use, sampled the same way with
    /// [`max_commit_proxy_connections`](super::ClusterMonitorConfig::max_commit_proxy_connections).
    pub commit_proxies: Vec<CommitProxyEndpoints>,
    /// Counts the infos the monitor has published, from 1. Two replies carrying the same
    /// info (the coordinator re-sends it every `CLIENT_REGISTER_INTERVAL`) have different
    /// sequence numbers and the same [`id`](Self::id).
    pub sequence: u64,
}

impl ClusterInfo {
    /// The info's id. Infos with the same id are the same (C++'s `ClientDBInfo::operator==`).
    pub fn id(&self) -> Uid {
        self.client_info.id
    }

    /// Whether the proxies are provisional: recruited by a recovery that has not finished.
    /// C++ (`DatabaseContext::updateProxies`) looks at the first proxy of each list, and
    /// asserts that they agree; this is true if either is provisional.
    pub fn is_provisional(&self) -> bool {
        self.client_info
            .commit_proxies
            .first()
            .is_some_and(|p| p.provisional)
            || self
                .client_info
                .grv_proxies
                .first()
                .is_some_and(|p| p.provisional)
    }

    /// The GRV proxies a transaction may use (`getGrvProxies`): `None` while there are none
    /// (a recovery), or while they are provisional unless `use_provisional`
    /// (`USE_PROVISIONAL_PROXIES`).
    pub fn available_grv_proxies(&self, use_provisional: bool) -> Option<&[GrvProxyEndpoints]> {
        if self.grv_proxies.is_empty() || (self.is_provisional() && !use_provisional) {
            return None;
        }
        Some(&self.grv_proxies)
    }

    /// The commit proxies a transaction may use (`getCommitProxies`), with the same rule as
    /// [`available_grv_proxies`](Self::available_grv_proxies).
    pub fn available_commit_proxies(
        &self,
        use_provisional: bool,
    ) -> Option<&[CommitProxyEndpoints]> {
        if self.commit_proxies.is_empty() || (self.is_provisional() && !use_provisional) {
            return None;
        }
        Some(&self.commit_proxies)
    }

    /// The ids of the GRV proxies the client uses, in order.
    pub fn grv_proxy_ids(&self) -> Vec<Uid> {
        self.grv_proxies.iter().map(|p| p.proxy.id()).collect()
    }

    /// The ids of the commit proxies the client uses, in order.
    pub fn commit_proxy_ids(&self) -> Vec<Uid> {
        self.commit_proxies.iter().map(|p| p.proxy.id()).collect()
    }

    /// The first commit proxy of the full list the coordinator sent, with its endpoints: C++'s
    /// `firstCommitProxy` (kept when `shrinkProxyList` sampled the list), which
    /// `commitOnFirstProxy` transactions commit to. It need not be one of
    /// [`commit_proxies`](Self::commit_proxies). `None` while there are no commit proxies.
    pub fn first_commit_proxy(&self) -> Option<CommitProxyEndpoints> {
        self.client_info
            .commit_proxies
            .first()
            .cloned()
            .map(CommitProxyEndpoints::new)
    }

    /// Whether both proxy kinds are available.
    pub fn has_proxies(&self, use_provisional: bool) -> bool {
        self.available_grv_proxies(use_provisional).is_some()
            && self.available_commit_proxies(use_provisional).is_some()
    }
}

/// `shrinkProxyList`'s cache, kept for one generation.
#[derive(Debug, Default)]
pub(crate) struct ProxySampler {
    commit_ids: Vec<Uid>,
    commit: Vec<CommitProxy>,
    grv_ids: Vec<Uid>,
    grv: Vec<GrvProxy>,
}

impl ProxySampler {
    /// Whether no sample is cached (for tests).
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.commit_ids.is_empty()
            && self.commit.is_empty()
            && self.grv_ids.is_empty()
            && self.grv.is_empty()
    }

    /// The proxies to use from `info`.
    pub(crate) fn sample(
        &mut self,
        info: &ClientDbInfo,
        max_grv: usize,
        max_commit: usize,
        rng: &mut Rng,
    ) -> (Vec<GrvProxy>, Vec<CommitProxy>) {
        let commit = shrink(
            &info.commit_proxies,
            CommitProxy::id,
            max_commit,
            &mut self.commit_ids,
            &mut self.commit,
            rng,
        );
        let grv = shrink(
            &info.grv_proxies,
            GrvProxy::id,
            max_grv,
            &mut self.grv_ids,
            &mut self.grv,
            rng,
        );
        (grv, commit)
    }
}

fn shrink<T: Clone>(
    proxies: &[T],
    id: impl Fn(&T) -> Uid,
    max: usize,
    last_ids: &mut Vec<Uid>,
    last: &mut Vec<T>,
    rng: &mut Rng,
) -> Vec<T> {
    if proxies.len() <= max {
        // SHRINK_PROXY_LIST_CLEAR_CACHE_BELOW_THRESHOLD (8.0's default).
        last_ids.clear();
        last.clear();
        return proxies.to_vec();
    }
    let ids: Vec<Uid> = proxies.iter().map(id).collect();
    if ids != *last_ids {
        *last_ids = ids;
        *last = proxies.to_vec();
        rng.shuffle(last);
        last.truncate(max);
    }
    last.clone()
}
