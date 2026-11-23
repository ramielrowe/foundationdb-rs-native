//! Configuration of a [`ClusterMonitor`](super::ClusterMonitor).

use std::time::Duration;

use fdb_native_schema::SchemaVersion;
use fdb_native_schema::domain::ClientVersion;

/// How a [`ClusterMonitor`](super::ClusterMonitor) talks to the coordinators. Build one with
/// [`ClusterMonitorConfig::default`] and adjust the public fields. The defaults are C++'s
/// knobs and `fdb_c`'s request fields (see the [module documentation](super)).
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ClusterMonitorConfig {
    /// `COORDINATOR_RECONNECTION_DELAY` (1 s): how long to wait once every coordinator has
    /// failed in turn, before asking them again.
    pub coordinator_reconnection_delay: Duration,
    /// `MAX_GRV_PROXY_CONNECTIONS` (3): how many GRV proxies the client uses at most.
    pub max_grv_proxy_connections: usize,
    /// `MAX_COMMIT_PROXY_CONNECTIONS` (5): how many commit proxies the client uses at most.
    pub max_commit_proxy_connections: usize,
    /// `supportedVersions`: the client versions reported to the cluster, shown in `status`.
    /// The default, [`ClusterMonitorConfig::default_supported_versions`], has one entry per
    /// schema set. A cluster records the client only if this is non-empty.
    pub supported_versions: Vec<ClientVersion>,
    /// `traceLogGroup`, shown in `status` (default `"default"`, as `fdb_c`'s).
    pub trace_log_group: Vec<u8>,
    /// `issues` to report in `status`, besides `incorrect_cluster_file_contents`, which the
    /// monitor adds itself.
    pub issues: Vec<Vec<u8>>,
    /// `internal`: whether this client is one of the cluster's own processes (`false`).
    pub internal: bool,
    /// Seed of the random numbers (coordinator shuffle, hostname address choice, proxy
    /// sampling). `None` draws one.
    pub seed: Option<u64>,
}

impl ClusterMonitorConfig {
    /// What this client reports as its versions: for each schema set, `client_version`
    /// `"<release>.0 (fdb-native-client <crate version>)"`, `source_version`
    /// `"fdb-native-client"`, and the release's protocol version in lowercase hex without
    /// `0x`, the way `fdb_c` formats it (`%llx`).
    pub fn default_supported_versions() -> Vec<ClientVersion> {
        SchemaVersion::ALL
            .iter()
            .map(|schema| ClientVersion {
                client_version: format!(
                    "{schema}.0 (fdb-native-client {})",
                    env!("CARGO_PKG_VERSION")
                )
                .into_bytes(),
                source_version: b"fdb-native-client".to_vec(),
                protocol_version: format!("{:x}", schema.protocol_version().0).into_bytes(),
            })
            .collect()
    }
}

impl Default for ClusterMonitorConfig {
    fn default() -> Self {
        ClusterMonitorConfig {
            coordinator_reconnection_delay: Duration::from_secs(1),
            max_grv_proxy_connections: 3,
            max_commit_proxy_connections: 5,
            supported_versions: ClusterMonitorConfig::default_supported_versions(),
            trace_log_group: b"default".to_vec(),
            issues: Vec::new(),
            internal: false,
            seed: None,
        }
    }
}
