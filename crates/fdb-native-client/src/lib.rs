//! Public API of the pure-Rust native FoundationDB client.
//!
//! Built on `fdb-native-schema` and `fdb-native-transport`.
//!
//! * [`cluster_file`]: cluster files and connection strings.
//! * [`cluster`]: the coordinator connection, [`ClusterMonitor`], which keeps the client's
//!   `ClientDBInfo` (its GRV and commit proxies) current.
//! * [`commit`]: the commit path, [`Committer`] and [`CommitBuffer`].

pub mod cluster;
pub mod cluster_file;
pub mod commit;

pub use cluster::{
    ClusterInfo, ClusterInfoWatch, ClusterMonitor, ClusterMonitorConfig, CommitProxyEndpoints,
    ConnectionRecord, GrvProxyEndpoints, MonitorClosed, MonitorError, MonitorStatus,
};

pub use commit::{
    CommitBuffer, CommitError, CommitOptions, CommitOutcome, Committer, CommitterConfig,
};

pub use cluster_file::{
    ClusterConnectionString, ClusterFile, ClusterFileError, ClusterFileLocation, ClusterFileSource,
    ConnectionStringError, Coordinator, ResolvedCoordinator,
};

#[cfg(test)]
mod tests {
    #[test]
    fn scaffold() {}
}
