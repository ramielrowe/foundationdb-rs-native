//! Errors of the cluster monitor.

use crate::cluster_file::{ConnectionStringError, ResolvedCoordinator};
use fdb_native_transport::rpc::RequestError;

/// Why asking one coordinator failed. The monitor then asks the next one (C++ moves on for
/// any error); the latest is in [`MonitorStatus::last_error`](super::MonitorStatus).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum MonitorError {
    /// The request failed: the connection could not be made or broke
    /// (`request_maybe_delivered`), the coordinator replied with an error
    /// (`failed_to_progress`, `wrong_connection_file`, or `default_error_or` when it has no
    /// cluster to report yet), or the reply could not be decoded. `coordinator` is the
    /// address that was asked.
    #[error("coordinator {coordinator}: {error}")]
    Request {
        /// The coordinator's address (resolved, for a hostname).
        coordinator: ResolvedCoordinator,
        /// What went wrong.
        error: RequestError,
    },
    /// A hostname coordinator did not resolve to any address (`lookup_failed`).
    #[error("coordinator {host}:{port} did not resolve: {error}")]
    Resolve {
        /// The host name.
        host: String,
        /// The port.
        port: u16,
        /// The resolver's error, or that it returned no address.
        error: String,
    },
    /// A coordinator said the cluster moved (`forward`), to something that is not a valid
    /// connection string. C++ would stop monitoring altogether.
    #[error("coordinator {coordinator} forwarded to an invalid connection string: {error}")]
    InvalidForward {
        /// The coordinator.
        coordinator: ResolvedCoordinator,
        /// The `forward` value.
        forward: Vec<u8>,
        /// Why it does not parse.
        error: ConnectionStringError,
    },
}

/// The [`ClusterMonitor`](super::ClusterMonitor) was shut down (or dropped): no more
/// `ClientDBInfo` will come.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("the cluster monitor was shut down")]
pub struct MonitorClosed;
