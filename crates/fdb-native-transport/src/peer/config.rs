//! Configuration of a [`PeerManager`](super::PeerManager).

use std::time::Duration;

use fdb_native_wire::ProtocolVersion;

use super::BackoffConfig;
use crate::connection::{DEFAULT_MAX_PENDING_OUTPUT, MonitorConfig};
use crate::framing::DEFAULT_PACKET_LIMIT;

/// How a [`PeerManager`](super::PeerManager) connects, reconnects and judges its peers. Build
/// one with [`PeerManagerConfig::default`] and adjust the public fields. The defaults are
/// FDB's (see the [module documentation](super#reconnecting) for the knobs).
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct PeerManagerConfig {
    /// The protocol versions this client can speak, most preferred first (it must not be
    /// empty). A connection to an address nothing is known about announces the first;
    /// see [version detection](super#version-detection). Default: 7.4 and 8.0.
    pub supported_versions: Vec<ProtocolVersion>,
    /// How long a TCP connect (and, separately, the TLS handshake) may take:
    /// `CONNECTION_MONITOR_TIMEOUT`, 2 s (`flow/Knobs.cpp` 7.4:109, 8.0:112; used by
    /// `connectionKeeper` at 7.4:867 and 8.0:943). The FlowTransport handshake that follows
    /// is bounded by [`handshake_timeout`](Self::handshake_timeout).
    pub connect_timeout: Duration,
    /// How long the peer has to send its ConnectPacket once connected
    /// ([`ConnectionConfig::handshake_timeout`](crate::ConnectionConfig::handshake_timeout)).
    /// Default 3 s.
    pub handshake_timeout: Duration,
    /// The reconnect backoff.
    pub backoff: BackoffConfig,
    /// `FAILURE_DETECTION_DELAY` (4 s; 7.4:324 and 8.0:323 of `flow/Knobs.cpp`): an address
    /// is marked failed once its connections have failed continuously for longer than this.
    /// It is also how long an *available* peer that failed waits before it is probed again
    /// with nothing to send.
    pub failure_detection_delay: Duration,
    /// `SERVER_REQUEST_INTERVAL` (1 s; 7.4:62 and 8.0:67): how long a peer already marked
    /// failed waits between probes when there is nothing to send.
    pub failed_probe_interval: Duration,
    /// The connection monitor's timing (pings and idle rules).
    pub monitor: MonitorConfig,
    /// [`ConnectionConfig::max_pending_output`](crate::ConnectionConfig::max_pending_output):
    /// how much output may wait to be written before [`send`](super::PeerHandle::send) waits.
    pub max_pending_output: usize,
    /// [`ConnectionConfig::max_packet_len`](crate::ConnectionConfig::max_packet_len).
    pub max_packet_len: u32,
    /// How many commands a peer queues, from its handles, before
    /// [`send`](super::PeerHandle::send) waits for room (and the most it holds while it has no
    /// connection or is applying backpressure). Default 256.
    pub command_queue_len: usize,
    /// How long a shutdown waits to write what is already queued and to close each socket.
    /// Default 1 s.
    pub shutdown_timeout: Duration,
    /// Seed of the random numbers (jitter, connection seeds). `None` draws one.
    pub seed: Option<u64>,
}

impl PeerManagerConfig {
    /// The same configuration with every jitter turned off, for deterministic tests.
    pub fn without_jitter(mut self) -> PeerManagerConfig {
        self.backoff = self.backoff.without_jitter();
        self.monitor = self.monitor.without_jitter();
        self
    }
}

impl Default for PeerManagerConfig {
    fn default() -> PeerManagerConfig {
        PeerManagerConfig {
            supported_versions: vec![ProtocolVersion::V7_4, ProtocolVersion::V8_0],
            connect_timeout: Duration::from_secs(2),
            handshake_timeout: Duration::from_secs(3),
            backoff: BackoffConfig::FDB_DEFAULT,
            failure_detection_delay: Duration::from_secs(4),
            failed_probe_interval: Duration::from_secs(1),
            monitor: MonitorConfig::FDB_DEFAULT,
            max_pending_output: DEFAULT_MAX_PENDING_OUTPUT,
            max_packet_len: DEFAULT_PACKET_LIMIT,
            command_queue_len: 256,
            shutdown_timeout: Duration::from_secs(1),
            seed: None,
        }
    }
}
