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
    /// How long dialing may take, TCP connect **and** TLS handshake together:
    /// `CONNECTION_MONITOR_TIMEOUT`, 2 s (`flow/Knobs.cpp` 7.4:109, 8.0:112), which bounds
    /// `INetworkConnections::connect(...)` with a TLS wrapper's handshake included
    /// (`connectionKeeper` at 7.4:867 and 8.0:943). The FlowTransport handshake that follows
    /// is bounded by [`handshake_timeout`](Self::handshake_timeout).
    pub connect_timeout: Duration,
    /// How long the peer has to send its ConnectPacket once connected
    /// ([`ConnectionConfig::handshake_timeout`](crate::ConnectionConfig::handshake_timeout)).
    /// Default 3 s.
    pub handshake_timeout: Duration,
    /// The reconnect backoff.
    pub backoff: BackoffConfig,
    /// `TLS_CLIENT_CONNECTION_THROTTLE_TIMEOUT` (11 s; `flow/Knobs.cpp` 7.4:137, 8.0:140): the
    /// window, from a first failed TLS handshake with an address, in which later failures
    /// are counted. See [`tls_throttle_attempts`](Self::tls_throttle_attempts).
    pub tls_throttle_timeout: Duration,
    /// `TLS_CLIENT_CONNECTION_THROTTLE_ATTEMPTS` (1; 7.4:139, 8.0:142): once this many TLS
    /// handshakes with an address have failed *after the first* within
    /// [`tls_throttle_timeout`](Self::tls_throttle_timeout) (so by default, from the second
    /// failure), a connect to it does not dial for the rest of the window: it waits
    /// [`connect_timeout`](Self::connect_timeout) and fails with
    /// [`PeerError::TlsThrottled`](super::PeerError::TlsThrottled), as a C++ client does
    /// (see the [module documentation](super#tls-handshake-throttle)).
    pub tls_throttle_attempts: u32,
    /// `FAILURE_DETECTION_DELAY` (4 s; 7.4:324 and 8.0:323 of `flow/Knobs.cpp`): an address
    /// is marked failed once its connections have failed continuously for longer than this.
    /// It is also how long an *available* peer that failed waits before it is probed again
    /// with nothing to send.
    pub failure_detection_delay: Duration,
    /// `SERVER_REQUEST_INTERVAL` (1 s; 7.4:62 and 8.0:67): how long a peer already marked
    /// failed waits between probes when there is nothing to send.
    pub failed_probe_interval: Duration,
    /// `PERSISTENT_CONNECT_FAILED_COUNT_TTL` (600 s; 7.4:56 and 8.0:61): how long an address
    /// that stopped failing to connect keeps its count in
    /// [`FailureMonitor::connect_failures`](super::FailureMonitor::connect_failures) (the
    /// forgetting happens on a later failure, as in C++). Zero keeps every count.
    pub connect_failed_count_ttl: Duration,
    /// The connection monitor's timing (pings and idle rules).
    pub monitor: MonitorConfig,
    /// [`ConnectionConfig::max_pending_output`](crate::ConnectionConfig::max_pending_output):
    /// how much output may wait to be written before queued commands wait for the socket.
    pub max_pending_output: usize,
    /// [`ConnectionConfig::max_packet_len`](crate::ConnectionConfig::max_packet_len).
    pub max_packet_len: u32,
    /// How many commands (requests, one-way sends, registrations) a peer queues, from all its
    /// handles together, before the next waits for room: the queue's own count, so abandoned
    /// calls free their place when dropped. The task takes one command at a time and applies
    /// it at once, so this is the whole bound. Default 256.
    pub command_queue_len: usize,
    /// Whether this client prefers the TLS address of an endpoint that has both
    /// ([`choose_primary_address`](super::choose_primary_address)). **Default `false`, as in
    /// C++**: a `libfdb_c` client never sets a local address (the local address stays
    /// `NetworkAddress()`, which is not TLS), so `Endpoint::choosePrimaryAddress`
    /// (`FlowTransport.h` 7.4:169-176, 8.0:178-185) always swaps a TLS primary for the plain
    /// secondary, even in a client configured for TLS. An endpoint with only a TLS address is
    /// connected to over TLS either way. Set it to prefer the TLS address (this process's
    /// TLS-ness being the stream wrapper's), a deliberate deviation for a cluster whose plain
    /// listeners are unreachable or untrusted.
    pub prefer_tls: bool,
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
            tls_throttle_timeout: Duration::from_secs(11),
            tls_throttle_attempts: 1,
            failure_detection_delay: Duration::from_secs(4),
            failed_probe_interval: Duration::from_secs(1),
            connect_failed_count_ttl: Duration::from_secs(600),
            monitor: MonitorConfig::FDB_DEFAULT,
            max_pending_output: DEFAULT_MAX_PENDING_OUTPUT,
            max_packet_len: DEFAULT_PACKET_LIMIT,
            command_queue_len: 256,
            prefer_tls: false,
            shutdown_timeout: Duration::from_secs(1),
            seed: None,
        }
    }
}
