//! Configuration of a [`Connection`](super::Connection), with FDB's defaults.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::time::Duration;

use crate::ConnectPacket;
use crate::framing::DEFAULT_PACKET_LIMIT;

/// The connection monitor's timing: FlowTransport's `connectionMonitor` knobs
/// (`flow/Knobs.cpp`). The defaults are the non-simulation values, identical on 7.4 and 8.0:
///
/// | field | knob | default | 7.4 | 8.0 |
/// | --- | --- | --- | --- | --- |
/// | [`loop_time`](Self::loop_time) | `CONNECTION_MONITOR_LOOP_TIME` | 1.0 s | 108 | 111 |
/// | [`timeout`](Self::timeout) | `CONNECTION_MONITOR_TIMEOUT` | 2.0 s | 109 | 112 |
/// | [`idle_timeout`](Self::idle_timeout) | `CONNECTION_MONITOR_IDLE_TIMEOUT` | 180 s | 110 | 113 |
/// | [`unreferenced_close_delay`](Self::unreferenced_close_delay) | `CONNECTION_MONITOR_UNREFERENCED_CLOSE_DELAY` | 2.0 s | 112 | 115 |
/// | [`jitter_offset`](Self::jitter_offset) | `DELAY_JITTER_OFFSET` | 0.9 | 58 | 63 |
/// | [`jitter_range`](Self::jitter_range) | `DELAY_JITTER_RANGE` | 0.2 | 59 | 64 |
///
/// (`CONNECTION_MONITOR_INCOMING_IDLE_MULTIPLIER`, 1.2, applies only to a server watching an
/// incoming client connection, so a client has no use for it.) The jitter is
/// `delayJittered`'s: a delay `d` becomes `d * (offset + range * random01())`.
#[derive(Clone, Copy, Debug, PartialEq)]
#[non_exhaustive]
pub struct MonitorConfig {
    /// How long after a ping is answered the next one is sent (jittered).
    pub loop_time: Duration,
    /// How long a ping waits for bytes from the peer before the connection is declared failed;
    /// any bytes at all extend the wait by another `timeout`.
    pub timeout: Duration,
    /// How long a connection with nothing outstanding may go without sending data (and since it
    /// was opened) before it is closed as idle.
    pub idle_timeout: Duration,
    /// How long an unreferenced connection with nothing outstanding stays open after it last
    /// sent data.
    pub unreferenced_close_delay: Duration,
    /// `DELAY_JITTER_OFFSET`.
    pub jitter_offset: f64,
    /// `DELAY_JITTER_RANGE`. 0 turns jitter off.
    pub jitter_range: f64,
}

impl MonitorConfig {
    /// FDB's defaults.
    pub const FDB_DEFAULT: MonitorConfig = MonitorConfig {
        loop_time: Duration::from_secs(1),
        timeout: Duration::from_secs(2),
        idle_timeout: Duration::from_secs(180),
        unreferenced_close_delay: Duration::from_secs(2),
        jitter_offset: 0.9,
        jitter_range: 0.2,
    };

    /// The same timing without jitter: every delay is exact (for tests).
    pub const fn without_jitter(self) -> MonitorConfig {
        MonitorConfig {
            jitter_offset: 1.0,
            jitter_range: 0.0,
            ..self
        }
    }
}

impl Default for MonitorConfig {
    fn default() -> MonitorConfig {
        MonitorConfig::FDB_DEFAULT
    }
}

/// How a [`Connection`](super::Connection) behaves.
///
/// Build one with [`ConnectionConfig::new`] and adjust the public fields.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ConnectionConfig {
    /// The ConnectPacket this side sends. Its protocol version is the version announced, and
    /// the only one the peer's must be [compatible](fdb_native_wire::ProtocolVersion::is_compatible)
    /// with. Use [`ConnectPacket::multi_version_client`] with one process-wide id (see the
    /// [module documentation](super#connection-ids)).
    pub local: ConnectPacket,
    /// Whether the connection is TLS: frames then carry no checksum
    /// ([`Checksum::for_tls`](crate::Checksum::for_tls)). It must be the TLS flag of the
    /// address dialed.
    pub tls: bool,
    /// The port dialed. The peer's ConnectPacket must announce it, as FDB asserts for an
    /// outgoing connection (`ASSERT(pkt.canonicalRemotePort == peerAddress.port)`); `None`
    /// skips the check.
    pub dialed_port: Option<u16>,
    /// `PACKET_LIMIT`: the largest frame (token and payload) accepted or sent.
    pub max_packet_len: u32,
    /// How long the peer has to send its whole ConnectPacket, from
    /// [`Connection::new`](super::Connection::new). The default, 3 s, is
    /// `CONNECTION_MONITOR_LOOP_TIME + CONNECTION_MONITOR_TIMEOUT`: the longest FDB's own
    /// client waits for a silent peer (its monitor sends the first ping about one loop time
    /// after connecting and fails the connection if nothing arrives within the timeout).
    pub handshake_timeout: Duration,
    /// The connection monitor's timing.
    pub monitor: MonitorConfig,
    /// Seed of the connection's random numbers (the jitter and the reply tokens of its pings).
    /// [`ConnectionConfig::new`] draws one; set it for a reproducible run.
    pub seed: u64,
}

impl ConnectionConfig {
    /// A connection that sends `local` first, over TLS or not, with FDB's defaults and a random
    /// seed.
    pub fn new(local: ConnectPacket, tls: bool) -> ConnectionConfig {
        ConnectionConfig {
            local,
            tls,
            dialed_port: None,
            max_packet_len: DEFAULT_PACKET_LIMIT,
            handshake_timeout: Duration::from_secs(3),
            monitor: MonitorConfig::FDB_DEFAULT,
            seed: RandomState::new().build_hasher().finish(),
        }
    }
}
