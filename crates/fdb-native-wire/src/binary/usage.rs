//! The binary-format values a client of FDB 7.4 or 8.0 meets: the types behind the "needed"
//! rows of the [usage table](crate::binary#where-the-protocol-uses-the-binary-format).

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use super::error::{BinaryDecodeError, BinaryEncodeError};
use super::reader::{BinaryReader, from_slice};
use super::traits::{BinaryDeserialize, BinarySerialize};
use super::version::Versioning;
use super::writer::{BinaryWriter, to_vec};
use crate::{ProtocolVersion, Uid};

// ---------------------------------------------------------------------------------------
// The connection handshake.

/// FlowTransport's `ConnectPacket` (`fdbrpc/FlowTransport.actor.cpp`; `FlowTransport.cpp` in
/// 8.0): the first bytes each side sends on a new connection, written with a `PacketWriter`
/// (the binary format, unversioned) and read with a `BinaryReader`.
///
/// ```text
/// u32 connectPacketLength    bytes that follow (40)
/// u64 protocolVersion        the sender's version, with the object-serializer flag
/// u16 canonicalRemotePort    where the sender listens (0 for a client)
/// u64 connectionId           shared by a multi-version client's connections (else 0 or 1)
/// u32 canonicalRemoteIp4     IPv4 address, host order (the 4 bytes read as a u32, LE)
/// u16 flags                  bit 0: the address is IPv6
/// u8  canonicalRemoteIp6[16] IPv6 address, network order
/// ```
///
/// A receiver rejects a length above 40; the server reads 14 bytes first (the V0 packet) to
/// learn the length, so a sender must send at least that much. This type reads and writes
/// the 44-byte form that 7.4 and 8.0 send.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConnectPacket {
    /// The sender's protocol version, which has the object-serializer flag set.
    pub protocol_version: ProtocolVersion,
    /// The port the sender listens on, or 0 (a client).
    pub canonical_remote_port: u16,
    /// `connectionId`: nonzero ids tie a multi-version client's connections together.
    pub connection_id: u64,
    /// `canonicalRemoteIp4`: the IPv4 address as an integer (`10.0.0.1` is `0x0a000001`).
    pub canonical_remote_ip4: u32,
    /// `flags`: [`ConnectPacket::FLAG_IPV6`] or 0.
    pub flags: u16,
    /// `canonicalRemoteIp6`: the IPv6 address, valid if the IPv6 flag is set.
    pub canonical_remote_ip6: [u8; 16],
}

impl ConnectPacket {
    /// `connectPacketLength` of the packets this crate reads and writes: the 40 bytes after
    /// the length.
    pub const LENGTH: u32 = 40;
    /// The whole packet, length included.
    pub const SIZE: usize = 44;
    /// `FLAG_IPV6`.
    pub const FLAG_IPV6: u16 = 1;

    /// A client's packet: no listening address (`canonicalRemotePort` 0 and address 0, as
    /// a "mixed" TLS/non-TLS connection sends), the object-serializer flag added to
    /// `version`, and the given connection id.
    pub fn client(version: ProtocolVersion, connection_id: u64) -> ConnectPacket {
        ConnectPacket {
            protocol_version: version.with_object_serializer_flag(),
            canonical_remote_port: 0,
            connection_id,
            canonical_remote_ip4: 0,
            flags: 0,
            canonical_remote_ip6: [0; 16],
        }
    }

    /// The address the sender listens on, if it says: `None` when the port is 0.
    pub fn canonical_remote_address(&self) -> Option<(IpAddr, u16)> {
        if self.canonical_remote_port == 0 {
            return None;
        }
        let ip = if self.flags & Self::FLAG_IPV6 != 0 {
            IpAddr::V6(Ipv6Addr::from(self.canonical_remote_ip6))
        } else {
            IpAddr::V4(Ipv4Addr::from(self.canonical_remote_ip4))
        };
        Some((ip, self.canonical_remote_port))
    }

    /// The packet's 44 bytes (`prependConnectPacket`).
    pub fn encode(&self) -> Result<Vec<u8>, BinaryEncodeError> {
        let mut w = BinaryWriter::unversioned();
        w.write(&Self::LENGTH)?;
        w.write(&self.protocol_version)?;
        w.write(&self.canonical_remote_port)?;
        w.write(&self.connection_id)?;
        w.write(&self.canonical_remote_ip4)?;
        w.write(&self.flags)?;
        w.write(&self.canonical_remote_ip6)?;
        Ok(w.into_bytes())
    }

    /// Reads a packet from the start of `bytes` and returns it with the number of bytes it
    /// took, so a caller reading a stream can continue after it. Fails with
    /// [`UnexpectedEof`](BinaryDecodeError::UnexpectedEof) if `bytes` ends before the
    /// packet does (read more and retry), and with [`Invalid`](BinaryDecodeError::Invalid)
    /// for a length that is not 40 (FDB rejects a longer one with `serialization_failed`;
    /// a shorter one cannot hold the fields).
    pub fn decode(bytes: &[u8]) -> Result<(ConnectPacket, usize), BinaryDecodeError> {
        let mut r = BinaryReader::unversioned(bytes);
        let length = r.read_u32()?;
        if length != Self::LENGTH {
            return Err(BinaryDecodeError::Invalid(
                "connect packet length is not 40",
            ));
        }
        let packet = ConnectPacket {
            protocol_version: r.read()?,
            canonical_remote_port: r.read()?,
            connection_id: r.read()?,
            canonical_remote_ip4: r.read()?,
            flags: r.read()?,
            canonical_remote_ip6: r.read()?,
        };
        Ok((packet, r.position()))
    }
}

// ---------------------------------------------------------------------------------------
// Transaction conflict keys.

/// The prefix of the key `Transaction::makeSelfConflicting` and the idempotency-id commit
/// path add as a read and write conflict range.
pub const SELF_CONFLICT_PREFIX: &[u8] = b"\xff/SC/";

/// The key `Transaction::makeSelfConflicting` makes a transaction conflict with itself on
/// (so that a transaction with no intersecting read and write ranges still learns whether
/// it committed): `\xff/SC/` followed by a random UID in the binary format, 22 bytes. The
/// server never parses it; it only has to be unique.
pub fn self_conflict_key(unique_id: Uid) -> Result<Vec<u8>, BinaryEncodeError> {
    let mut w = BinaryWriter::unversioned();
    w.write_bytes(SELF_CONFLICT_PREFIX)?;
    w.write(&unique_id)?;
    Ok(w.into_bytes())
}

// ---------------------------------------------------------------------------------------
// SPAN_PARENT.

/// `TraceFlags`: the flag byte of a [`SpanContext`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TraceFlags {
    /// `unsampled` (0).
    Unsampled,
    /// `sampled` (1).
    Sampled,
}

crate::binary_enum!(TraceFlags: u8 {
    TraceFlags::Unsampled = 0,
    TraceFlags::Sampled = 1,
});

/// `SpanContext` (`fdbclient/Tracing.h`): a trace id, a span id and flags, serialized as
/// `serializer(ar, traceID, spanID, m_Flags)`: 25 bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpanContext {
    /// `traceID`.
    pub trace_id: Uid,
    /// `spanID`.
    pub span_id: u64,
    /// `m_Flags`.
    pub flags: TraceFlags,
}

impl SpanContext {
    /// The length of a `SPAN_PARENT` transaction option value: an `IncludeVersion` prefix
    /// and the 25 bytes of the span context. (FDB rejects other lengths with
    /// `invalid_option_value`.)
    pub const OPTION_VALUE_LEN: usize = 33;

    /// The value of the `SPAN_PARENT` transaction option: `BinaryWriter::toValue(span,
    /// IncludeVersion(version))`. `version` is the protocol version of the process that
    /// writes it; the receiver only checks that it is valid.
    pub fn to_option_value(&self, version: ProtocolVersion) -> Result<Vec<u8>, BinaryEncodeError> {
        to_vec(self, Versioning::Include(version))
    }

    /// Reads the value of a `SPAN_PARENT` transaction option.
    pub fn from_option_value(value: &[u8]) -> Result<SpanContext, BinaryDecodeError> {
        if value.len() != Self::OPTION_VALUE_LEN {
            return Err(BinaryDecodeError::Invalid(
                "SPAN_PARENT value is not 33 bytes",
            ));
        }
        from_slice(value, Versioning::Include(ProtocolVersion(0)))
    }
}

impl BinarySerialize for SpanContext {
    fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
        w.write(&self.trace_id)?;
        w.write(&self.span_id)?;
        w.write(&self.flags)
    }
}

impl<'de> BinaryDeserialize<'de> for SpanContext {
    fn deserialize(r: &mut BinaryReader<'de>) -> Result<Self, BinaryDecodeError> {
        Ok(SpanContext {
            trace_id: r.read()?,
            span_id: r.read()?,
            flags: r.read()?,
        })
    }
}

// ---------------------------------------------------------------------------------------
// GetHealthMetricsReply.serialized.

/// `HealthMetrics::StorageStats` (`fdbclient/FDBTypes.h`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct StorageStats {
    /// `storageQueue`.
    pub storage_queue: i64,
    /// `storageDurabilityLag`.
    pub storage_durability_lag: i64,
    /// `diskUsage`.
    pub disk_usage: f64,
    /// `cpuUsage`.
    pub cpu_usage: f64,
}

impl BinarySerialize for StorageStats {
    fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
        w.write(&self.storage_queue)?;
        w.write(&self.storage_durability_lag)?;
        w.write(&self.disk_usage)?;
        w.write(&self.cpu_usage)
    }
}

impl<'de> BinaryDeserialize<'de> for StorageStats {
    fn deserialize(r: &mut BinaryReader<'de>) -> Result<Self, BinaryDecodeError> {
        Ok(StorageStats {
            storage_queue: r.read()?,
            storage_durability_lag: r.read()?,
            disk_usage: r.read()?,
            cpu_usage: r.read()?,
        })
    }
}

/// `HealthMetrics` (`fdbclient/FDBTypes.h`): the cluster's health as the ratekeeper
/// reports it. Inside a flatbuffers `GetHealthMetricsReply` it travels pre-serialized in
/// the binary format, as the `serialized` string, behind an `IncludeVersion` prefix:
/// [`HealthMetrics::from_blob`] reads that string.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HealthMetrics {
    /// `worstStorageQueue`.
    pub worst_storage_queue: i64,
    /// `limitingStorageQueue`.
    pub limiting_storage_queue: i64,
    /// `worstStorageDurabilityLag`.
    pub worst_storage_durability_lag: i64,
    /// `limitingStorageDurabilityLag`.
    pub limiting_storage_durability_lag: i64,
    /// `worstTLogQueue`.
    pub worst_t_log_queue: i64,
    /// `tpsLimit`.
    pub tps_limit: f64,
    /// `batchLimited`.
    pub batch_limited: bool,
    /// `storageStats`, by storage server id.
    pub storage_stats: BTreeMap<Uid, StorageStats>,
    /// `tLogQueue`, by log server id.
    pub t_log_queue: BTreeMap<Uid, i64>,
}

impl HealthMetrics {
    /// Reads `GetHealthMetricsReply::serialized`.
    pub fn from_blob(blob: &[u8]) -> Result<HealthMetrics, BinaryDecodeError> {
        from_slice(blob, Versioning::Include(ProtocolVersion(0)))
    }

    /// The `serialized` string a server of protocol `version` sends.
    pub fn to_blob(&self, version: ProtocolVersion) -> Result<Vec<u8>, BinaryEncodeError> {
        to_vec(self, Versioning::Include(version))
    }
}

// The members in `serializer` argument order, which is not declaration order.
impl BinarySerialize for HealthMetrics {
    fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
        w.write(&self.worst_storage_queue)?;
        w.write(&self.worst_storage_durability_lag)?;
        w.write(&self.worst_t_log_queue)?;
        w.write(&self.tps_limit)?;
        w.write(&self.batch_limited)?;
        w.write(&self.storage_stats)?;
        w.write(&self.t_log_queue)?;
        w.write(&self.limiting_storage_queue)?;
        w.write(&self.limiting_storage_durability_lag)
    }
}

impl<'de> BinaryDeserialize<'de> for HealthMetrics {
    fn deserialize(r: &mut BinaryReader<'de>) -> Result<Self, BinaryDecodeError> {
        let worst_storage_queue = r.read()?;
        let worst_storage_durability_lag = r.read()?;
        let worst_t_log_queue = r.read()?;
        let tps_limit = r.read()?;
        let batch_limited = r.read()?;
        let storage_stats = r.read()?;
        let t_log_queue = r.read()?;
        let limiting_storage_queue = r.read()?;
        let limiting_storage_durability_lag = r.read()?;
        Ok(HealthMetrics {
            worst_storage_queue,
            limiting_storage_queue,
            worst_storage_durability_lag,
            limiting_storage_durability_lag,
            worst_t_log_queue,
            tps_limit,
            batch_limited,
            storage_stats,
            t_log_queue,
        })
    }
}

// ---------------------------------------------------------------------------------------
// Idempotency ids (`\xff\x02/idmp/`).

/// `idempotencyIdKeys.begin`: the prefix of the keys the commit proxies store idempotency
/// ids under (`\xff\x02/idmp/`).
pub const IDEMPOTENCY_ID_KEYS_BEGIN: &[u8] = b"\xff\x02/idmp/";

/// The key of an idempotency-id record, from `makeIdempotencySingleKeyRange`: the prefix,
/// the commit version as a big-endian `i64`, and the high-order byte of the batch index.
pub fn idempotency_key(commit_version: i64, high_order_batch_index: u8) -> Vec<u8> {
    let mut key = IDEMPOTENCY_ID_KEYS_BEGIN.to_vec();
    key.extend_from_slice(&commit_version.to_be_bytes());
    key.push(high_order_batch_index);
    key
}

/// `decodeIdempotencyKey`: the commit version and high-order batch-index byte of a record's
/// key. The key's prefix is skipped, not checked, as in C++.
pub fn decode_idempotency_key(key: &[u8]) -> Result<(i64, u8), BinaryDecodeError> {
    let mut r = BinaryReader::unversioned(key);
    r.read_bytes(IDEMPOTENCY_ID_KEYS_BEGIN.len())?;
    let version = i64::from_be_bytes(r.read_array()?);
    Ok((version, r.read_u8()?))
}

/// The key range `determineCommitStatus` reads to look for a transaction's idempotency id
/// when the commit version is known to be in `min..=max`: the prefix followed by `min` and
/// by `max + 1`, each a big-endian `i64`. `None` if `max + 1` overflows.
pub fn idempotency_scan_range(
    min_commit_version: i64,
    max_commit_version: i64,
) -> Option<(Vec<u8>, Vec<u8>)> {
    let end = max_commit_version.checked_add(1)?;
    let key = |v: i64| [IDEMPOTENCY_ID_KEYS_BEGIN, &v.to_be_bytes()[..]].concat();
    Some((key(min_commit_version), key(end)))
}

/// One idempotency id in an [`IdempotencyValue`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IdempotencyEntry<'a> {
    /// The id: 16 to 255 bytes, borrowed from the value.
    pub id: &'a [u8],
    /// The low-order byte of the transaction's batch index in the commit.
    pub low_order_batch_index: u8,
}

/// The value of an idempotency-id record (`IdempotencyIdKVBuilder`), written by a commit
/// proxy and read by a client that retries a commit after `commit_unknown_result`:
///
/// ```text
/// u64 protocol version   IncludeVersion() (no object-serializer flag)
/// i64 timestamp          the proxy's now(), whole seconds
/// repeated: u8 id length, id bytes, u8 low-order batch index
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdempotencyValue<'a> {
    /// The protocol version of the proxy that wrote the value.
    pub protocol_version: ProtocolVersion,
    /// `int64_t(now())` when the first id was added.
    pub timestamp: i64,
    /// The ids committed at this version with this batch-index high byte.
    pub entries: Vec<IdempotencyEntry<'a>>,
}

impl<'a> IdempotencyValue<'a> {
    /// Reads a record's value (`kvContainsIdempotencyId` does the same loop).
    pub fn decode(value: &'a [u8]) -> Result<IdempotencyValue<'a>, BinaryDecodeError> {
        let mut r = BinaryReader::include_version(value)?;
        let protocol_version = r.protocol_version().unwrap_or(ProtocolVersion(0));
        let timestamp = r.read()?;
        let mut entries = Vec::new();
        while !r.is_empty() {
            let len = usize::from(r.read_u8()?);
            let id = r.read_bytes(len)?;
            entries.push(IdempotencyEntry {
                id,
                low_order_batch_index: r.read_u8()?,
            });
        }
        Ok(IdempotencyValue {
            protocol_version,
            timestamp,
            entries,
        })
    }

    /// Writes the value as a proxy of `self.protocol_version` does.
    pub fn encode(&self) -> Result<Vec<u8>, BinaryEncodeError> {
        let mut w = BinaryWriter::include_version(self.protocol_version)?;
        w.write(&self.timestamp)?;
        for entry in &self.entries {
            let len = u8::try_from(entry.id.len()).map_err(|_| BinaryEncodeError::TooLarge)?;
            w.write(&len)?;
            w.write_bytes(entry.id)?;
            w.write(&entry.low_order_batch_index)?;
        }
        Ok(w.into_bytes())
    }

    /// `kvContainsIdempotencyId`: the batch-index low byte stored with `id`, if the value has it.
    pub fn find(&self, id: &[u8]) -> Option<u8> {
        self.entries
            .iter()
            .find(|e| e.id == id)
            .map(|e| e.low_order_batch_index)
    }
}
