//! Wire types whose layout is the same on 7.4 and 8.0. Both schema sets re-export them; a
//! type that changes in a later release gets its own definition in that release's module.

use std::collections::BTreeMap;

use fdb_native_wire::{
    Decode, DecodeError, Encode, EncodeError, FdbError, KeyValueVectorRef, NetworkAddress,
    ObjectRef, Reader, ReplyPromise, RequestStream, Shape, Tag, Uid, VectorView, Wire, Writer,
};

use crate::SchemaVersion;
use crate::domain;
pub use crate::span_context::{SpanContext, TraceFlags};

/// `std::pair<A, B>` in a vector (`std::vector<std::pair<...>>`, `std::deque`): a table
/// `{first, second}`. (`std::map` and `std::unordered_map` fields are `BTreeMap`s, which
/// have the same wire form.)
#[derive(Wire, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Pair<A, B> {
    /// `first`.
    pub first: A,
    /// `second`.
    pub second: B,
}

impl<A, B> Pair<A, B> {
    /// `{first, second}`.
    pub const fn new(first: A, second: B) -> Self {
        Pair { first, second }
    }
}

// ---------------------------------------------------------------------------------------
// KeyRangeRef and MutationRef: single-key compression.

/// `equalsKeyAfter(key, compare)`: `compare` is `key\x00`.
pub(crate) fn equals_key_after(key: &[u8], compare: &[u8]) -> bool {
    compare.len() == key.len() + 1 && compare.last() == Some(&0) && compare.starts_with(key)
}

/// Undoes single-key compression: `(k\x00, "")` is `[k, k\x00)`. The caller has checked that
/// `second` is empty and `first` is not.
fn expand_single_key<'a>(
    first: &'a [u8],
    what: &'static str,
) -> Result<(&'a [u8], &'a [u8]), DecodeError> {
    match first.split_last() {
        Some((0, key)) => Ok((key, first)),
        _ => Err(DecodeError::Invalid(what)),
    }
}

/// `KeyRangeRef` (`serializer(ar, begin, end)`): a table of two strings, with single-key
/// compression.
///
/// The writer sends a single-key range `[k, k\x00)` as `(begin = k\x00, end = "")`, and the
/// reader expands a range whose `end` is empty and whose `begin` is not back to
/// `[begin - last byte, begin)`. This type holds the expanded range; [`KeyRangeRef::wire_form`]
/// is what is written.
///
/// An inverted range, `begin > end`, is an error both ways, as in C++, whose `serialize`
/// throws `inverted_range` on the writer and on the reader side (and a server that fails to
/// deserialize a request exits, so an inverted conflict range must never be sent). It is
/// also what keeps the compressed form unambiguous: `(k\x00, "")` as a real range would be
/// inverted. Decoding also rejects a compressed `begin` that does not end in `\x00` (C++
/// `ASSERT`s, killing the process).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KeyRangeRef<'a> {
    /// `begin`.
    pub begin: &'a [u8],
    /// `end`.
    pub end: &'a [u8],
}

impl<'a> KeyRangeRef<'a> {
    /// `[begin, end)`.
    pub const fn new(begin: &'a [u8], end: &'a [u8]) -> Self {
        KeyRangeRef { begin, end }
    }

    /// The `(begin, end)` the writer sends: `(end, "")` for a single-key range, else
    /// `(begin, end)`; an error for an inverted range.
    pub fn wire_form(&self) -> Result<(&'a [u8], &'a [u8]), EncodeError> {
        if self.begin > self.end {
            Err(EncodeError::Invalid("inverted KeyRangeRef (begin > end)"))
        } else if equals_key_after(self.begin, self.end) {
            Ok((self.end, b""))
        } else {
            Ok((self.begin, self.end))
        }
    }

    /// The range a reader makes of `(begin, end)` as received.
    pub fn from_wire_form(begin: &'a [u8], end: &'a [u8]) -> Result<Self, DecodeError> {
        let (begin, end) = if end.is_empty() && !begin.is_empty() {
            expand_single_key(
                begin,
                "a compressed single-key KeyRangeRef must end in \\x00",
            )?
        } else {
            (begin, end)
        };
        if begin > end {
            return Err(DecodeError::Invalid("inverted KeyRangeRef (begin > end)"));
        }
        Ok(KeyRangeRef { begin, end })
    }
}

impl Wire for KeyRangeRef<'_> {
    const SHAPE: Shape = Shape::Table;
}

impl Encode for KeyRangeRef<'_> {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let (begin, end) = self.wire_form()?;
        let mut t = w.table();
        t.field(&begin)?;
        t.field(&end)?;
        t.finish()
    }
}

impl<'de: 'a, 'a> Decode<'de> for KeyRangeRef<'a> {
    /// `KeyRangeRef()`: both keys empty (C++ `KeyRangeRef() = default`, 7.4 and 8.0).
    fn absent() -> Self {
        KeyRangeRef::default()
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let mut t = r.table(at)?;
        let begin: &'a [u8] = t.field()?;
        let end: &'a [u8] = t.field()?;
        KeyRangeRef::from_wire_form(begin, end)
    }
}

/// `MutationRef` (`serializer(ar, type, param1, param2)`): a table of a `u8` type and two
/// strings, with single-key compression for clears.
///
/// The writer sends a `ClearRange` (type 1) of a single key, `[k, k\x00)`, as `(type,
/// param1 = k\x00, param2 = "")`; the reader expands a `ClearRange` whose `param2` is empty
/// and whose `param1` is not. This type holds the expanded mutation;
/// [`MutationRef::wire_form`] is what is written. Encoding rejects an inverted `ClearRange`
/// (`param1 > param2`): `(k\x00, "")` would otherwise be read back as a clear of `k`. Decoding
/// rejects a compressed `param1` that does not end in `\x00` (C++ logs a `SevError` and marks
/// the mutation corrupted).
///
/// **Checksums.** With the `ENABLE_MUTATION_CHECKSUM` client knob (off by default), the C++
/// writer sets bit 7 of the type ([`MutationRef::CHECKSUM_FLAG`]) and appends a 4-byte
/// checksum to the written `param2`, and with `ENABLE_ACCUMULATIVE_CHECKSUM` also bit 6
/// ([`MutationRef::ACCUMULATIVE_CHECKSUM_INDEX_FLAG`]) and a 2-byte index after it
/// (`MutationRef::serialize` in `fdbclient/include/fdbclient/CommitTransaction.h`, the same
/// on 7.4 and 8.0). The reader undoes this before the single-key expansion, as C++'s
/// deserialize branch does (`offloadAccumulativeChecksumIndex`, then `offloadChecksum`: the
/// flag bits are cleared and the suffixes, little-endian, removed from `param2`). The values
/// are kept in [`checksum`](MutationRef::checksum) and
/// [`accumulative_checksum_index`](MutationRef::accumulative_checksum_index) as received; the
/// CRC-32C is not verified (C++ only logs a mismatch and marks the mutation corrupted), and
/// the encoder never writes checksums, like a C++ writer with the knob off. `kind` never has
/// the flag bits: decoding rejects a `param2` too short for its flags and an index flag
/// without the checksum flag, and encoding rejects a type with bit 6 or 7 set.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct MutationRef<'a> {
    /// `type` (`MutationRef::Type`), without the checksum flags.
    pub kind: u8,
    /// `param1`.
    pub param1: &'a [u8],
    /// `param2`, without a checksum suffix.
    pub param2: &'a [u8],
    /// The checksum suffix of a received mutation (`MutationRef::checksum`). Not written.
    pub checksum: Option<u32>,
    /// The accumulative checksum index suffix of a received mutation
    /// (`MutationRef::accumulativeChecksumIndex`). Not written.
    pub accumulative_checksum_index: Option<u16>,
}

impl<'a> MutationRef<'a> {
    /// `ClearRange`.
    pub const CLEAR_RANGE: u8 = 1;
    /// `MAX_ATOMIC_OP`: the type of a default-constructed `MutationRef()`, one past the last
    /// mutation type (24 on both 7.4 and 8.0).
    pub const MAX_ATOMIC_OP: u8 = 24;
    /// `CHECKSUM_FLAG_MASK`: the type bit that says `param2` ends in a 4-byte checksum.
    pub const CHECKSUM_FLAG: u8 = 0x80;
    /// `ACCUMULATIVE_CHECKSUM_INDEX_FLAG_MASK`: the type bit that says `param2` ends in a
    /// 2-byte accumulative checksum index (after the checksum).
    pub const ACCUMULATIVE_CHECKSUM_INDEX_FLAG: u8 = 0x40;

    /// A mutation.
    pub const fn new(kind: u8, param1: &'a [u8], param2: &'a [u8]) -> Self {
        MutationRef {
            kind,
            param1,
            param2,
            checksum: None,
            accumulative_checksum_index: None,
        }
    }

    /// The `(param1, param2)` the writer sends: `(param2, "")` for a single-key clear, else
    /// `(param1, param2)`; an error for a type with a checksum flag bit or an inverted
    /// `ClearRange`.
    pub fn wire_form(&self) -> Result<(&'a [u8], &'a [u8]), EncodeError> {
        if self.kind & (Self::CHECKSUM_FLAG | Self::ACCUMULATIVE_CHECKSUM_INDEX_FLAG) != 0 {
            return Err(EncodeError::Invalid(
                "a MutationRef type has a checksum flag bit (6 or 7) set",
            ));
        }
        if self.kind != Self::CLEAR_RANGE {
            Ok((self.param1, self.param2))
        } else if self.param1 > self.param2 {
            Err(EncodeError::Invalid(
                "inverted ClearRange MutationRef (param1 > param2)",
            ))
        } else if equals_key_after(self.param1, self.param2) {
            Ok((self.param2, b""))
        } else {
            Ok((self.param1, self.param2))
        }
    }

    /// The mutation a reader makes of `(type, param1, param2)` as received: checksum
    /// suffixes removed, then a single-key clear expanded.
    pub fn from_wire_form(
        type_byte: u8,
        param1: &'a [u8],
        param2: &'a [u8],
    ) -> Result<Self, DecodeError> {
        let mut kind = type_byte;
        let mut param2 = param2;
        let mut checksum = None;
        let mut accumulative_checksum_index = None;
        if kind & Self::CHECKSUM_FLAG != 0 {
            // validateParam2, offloadAccumulativeChecksumIndex, offloadChecksum.
            if kind & Self::ACCUMULATIVE_CHECKSUM_INDEX_FLAG != 0 {
                let (rest, index) = param2.split_last_chunk::<2>().ok_or(DecodeError::Invalid(
                    "a MutationRef param2 is too short for its checksum flags",
                ))?;
                accumulative_checksum_index = Some(u16::from_le_bytes(*index));
                param2 = rest;
                kind &= !Self::ACCUMULATIVE_CHECKSUM_INDEX_FLAG;
            }
            let (rest, crc) = param2.split_last_chunk::<4>().ok_or(DecodeError::Invalid(
                "a MutationRef param2 is too short for its checksum flags",
            ))?;
            checksum = Some(u32::from_le_bytes(*crc));
            param2 = rest;
            kind &= !Self::CHECKSUM_FLAG;
        } else if kind & Self::ACCUMULATIVE_CHECKSUM_INDEX_FLAG != 0 {
            // C++ only offloads the index together with a checksum; it would keep an
            // invalid type.
            return Err(DecodeError::Invalid(
                "a MutationRef has the accumulative checksum index flag without the checksum flag",
            ));
        }
        let (param1, param2) =
            if kind == Self::CLEAR_RANGE && param2.is_empty() && !param1.is_empty() {
                expand_single_key(
                    param1,
                    "a compressed single-key ClearRange must end in \\x00",
                )?
            } else {
                (param1, param2)
            };
        Ok(MutationRef {
            kind,
            param1,
            param2,
            checksum,
            accumulative_checksum_index,
        })
    }
}

impl Wire for MutationRef<'_> {
    const SHAPE: Shape = Shape::Table;
}

impl Encode for MutationRef<'_> {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let (param1, param2) = self.wire_form()?;
        let mut t = w.table();
        t.field(&self.kind)?;
        t.field(&param1)?;
        t.field(&param2)?;
        t.finish()
    }
}

impl<'de: 'a, 'a> Decode<'de> for MutationRef<'a> {
    /// `MutationRef()`: `type(MAX_ATOMIC_OP)`, empty parameters, no checksum (7.4 and 8.0).
    /// Not [`MutationRef::default`], whose type is 0 (`SetValue`).
    fn absent() -> Self {
        MutationRef::new(Self::MAX_ATOMIC_OP, b"", b"")
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let mut t = r.table(at)?;
        let kind: u8 = t.field()?;
        let param1: &'a [u8] = t.field()?;
        let param2: &'a [u8] = t.field()?;
        MutationRef::from_wire_form(kind, param1, param2)
    }
}

impl<'a> From<&'a domain::KeyRange> for KeyRangeRef<'a> {
    fn from(r: &'a domain::KeyRange) -> Self {
        KeyRangeRef::new(&r.begin, &r.end)
    }
}

impl From<KeyRangeRef<'_>> for domain::KeyRange {
    fn from(r: KeyRangeRef<'_>) -> Self {
        domain::KeyRange {
            begin: r.begin.to_vec(),
            end: r.end.to_vec(),
        }
    }
}

impl<'a> From<&'a domain::Mutation> for MutationRef<'a> {
    fn from(m: &'a domain::Mutation) -> Self {
        MutationRef::new(m.kind.0, &m.param1, &m.param2)
    }
}

impl From<MutationRef<'_>> for domain::Mutation {
    /// The checksum suffixes, already removed from `param2`, are dropped.
    fn from(m: MutationRef<'_>) -> Self {
        domain::Mutation {
            kind: domain::MutationType(m.kind),
            param1: m.param1.to_vec(),
            param2: m.param2.to_vec(),
        }
    }
}

// ---------------------------------------------------------------------------------------
// Interfaces.

/// `LocalityData` (`serializer(ar, _data)`): a `std::map<Key, Optional<Value>>`.
///
/// Absent: `LocalityData() {}`, an empty map (zero).
#[derive(Wire, Clone, Debug, Default, PartialEq, Eq)]
pub struct LocalityData<'a> {
    /// `_data`.
    pub data: BTreeMap<&'a [u8], Option<&'a [u8]>>,
}

impl<'a> From<&'a domain::Locality> for LocalityData<'a> {
    fn from(l: &'a domain::Locality) -> Self {
        LocalityData {
            data: l
                .iter()
                .map(|(k, v)| (k.as_slice(), v.as_deref()))
                .collect(),
        }
    }
}

impl From<LocalityData<'_>> for domain::Locality {
    fn from(l: LocalityData<'_>) -> Self {
        l.data
            .into_iter()
            .map(|(k, v)| (k.to_vec(), v.map(<[u8]>::to_vec)))
            .collect()
    }
}

/// `GrvProxyInterface` (`serializer(ar, processId, provisional, getConsistentReadVersion)`).
/// The other streams are [adjusted endpoints](crate::streams) of
/// `getConsistentReadVersion`.
///
/// Absent: no constructor, so value-initialized (zero): no process id, not provisional.
#[derive(Wire, Clone, Debug, Default, PartialEq, Eq)]
#[fdb(file_identifier = 8743216)]
pub struct GrvProxyInterface<'a> {
    /// `processId`.
    pub process_id: Option<&'a [u8]>,
    /// `provisional`.
    pub provisional: bool,
    /// `getConsistentReadVersion`, the base endpoint.
    pub get_consistent_read_version: RequestStream<domain::GetReadVersion>,
}

impl<'a> From<&'a domain::GrvProxy> for GrvProxyInterface<'a> {
    fn from(p: &'a domain::GrvProxy) -> Self {
        GrvProxyInterface {
            process_id: p.process_id.as_deref(),
            provisional: p.provisional,
            get_consistent_read_version: RequestStream::new(p.base),
        }
    }
}

impl From<GrvProxyInterface<'_>> for domain::GrvProxy {
    fn from(p: GrvProxyInterface<'_>) -> Self {
        domain::GrvProxy {
            process_id: p.process_id.map(<[u8]>::to_vec),
            provisional: p.provisional,
            base: *p.get_consistent_read_version.endpoint(),
        }
    }
}

/// `CommitProxyInterface` (`serializer(ar, processId, provisional, commit)`). The layout is
/// the same on 7.4 and 8.0, but the [adjusted-endpoint indices](crate::streams) of the other
/// streams are not, so [`CommitProxyInterface::to_domain`] takes the version.
///
/// Absent: no constructor, so value-initialized (zero).
#[derive(Wire, Clone, Debug, Default, PartialEq, Eq)]
#[fdb(file_identifier = 8954922)]
pub struct CommitProxyInterface<'a> {
    /// `processId`.
    pub process_id: Option<&'a [u8]>,
    /// `provisional`.
    pub provisional: bool,
    /// `commit`, the base endpoint.
    pub commit: RequestStream<domain::Commit>,
}

impl CommitProxyInterface<'_> {
    /// The domain proxy, whose stream indices are those of `schema`.
    pub fn to_domain(&self, schema: SchemaVersion) -> domain::CommitProxy {
        domain::CommitProxy {
            process_id: self.process_id.map(<[u8]>::to_vec),
            provisional: self.provisional,
            base: *self.commit.endpoint(),
            schema,
        }
    }
}

impl<'a> From<&'a domain::CommitProxy> for CommitProxyInterface<'a> {
    fn from(p: &'a domain::CommitProxy) -> Self {
        CommitProxyInterface {
            process_id: p.process_id.as_deref(),
            provisional: p.provisional,
            commit: RequestStream::new(p.base),
        }
    }
}

/// `StorageServerInterface` (`serializer(ar, uniqueID, locality, getValue, tssPairID,
/// acceptingRequests)`). The other streams are [adjusted endpoints](crate::streams) of
/// `getValue`.
///
/// Absent: `StorageServerInterface()` sets `acceptingRequests = false` and a *random*
/// `uniqueID` (`deterministicRandom()->randomUniqueID()`); this is the zero id, the one value
/// a reader cannot reproduce.
#[derive(Wire, Clone, Debug, Default, PartialEq, Eq)]
#[fdb(file_identifier = 15302073)]
pub struct StorageServerInterface<'a> {
    /// `uniqueID`.
    pub unique_id: Uid,
    /// `locality`.
    pub locality: LocalityData<'a>,
    /// `getValue`, the base endpoint.
    pub get_value: RequestStream<domain::GetValue>,
    /// `tssPairID`.
    pub tss_pair_id: Option<Uid>,
    /// `acceptingRequests`.
    pub accepting_requests: bool,
}

impl<'a> From<&'a domain::StorageServer> for StorageServerInterface<'a> {
    fn from(s: &'a domain::StorageServer) -> Self {
        StorageServerInterface {
            unique_id: s.id,
            locality: LocalityData::from(&s.locality),
            get_value: RequestStream::new(s.base),
            tss_pair_id: s.tss_pair_id,
            accepting_requests: s.accepting_requests,
        }
    }
}

impl From<StorageServerInterface<'_>> for domain::StorageServer {
    fn from(s: StorageServerInterface<'_>) -> Self {
        domain::StorageServer {
            id: s.unique_id,
            locality: s.locality.into(),
            base: *s.get_value.endpoint(),
            tss_pair_id: s.tss_pair_id,
            accepting_requests: s.accepting_requests,
        }
    }
}

// ---------------------------------------------------------------------------------------
// Coordinator.

/// `VersionHistory` (`serializer(ar, mutations, version)`).
///
/// Absent: `VersionHistory()`; `version` is uninitialized on 7.4 (`VersionHistory() {}`) and
/// zero on 8.0 (`= default`), so zero.
#[derive(Wire, Clone, Debug, Default, PartialEq, Eq)]
#[fdb(file_identifier = 5863456)]
pub struct VersionHistory<'a> {
    /// `mutations`.
    pub mutations: Vec<MutationRef<'a>>,
    /// `version`.
    pub version: i64,
}

impl<'a> From<&'a domain::VersionHistory> for VersionHistory<'a> {
    fn from(h: &'a domain::VersionHistory) -> Self {
        VersionHistory {
            mutations: h.mutations.iter().map(MutationRef::from).collect(),
            version: h.version,
        }
    }
}

impl From<VersionHistory<'_>> for domain::VersionHistory {
    fn from(h: VersionHistory<'_>) -> Self {
        domain::VersionHistory {
            version: h.version,
            mutations: h
                .mutations
                .into_iter()
                .map(domain::Mutation::from)
                .collect(),
        }
    }
}

/// `ClientVersionRef` (`serializer(ar, clientVersion, sourceVersion, protocolVersion)`).
///
/// Absent: `ClientVersionRef() { initUnknown(); }`, all three strings `"Unknown"` (7.4 and
/// 8.0). [`Default`] is that too.
#[derive(Wire, Clone, Copy, Debug, PartialEq, Eq)]
#[fdb(absent = "default")]
pub struct ClientVersionRef<'a> {
    /// `clientVersion`.
    pub client_version: &'a [u8],
    /// `sourceVersion`.
    pub source_version: &'a [u8],
    /// `protocolVersion`.
    pub protocol_version: &'a [u8],
}

/// `Hostname` (`serializer(ar, host, service, isTLS)`).
///
/// Absent: `Hostname() : host(""), service(""), isTLS(false)`, zero.
#[derive(Wire, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Hostname<'a> {
    /// `host`.
    pub host: &'a [u8],
    /// `service`.
    pub service: &'a [u8],
    /// `isTLS`.
    pub is_tls: bool,
}

impl Default for ClientVersionRef<'_> {
    /// `ClientVersionRef()`: `"Unknown"` for all three.
    fn default() -> Self {
        ClientVersionRef {
            client_version: b"Unknown",
            source_version: b"Unknown",
            protocol_version: b"Unknown",
        }
    }
}

/// `OpenDatabaseCoordRequest` (`serializer(ar, issues, supportedVersions, traceLogGroup,
/// knownClientInfoID, clusterKey, coordinators, reply, hostnames, internal)`).
///
/// The reply is declared `ReplyPromise<CachedSerialization<ClientDBInfo>>`, but it arrives
/// exactly as `ErrorOr<EnsureTable<ClientDBInfo>>`, with the version's `ClientDBInfo`.
///
/// Absent: `bool internal{ true }` is its only non-zero member initializer (7.4 and 8.0), so
/// [`Default`] and an absent table have `internal` set; a *present* table without the `internal`
/// slot reads `false`.
#[derive(Wire, Clone, Debug, PartialEq, Eq)]
#[fdb(file_identifier = 214728, absent = "default")]
pub struct OpenDatabaseCoordRequest<'a> {
    /// `issues`.
    pub issues: Vec<&'a [u8]>,
    /// `supportedVersions`.
    pub supported_versions: Vec<ClientVersionRef<'a>>,
    /// `traceLogGroup`.
    pub trace_log_group: &'a [u8],
    /// `knownClientInfoID`.
    pub known_client_info_id: Uid,
    /// `clusterKey`.
    pub cluster_key: &'a [u8],
    /// `coordinators`.
    pub coordinators: Vec<NetworkAddress>,
    /// `reply`.
    pub reply: ReplyPromise<domain::ClientDbInfo>,
    /// `hostnames`.
    pub hostnames: Vec<Hostname<'a>>,
    /// `internal`.
    pub internal: bool,
}

impl Default for OpenDatabaseCoordRequest<'_> {
    /// `OpenDatabaseCoordRequest()`: everything empty or zero except `internal`, which is
    /// `true`.
    fn default() -> Self {
        OpenDatabaseCoordRequest {
            issues: Vec::new(),
            supported_versions: Vec::new(),
            trace_log_group: b"",
            known_client_info_id: Uid::default(),
            cluster_key: b"",
            coordinators: Vec::new(),
            reply: ReplyPromise::default(),
            hostnames: Vec::new(),
            internal: true,
        }
    }
}

impl<'a> OpenDatabaseCoordRequest<'a> {
    /// The request for `r`, whose reply goes to the endpoint with token `reply`.
    pub fn from_domain(r: &'a domain::OpenDatabaseCoord, reply: Uid) -> Self {
        OpenDatabaseCoordRequest {
            issues: r.issues.iter().map(Vec::as_slice).collect(),
            supported_versions: r
                .supported_versions
                .iter()
                .map(|v| ClientVersionRef {
                    client_version: &v.client_version,
                    source_version: &v.source_version,
                    protocol_version: &v.protocol_version,
                })
                .collect(),
            trace_log_group: &r.trace_log_group,
            known_client_info_id: r.known_client_info_id,
            cluster_key: &r.cluster_key,
            coordinators: r.coordinators.clone(),
            reply: ReplyPromise::new(reply),
            hostnames: r
                .hostnames
                .iter()
                .map(|h| Hostname {
                    host: &h.host,
                    service: &h.service,
                    is_tls: h.is_tls,
                })
                .collect(),
            internal: r.internal,
        }
    }
}

impl From<OpenDatabaseCoordRequest<'_>> for domain::OpenDatabaseCoord {
    /// The reply token is `r.reply.token()`.
    fn from(r: OpenDatabaseCoordRequest<'_>) -> Self {
        domain::OpenDatabaseCoord {
            issues: r.issues.into_iter().map(<[u8]>::to_vec).collect(),
            supported_versions: r
                .supported_versions
                .into_iter()
                .map(|v| domain::ClientVersion {
                    client_version: v.client_version.to_vec(),
                    source_version: v.source_version.to_vec(),
                    protocol_version: v.protocol_version.to_vec(),
                })
                .collect(),
            trace_log_group: r.trace_log_group.to_vec(),
            known_client_info_id: r.known_client_info_id,
            cluster_key: r.cluster_key.to_vec(),
            coordinators: r.coordinators,
            hostnames: r
                .hostnames
                .into_iter()
                .map(|h| domain::Hostname {
                    host: h.host.to_vec(),
                    service: h.service.to_vec(),
                    is_tls: h.is_tls,
                })
                .collect(),
            internal: r.internal,
        }
    }
}

// ---------------------------------------------------------------------------------------
// GRV proxy.

/// `ClientTagThrottleLimits` as it is on the wire: `serializer(ar, tpsRate, duration)`, where
/// `duration` is a local of `serialize()`, `expiration - now()` on the writer's clock (C++'s
/// reader sets `expiration = now() + duration` on its own clock). The domain
/// [`TagThrottleLimits`](domain::TagThrottleLimits) keeps the duration as received and turns
/// it into a deadline with the receive time.
///
/// Absent: `ClientTagThrottleLimits() : tpsRate(0), expiration(0)`, so a zero rate that expired
/// long ago: zero here (an expired duration).
#[derive(Wire, Clone, Copy, Debug, Default, PartialEq)]
pub struct ClientTagThrottleLimits {
    /// `tpsRate`.
    pub tps_rate: f64,
    /// `duration`: seconds until the limit expires.
    pub duration: f64,
}

impl From<ClientTagThrottleLimits> for domain::TagThrottleLimits {
    fn from(l: ClientTagThrottleLimits) -> Self {
        domain::TagThrottleLimits {
            tps_rate: l.tps_rate,
            duration: l.duration,
        }
    }
}

impl From<&domain::TagThrottleLimits> for ClientTagThrottleLimits {
    fn from(l: &domain::TagThrottleLimits) -> Self {
        ClientTagThrottleLimits {
            tps_rate: l.tps_rate,
            duration: l.duration,
        }
    }
}

// ---------------------------------------------------------------------------------------
// Key server locations.

/// One entry of `GetKeyServerLocationsReply::results`: a shard and its storage servers
/// (`std::pair<KeyRangeRef, std::vector<StorageServerInterface>>`).
pub type KeyServerLocation<'a> = Pair<KeyRangeRef<'a>, Vec<StorageServerInterface<'a>>>;

/// `GetKeyServerLocationsReply` (`serializer(ar, results, resultsTssMapping,
/// resultsTagMapping, arena)`).
///
/// `R` is the type of `results`: by default a [`VectorView`], so a decoded reply reads each
/// shard (and its interfaces, borrowed from the message) only as it is converted, with no
/// intermediate vector; a reply built to be encoded uses a `Vec`
/// ([`GetKeyServerLocationsReply::from_domain`]). Both have the same wire form.
///
/// Absent: no constructor, so value-initialized: every vector empty (zero).
#[derive(Wire, Clone, Debug)]
#[fdb(file_identifier = 10636023)]
pub struct GetKeyServerLocationsReply<'a, R = VectorView<'a, KeyServerLocation<'a>>> {
    /// `results`.
    pub results: R,
    /// `resultsTssMapping`.
    pub results_tss_mapping: Vec<Pair<Uid, StorageServerInterface<'a>>>,
    /// `resultsTagMapping`.
    pub results_tag_mapping: Vec<Pair<Uid, Tag>>,
    /// `arena`.
    #[fdb(arena)]
    pub arena: (),
}

impl<'a> GetKeyServerLocationsReply<'a, Vec<KeyServerLocation<'a>>> {
    /// The reply to encode for `locations`.
    pub fn from_domain(locations: &'a domain::KeyServerLocations) -> Self {
        GetKeyServerLocationsReply {
            results: locations
                .results
                .iter()
                .map(|(range, servers)| {
                    Pair::new(
                        KeyRangeRef::from(range),
                        servers.iter().map(StorageServerInterface::from).collect(),
                    )
                })
                .collect(),
            results_tss_mapping: locations
                .tss_mapping
                .iter()
                .map(|(id, s)| Pair::new(*id, StorageServerInterface::from(s)))
                .collect(),
            results_tag_mapping: locations
                .tag_mapping
                .iter()
                .map(|&(id, tag)| Pair::new(id, tag))
                .collect(),
            arena: (),
        }
    }
}

fn location_to_domain(
    entry: KeyServerLocation<'_>,
) -> (domain::KeyRange, Vec<domain::StorageServer>) {
    (
        entry.first.into(),
        entry
            .second
            .into_iter()
            .map(domain::StorageServer::from)
            .collect(),
    )
}

impl<'a> TryFrom<GetKeyServerLocationsReply<'a>> for domain::KeyServerLocations {
    type Error = DecodeError;

    /// Reads every shard of the view: an error is a shard that does not decode.
    fn try_from(reply: GetKeyServerLocationsReply<'a>) -> Result<Self, DecodeError> {
        Ok(domain::KeyServerLocations {
            results: reply
                .results
                .iter()
                .map(|entry| entry.map(location_to_domain))
                .collect::<Result<_, _>>()?,
            tss_mapping: tss_to_domain(reply.results_tss_mapping),
            tag_mapping: tags_to_domain(reply.results_tag_mapping),
        })
    }
}

impl<'a> From<GetKeyServerLocationsReply<'a, Vec<KeyServerLocation<'a>>>>
    for domain::KeyServerLocations
{
    fn from(reply: GetKeyServerLocationsReply<'a, Vec<KeyServerLocation<'a>>>) -> Self {
        domain::KeyServerLocations {
            results: reply.results.into_iter().map(location_to_domain).collect(),
            tss_mapping: tss_to_domain(reply.results_tss_mapping),
            tag_mapping: tags_to_domain(reply.results_tag_mapping),
        }
    }
}

fn tss_to_domain(
    mapping: Vec<Pair<Uid, StorageServerInterface<'_>>>,
) -> Vec<(Uid, domain::StorageServer)> {
    mapping
        .into_iter()
        .map(|p| (p.first, p.second.into()))
        .collect()
}

fn tags_to_domain(mapping: Vec<Pair<Uid, Tag>>) -> Vec<(Uid, Tag)> {
    mapping.into_iter().map(|p| (p.first, p.second)).collect()
}

// ---------------------------------------------------------------------------------------
// Storage server.

/// `GetValueReply` (`serializer(ar, LoadBalancedReply::penalty, LoadBalancedReply::error,
/// value, cached)`). The value borrows from the message. [`Default`] is `GetValueReply()`:
/// penalty 1.0 (`LoadBalancedReply()`), no error, no value.
///
/// Absent: `GetValueReply()`, that is, `penalty = 1.0` (7.4 and 8.0), not zero. A present reply
/// whose `penalty` slot is missing reads `0.0`.
#[derive(Wire, Clone, Copy, Debug, PartialEq)]
#[fdb(file_identifier = 1378929, absent = "default")]
pub struct GetValueReply<'a> {
    /// `penalty`.
    pub penalty: f64,
    /// `error`.
    pub error: Option<FdbError>,
    /// `value`.
    pub value: Option<&'a [u8]>,
    /// `cached`.
    pub cached: bool,
}

impl Default for GetValueReply<'_> {
    fn default() -> Self {
        GetValueReply {
            penalty: 1.0,
            error: None,
            value: None,
            cached: false,
        }
    }
}

impl<'a> From<GetValueReply<'a>> for domain::GetValueReply<'a> {
    fn from(r: GetValueReply<'a>) -> Self {
        domain::GetValueReply {
            penalty: r.penalty,
            error: r.error,
            value: r.value,
            cached: r.cached,
        }
    }
}

impl<'a> From<&domain::GetValueReply<'a>> for GetValueReply<'a> {
    fn from(r: &domain::GetValueReply<'a>) -> Self {
        GetValueReply {
            penalty: r.penalty,
            error: r.error,
            value: r.value,
            cached: r.cached,
        }
    }
}

/// `KeySelectorRef` (`serializer(ar, key, orEqual, offset)`): the key a selector is anchored
/// at, whether the anchor itself counts (`orEqual`), and how many keys to move from it
/// (`offset`). `KeySelector`, the `Standalone` form, has the same wire form.
///
/// Absent: `KeySelectorRef() : orEqual(false), offset(0)` with an empty key (7.4 and 8.0):
/// zero.
#[derive(Wire, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KeySelectorRef<'a> {
    /// `key`.
    pub key: &'a [u8],
    /// `orEqual`.
    pub or_equal: bool,
    /// `offset`.
    pub offset: i32,
}

impl<'a> From<&'a domain::KeySelector> for KeySelectorRef<'a> {
    fn from(s: &'a domain::KeySelector) -> Self {
        KeySelectorRef {
            key: &s.key,
            or_equal: s.or_equal,
            offset: s.offset,
        }
    }
}

impl<'a> From<KeySelectorRef<'a>> for domain::KeySelectorRef<'a> {
    fn from(s: KeySelectorRef<'a>) -> Self {
        domain::KeySelectorRef {
            key: s.key,
            or_equal: s.or_equal,
            offset: s.offset,
        }
    }
}

impl<'a> From<domain::KeySelectorRef<'a>> for KeySelectorRef<'a> {
    fn from(s: domain::KeySelectorRef<'a>) -> Self {
        KeySelectorRef {
            key: s.key,
            or_equal: s.or_equal,
            offset: s.offset,
        }
    }
}

impl From<KeySelectorRef<'_>> for domain::KeySelector {
    fn from(s: KeySelectorRef<'_>) -> Self {
        domain::KeySelector {
            key: s.key.to_vec(),
            or_equal: s.or_equal,
            offset: s.offset,
        }
    }
}

/// `GetKeyReply` (`serializer(ar, LoadBalancedReply::penalty, LoadBalancedReply::error, sel,
/// cached)`): the selector the server resolved the request's selector to (`sel` is a
/// `KeySelector`). The key borrows from the message. [`Default`] is `GetKeyReply()`.
///
/// Absent: `GetKeyReply() : cached(false)` over `LoadBalancedReply()`, that is, `penalty =
/// 1.0` (7.4 and 8.0), not zero, and a default `KeySelector()`. A present reply whose
/// `penalty` slot is missing reads `0.0`.
#[derive(Wire, Clone, Copy, Debug, PartialEq)]
#[fdb(file_identifier = 11226513, absent = "default")]
pub struct GetKeyReply<'a> {
    /// `penalty`.
    pub penalty: f64,
    /// `error`.
    pub error: Option<FdbError>,
    /// `sel`.
    pub sel: KeySelectorRef<'a>,
    /// `cached`.
    pub cached: bool,
}

impl Default for GetKeyReply<'_> {
    /// `GetKeyReply()`: `penalty` 1.0, no error, a default selector.
    fn default() -> Self {
        GetKeyReply {
            penalty: 1.0,
            error: None,
            sel: KeySelectorRef::default(),
            cached: false,
        }
    }
}

impl<'a> From<GetKeyReply<'a>> for domain::GetKeyReply<'a> {
    fn from(r: GetKeyReply<'a>) -> Self {
        domain::GetKeyReply {
            penalty: r.penalty,
            error: r.error,
            sel: r.sel.into(),
            cached: r.cached,
        }
    }
}

impl<'a> From<&domain::GetKeyReply<'a>> for GetKeyReply<'a> {
    fn from(r: &domain::GetKeyReply<'a>) -> Self {
        GetKeyReply {
            penalty: r.penalty,
            error: r.error,
            sel: r.sel.into(),
            cached: r.cached,
        }
    }
}

/// `GetKeyValuesReply` (`serializer(ar, LoadBalancedReply::penalty, LoadBalancedReply::error,
/// data, version, more, cached, arena)`). `data` is a `VectorRef<KeyValueRef,
/// VecSerStrategy::String>`: one pre-serialized blob, which is not parsed into a vector. A
/// [`KeyValueVectorRef`] validates it and iterates the pairs in place, borrowing from the
/// message. [`Default`] is `GetKeyValuesReply()`.
///
/// Absent: `GetKeyValuesReply() : version(invalidVersion), more(false), cached(false)` over
/// `LoadBalancedReply()` (7.4 and 8.0): `penalty` is 1.0 and `version` -1, not zero, and `data`
/// is empty. A present reply whose `penalty` or `version` slot is missing reads zero.
#[derive(Wire, Clone, Copy, Debug, PartialEq)]
#[fdb(file_identifier = 1783066, absent = "default")]
pub struct GetKeyValuesReply<'a> {
    /// `penalty`.
    pub penalty: f64,
    /// `error`.
    pub error: Option<FdbError>,
    /// `data`.
    pub data: KeyValueVectorRef<'a>,
    /// `version`: the version the read was served at (useful when `latestVersion` was
    /// requested).
    pub version: i64,
    /// `more`: whether the range holds more rows than the reply does.
    pub more: bool,
    /// `cached`.
    pub cached: bool,
    /// `arena`.
    #[fdb(arena)]
    pub arena: (),
}

impl Default for GetKeyValuesReply<'_> {
    /// `GetKeyValuesReply()`: `penalty` 1.0, `version = invalidVersion`, no rows.
    fn default() -> Self {
        GetKeyValuesReply {
            penalty: 1.0,
            error: None,
            data: KeyValueVectorRef::EMPTY,
            version: domain::INVALID_VERSION,
            more: false,
            cached: false,
            arena: (),
        }
    }
}

impl<'a> From<GetKeyValuesReply<'a>> for domain::GetKeyValuesReply<'a> {
    fn from(r: GetKeyValuesReply<'a>) -> Self {
        domain::GetKeyValuesReply {
            penalty: r.penalty,
            error: r.error,
            data: r.data,
            version: r.version,
            more: r.more,
            cached: r.cached,
        }
    }
}

impl<'a> From<&domain::GetKeyValuesReply<'a>> for GetKeyValuesReply<'a> {
    fn from(r: &domain::GetKeyValuesReply<'a>) -> Self {
        GetKeyValuesReply {
            penalty: r.penalty,
            error: r.error,
            data: r.data,
            version: r.version,
            more: r.more,
            cached: r.cached,
            arena: (),
        }
    }
}

/// `WatchValueReply` (`serializer(ar, version, cached)`), file identifier 3: the version at
/// which the watched key stopped having the watched value.
///
/// Absent: `WatchValueReply() = default` with `bool cached = false` (7.4 and 8.0): zero.
#[derive(Wire, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[fdb(file_identifier = 3)]
pub struct WatchValueReply {
    /// `version`.
    pub version: i64,
    /// `cached`.
    pub cached: bool,
}

impl From<WatchValueReply> for domain::WatchValueReply {
    fn from(r: WatchValueReply) -> Self {
        domain::WatchValueReply {
            version: r.version,
            cached: r.cached,
        }
    }
}

impl From<&domain::WatchValueReply> for WatchValueReply {
    fn from(r: &domain::WatchValueReply) -> Self {
        WatchValueReply {
            version: r.version,
            cached: r.cached,
        }
    }
}

// ---------------------------------------------------------------------------------------
// Commit.

/// `ClientTrCommitCostEstimation` (`serializer(ar, opsCount, writeCosts, clearIdxCosts,
/// expensiveCostEstCount)`); `clearIdxCosts` is a `std::deque<std::pair<int, uint64_t>>`.
///
/// Absent: every member has a `= 0` initializer or is empty: zero.
#[derive(Wire, Clone, Debug, Default, PartialEq, Eq)]
pub struct ClientTrCommitCostEstimation {
    /// `opsCount`.
    pub ops_count: i32,
    /// `writeCosts`.
    pub write_costs: u64,
    /// `clearIdxCosts`.
    pub clear_idx_costs: Vec<Pair<i32, u64>>,
    /// `expensiveCostEstCount`.
    pub expensive_cost_est_count: u32,
}

impl From<&domain::CommitCostEstimation> for ClientTrCommitCostEstimation {
    fn from(c: &domain::CommitCostEstimation) -> Self {
        ClientTrCommitCostEstimation {
            ops_count: c.ops_count,
            write_costs: c.write_costs,
            clear_idx_costs: c
                .clear_idx_costs
                .iter()
                .map(|&(i, cost)| Pair::new(i, cost))
                .collect(),
            expensive_cost_est_count: c.expensive_cost_est_count,
        }
    }
}

impl From<ClientTrCommitCostEstimation> for domain::CommitCostEstimation {
    fn from(c: ClientTrCommitCostEstimation) -> Self {
        domain::CommitCostEstimation {
            ops_count: c.ops_count,
            write_costs: c.write_costs,
            clear_idx_costs: c
                .clear_idx_costs
                .into_iter()
                .map(|p| (p.first, p.second))
                .collect(),
            expensive_cost_est_count: c.expensive_cost_est_count,
        }
    }
}

/// `CommitTransactionRef` (the flatbuffers branch: `serializer(ar, read_conflict_ranges,
/// write_conflict_ranges, mutations, read_snapshot, report_conflicting_keys, lock_aware,
/// spanContext, tenantIds)`).
///
/// Absent: `CommitTransactionRef() = default` with `read_snapshot = 0` and `false` flags
/// (7.4 and 8.0): zero.
#[derive(Wire, Clone, Debug, Default, PartialEq, Eq)]
pub struct CommitTransactionRef<'a> {
    /// `read_conflict_ranges`.
    pub read_conflict_ranges: Vec<KeyRangeRef<'a>>,
    /// `write_conflict_ranges`.
    pub write_conflict_ranges: Vec<KeyRangeRef<'a>>,
    /// `mutations`.
    pub mutations: Vec<MutationRef<'a>>,
    /// `read_snapshot`.
    pub read_snapshot: i64,
    /// `report_conflicting_keys`.
    pub report_conflicting_keys: bool,
    /// `lock_aware`.
    pub lock_aware: bool,
    /// `spanContext`.
    pub span_context: Option<SpanContext>,
    /// `tenantIds`: set by the commit proxy, never by a client.
    pub tenant_ids: Option<Vec<i64>>,
}

impl<'a> From<&'a domain::Commit> for CommitTransactionRef<'a> {
    fn from(c: &'a domain::Commit) -> Self {
        CommitTransactionRef {
            read_conflict_ranges: c
                .read_conflict_ranges
                .iter()
                .map(KeyRangeRef::from)
                .collect(),
            write_conflict_ranges: c
                .write_conflict_ranges
                .iter()
                .map(KeyRangeRef::from)
                .collect(),
            mutations: c.mutations.iter().map(MutationRef::from).collect(),
            read_snapshot: c.read_snapshot,
            report_conflicting_keys: c.report_conflicting_keys,
            lock_aware: c.lock_aware,
            span_context: c.transaction_span_context,
            tenant_ids: None,
        }
    }
}

impl CommitTransactionRef<'_> {
    /// Sets the transaction fields of `commit` (`tenantIds` is dropped).
    pub(crate) fn fill_domain(self, commit: &mut domain::Commit) {
        commit.read_conflict_ranges = self
            .read_conflict_ranges
            .into_iter()
            .map(Into::into)
            .collect();
        commit.write_conflict_ranges = self
            .write_conflict_ranges
            .into_iter()
            .map(Into::into)
            .collect();
        commit.mutations = self.mutations.into_iter().map(Into::into).collect();
        commit.read_snapshot = self.read_snapshot;
        commit.report_conflicting_keys = self.report_conflicting_keys;
        commit.lock_aware = self.lock_aware;
        commit.transaction_span_context = self.span_context;
    }
}

/// `CommitID` (`serializer(ar, version, txnBatchId, metadataVersion,
/// conflictingKRIndices)`).
///
/// Absent: `CommitID() : version(invalidVersion), txnBatchId(0)` (7.4 and 8.0): `version` is
/// -1, not zero. [`Default`] is that too.
#[derive(Wire, Clone, Debug, PartialEq, Eq)]
#[fdb(file_identifier = 14254927, absent = "default")]
pub struct CommitID<'a> {
    /// `version`.
    pub version: i64,
    /// `txnBatchId`.
    pub txn_batch_id: u16,
    /// `metadataVersion`.
    pub metadata_version: Option<&'a [u8]>,
    /// `conflictingKRIndices`.
    pub conflicting_kr_indices: Option<Vec<i32>>,
}

impl Default for CommitID<'_> {
    /// `CommitID()`: `version = invalidVersion` (-1).
    fn default() -> Self {
        CommitID {
            version: domain::INVALID_VERSION,
            txn_batch_id: 0,
            metadata_version: None,
            conflicting_kr_indices: None,
        }
    }
}

impl<'a> From<&'a domain::CommitId> for CommitID<'a> {
    fn from(c: &'a domain::CommitId) -> Self {
        CommitID {
            version: c.version,
            txn_batch_id: c.txn_batch_id,
            metadata_version: c.metadata_version.as_deref(),
            conflicting_kr_indices: c.conflicting_kr_indices.clone(),
        }
    }
}

impl From<CommitID<'_>> for domain::CommitId {
    fn from(c: CommitID<'_>) -> Self {
        domain::CommitId {
            version: c.version,
            txn_batch_id: c.txn_batch_id,
            metadata_version: c.metadata_version.map(<[u8]>::to_vec),
            conflicting_kr_indices: c.conflicting_kr_indices,
        }
    }
}
