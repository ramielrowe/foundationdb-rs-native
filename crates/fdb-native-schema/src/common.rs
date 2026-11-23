//! Wire types whose layout is the same on 7.4 and 8.0. Both schema sets re-export them; a
//! type that changes in a later release gets its own definition in that release's module.

use std::collections::BTreeMap;

use fdb_native_wire::{
    Decode, DecodeError, Encode, EncodeError, FdbError, NetworkAddress, ObjectRef, Reader,
    ReplyPromise, RequestStream, Shape, Tag, Uid, VectorView, Wire, Writer,
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
/// is what is written. Decoding rejects a compressed `begin` that does not end in `\x00`
/// (C++ `ASSERT`s, killing the process) and an inverted range, `begin > end` (C++ throws
/// `inverted_range`). Encoding does not check for inversion.
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
    /// `(begin, end)`.
    pub fn wire_form(&self) -> (&'a [u8], &'a [u8]) {
        if equals_key_after(self.begin, self.end) {
            (self.end, b"")
        } else {
            (self.begin, self.end)
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
        let (begin, end) = self.wire_form();
        let mut t = w.table();
        t.field(&begin)?;
        t.field(&end)?;
        t.finish()
    }
}

impl<'de: 'a, 'a> Decode<'de> for KeyRangeRef<'a> {
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
/// [`MutationRef::wire_form`] is what is written. Decoding rejects a compressed `param1` that
/// does not end in `\x00` (C++ logs a `SevError` and marks the mutation corrupted).
///
/// **Checksums.** With the `ENABLE_MUTATION_CHECKSUM` client knob (off by default), C++
/// sets bit 7 of the type (and bit 6 with an accumulative checksum index) and appends the
/// checksum to `param2`. Neither is interpreted here: such a mutation keeps its flagged type
/// and suffixed `param2` exactly as received (its type is not `ClearRange`, so it is not
/// expanded) and is re-encoded unchanged. This type never adds checksums.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct MutationRef<'a> {
    /// `type` (`MutationRef::Type`, plus the checksum flags).
    pub kind: u8,
    /// `param1`.
    pub param1: &'a [u8],
    /// `param2`.
    pub param2: &'a [u8],
}

impl<'a> MutationRef<'a> {
    /// `ClearRange`.
    pub const CLEAR_RANGE: u8 = 1;

    /// A mutation.
    pub const fn new(kind: u8, param1: &'a [u8], param2: &'a [u8]) -> Self {
        MutationRef {
            kind,
            param1,
            param2,
        }
    }

    /// The `(param1, param2)` the writer sends: `(param2, "")` for a single-key clear, else
    /// `(param1, param2)`.
    pub fn wire_form(&self) -> (&'a [u8], &'a [u8]) {
        if self.kind == Self::CLEAR_RANGE && equals_key_after(self.param1, self.param2) {
            (self.param2, b"")
        } else {
            (self.param1, self.param2)
        }
    }

    /// The mutation a reader makes of `(kind, param1, param2)` as received.
    pub fn from_wire_form(
        kind: u8,
        param1: &'a [u8],
        param2: &'a [u8],
    ) -> Result<Self, DecodeError> {
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
        })
    }
}

impl Wire for MutationRef<'_> {
    const SHAPE: Shape = Shape::Table;
}

impl Encode for MutationRef<'_> {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let (param1, param2) = self.wire_form();
        let mut t = w.table();
        t.field(&self.kind)?;
        t.field(&param1)?;
        t.field(&param2)?;
        t.finish()
    }
}

impl<'de: 'a, 'a> Decode<'de> for MutationRef<'a> {
    fn absent() -> Self {
        MutationRef::default()
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
        domain::KeyRange::new(r.begin, r.end)
    }
}

impl<'a> From<&'a domain::Mutation> for MutationRef<'a> {
    fn from(m: &'a domain::Mutation) -> Self {
        MutationRef::new(m.kind.0, &m.param1, &m.param2)
    }
}

impl From<MutationRef<'_>> for domain::Mutation {
    fn from(m: MutationRef<'_>) -> Self {
        domain::Mutation::new(domain::MutationType(m.kind), m.param1, m.param2)
    }
}

// ---------------------------------------------------------------------------------------
// Interfaces.

/// `LocalityData` (`serializer(ar, _data)`): a `std::map<Key, Optional<Value>>`.
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
#[derive(Wire, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClientVersionRef<'a> {
    /// `clientVersion`.
    pub client_version: &'a [u8],
    /// `sourceVersion`.
    pub source_version: &'a [u8],
    /// `protocolVersion`.
    pub protocol_version: &'a [u8],
}

/// `Hostname` (`serializer(ar, host, service, isTLS)`).
#[derive(Wire, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Hostname<'a> {
    /// `host`.
    pub host: &'a [u8],
    /// `service`.
    pub service: &'a [u8],
    /// `isTLS`.
    pub is_tls: bool,
}

/// `OpenDatabaseCoordRequest` (`serializer(ar, issues, supportedVersions, traceLogGroup,
/// knownClientInfoID, clusterKey, coordinators, reply, hostnames, internal)`).
///
/// The reply is declared `ReplyPromise<CachedSerialization<ClientDBInfo>>`, but it arrives
/// exactly as `ErrorOr<EnsureTable<ClientDBInfo>>`, with the version's `ClientDBInfo`.
#[derive(Wire, Clone, Debug, Default, PartialEq, Eq)]
#[fdb(file_identifier = 214728)]
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

impl<'a> From<&'a domain::OpenDatabaseCoord> for OpenDatabaseCoordRequest<'a> {
    fn from(r: &'a domain::OpenDatabaseCoord) -> Self {
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
            reply: ReplyPromise::new(r.reply),
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
            reply: r.reply.token(),
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
/// `duration` is a local of `serialize()`, `expiration - now()` on the writer's clock. The
/// reader's `expiration` is `now() + duration` on its own clock:
/// [`ClientTagThrottleLimits::to_domain`] and [`ClientTagThrottleLimits::from_domain`] take
/// that `now`.
#[derive(Wire, Clone, Copy, Debug, Default, PartialEq)]
pub struct ClientTagThrottleLimits {
    /// `tpsRate`.
    pub tps_rate: f64,
    /// `duration`: seconds until the limit expires.
    pub duration: f64,
}

impl ClientTagThrottleLimits {
    /// The limits with `expiration = now + duration`.
    pub fn to_domain(self, now: f64) -> domain::TagThrottleLimits {
        domain::TagThrottleLimits {
            tps_rate: self.tps_rate,
            expiration: now + self.duration,
        }
    }

    /// The limits as written at `now`: `duration = expiration - now`.
    pub fn from_domain(limits: &domain::TagThrottleLimits, now: f64) -> Self {
        ClientTagThrottleLimits {
            tps_rate: limits.tps_rate,
            duration: limits.expiration - now,
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
/// value, cached)`). The value borrows from the message.
#[derive(Wire, Clone, Copy, Debug, Default, PartialEq)]
#[fdb(file_identifier = 1378929)]
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

// ---------------------------------------------------------------------------------------
// Commit.

/// `ClientTrCommitCostEstimation` (`serializer(ar, opsCount, writeCosts, clearIdxCosts,
/// expensiveCostEstCount)`); `clearIdxCosts` is a `std::deque<std::pair<int, uint64_t>>`.
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
#[derive(Wire, Clone, Debug, Default, PartialEq, Eq)]
#[fdb(file_identifier = 14254927)]
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
