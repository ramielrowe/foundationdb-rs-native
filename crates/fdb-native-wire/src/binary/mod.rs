//! Sans-IO codec for FDB's legacy binary format: `BinaryWriter` and `BinaryReader`
//! (`flow/include/flow/serialize.h`), which FDB 7.4 and 8.0 still use in a few places next
//! to the flatbuffers dialect of the crate root.
//!
//! # The format
//!
//! There is no framing, schema or alignment: a value is its bytes, and a struct is its
//! members back to back in `serializer(ar, a, b, c)` argument order.
//!
//! | C++ | Rust | Bytes |
//! |---|---|---|
//! | `int8_t`..`int64_t`, `uint8_t`..`uint64_t`, `double` | `i8`..`i64`, `u8`..`u64`, `f64` | little-endian, unpadded (the raw memory) |
//! | `bool` | `bool` | one byte, 0 or 1 |
//! | `enum` | [`binary_enum!`](crate::binary::binary_enum) | the underlying integer type |
//! | `ProtocolVersion` | [`ProtocolVersion`](crate::ProtocolVersion) | `versionWithFlags()` as a `u64` |
//! | `UID` | [`Uid`](crate::Uid) | `first` then `second`, two `u64` (16 bytes; never versioned) |
//! | `Error` | [`FdbError`](crate::FdbError) | the code, a `u16` |
//! | `StringRef`, `Standalone<StringRef>`, `std::string` | `&[u8]`/`Vec<u8>`, `&str`/`String` | a 4-byte count, the bytes |
//! | `std::vector<T>`, `VectorRef<T>` | `Vec<T>`, `&[T]` | a 4-byte count, the elements |
//! | `std::array<T, N>` | `[T; N]` | the elements, no count |
//! | `std::set<T>` | `BTreeSet<T>` | a 4-byte count, the elements in order |
//! | `std::map<K, V>`, `flat_map<K, V>` | `BTreeMap<K, V>` | a 4-byte count, each key then its value |
//! | `Optional<T>` | `Option<T>` | a `bool` (present), then the `T` if it is |
//! | `std::pair<A, B>` | `(A, B)` | `A` then `B` |
//! | `std::variant<...>` | an enum, see [`Writer::write_variant_index`] | a `u8` index, then the alternative |
//! | a struct with `serialize(Ar&)` | [`Encode`] and [`Decode`] | its members |
//! | `Arena` | (nothing) | nothing |
//!
//! FDB's headers cannot write a `std::tuple`, a `float` or a `Void` (no zero-argument
//! `serializer`) and cannot read a `std::deque` (it calls `reserve`); and `load` and `save`
//! of `ErrorOr<T>` (`flow.h`) disagree about which error code means "a value follows", so it
//! cannot round-trip. None of them is implemented here.
//! Tuples `(A, B, C, ...)` model a struct of those members.
//!
//! **The version option.** An archive is made with one of three options (see
//! [`Versioning`] for writing and [`ReadVersioning`] for reading): `Unversioned()` (no
//! version; only for scalars and UIDs, which is why it is used for keys), `AssumeVersion(v)`
//! (nothing on the wire; the code knows `v`) and `IncludeVersion(v)` (a `u64` protocol
//! version first, which the reader checks against its release's limit and then exposes
//! through [`Reader::protocol_version`] so that a struct can read the layout that version
//! wrote). **If the included version has the object-serializer flag, the bytes after it are a
//! flatbuffers-dialect message, not binary**: `BinaryReader` hands them to an `ObjectReader`.
//! Read such a value with [`Reader::include_version`] and give [`Reader::remaining`] to the
//! flatbuffers [`Reader`](crate::Reader).
//!
//! # Names
//!
//! The types mirror the flatbuffers side: [`Encode`] and [`Decode`] (and [`DecodeOwned`])
//! are this format's `Encode` and `Decode`, [`Writer`] and [`Reader`] its `Writer` and
//! `Reader`, [`EncodeError`] and [`DecodeError`] its errors. They are different types from
//! the crate-root ones of the same names; import them from `binary`. Nesting is limited by
//! the crate's [`MAX_DEPTH`](crate::MAX_DEPTH), as for tables.
//!
//! # Where the protocol uses the binary format
//!
//! Found by reading `release-7.4` and `release-8.0` of FDB (`BinaryWriter`, `BinaryReader`,
//! `PacketWriter`, `ArenaReader`, `AssumeVersion` and `IncludeVersion` in `flow`,
//! `fdbrpc`, `fdbclient` and the client-visible parts of `fdbserver`); the needed rows are the
//! same on the two releases. FlowTransport packet bodies and every request and reply are
//! flatbuffers, not binary. This crate has only the generic codec; the types for the rows a
//! client needs live where they are used: `ConnectPacket` in `fdb-native-transport` and the
//! rest in `fdb-native-schema`.
//!
//! "Needed" means a v1 client (reads, writes, commits, watches, retry, `SPAN_PARENT`; no
//! backup, management or tracing tools) must produce or parse it.
//!
//! | Usage site | Needed | Why |
//! |---|---|---|
//! | `ConnectPacket`, the handshake on every connection (`FlowTransport`): a 44-byte raw struct written with `PacketWriter(Unversioned())` and read with `BinaryReader(AssumeVersion(the packet's own version))`. `fdb_native_transport::ConnectPacket` | **yes** | Both sides send it first; nothing else can be exchanged until it is. The same struct in 7.4 and 8.0. |
//! | The destination token in front of every packet body: `wr << destination.token`, `reader >> token`. After the `u32` length and the `u64` XXH3 checksum (absent on TLS), which are raw words, a [`Uid`](crate::Uid) precedes the flatbuffers message | **yes** | Routes each message to an endpoint. |
//! | The self-conflict key `\xff/SC/` + UID, `Transaction::makeSelfConflicting`, added as a read and a write conflict range to a commit that is not `causalWriteRisky` and has no intersecting read and write ranges; with an idempotency id, `\xff/SC/` + the id's raw bytes is added unconditionally. `fdb_native_schema::self_conflict_key` and `self_conflict_key_for_idempotency_id` | **yes** | Without it a commit is not self-conflicting, which changes `commit_unknown_result` handling. The format is not parsed by the server (it is only a conflict range), but a faithful client writes the same bytes. |
//! | Idempotency-id records, `\xff\x02/idmp/` keys and values (`IdempotencyIdKVBuilder`), read by `determineCommitStatus` after `commit_unknown_result`. `fdb_native_schema::IdempotencyValue`, `idempotency_key` | only when the `IDEMPOTENCY_ID` or `AUTOMATIC_IDEMPOTENCY` options are supported | The client scans the records to learn whether its commit happened. Absent for a client that does not support those options. |
//! | `SPAN_PARENT` transaction option value: `BinaryReader::fromStringRef<SpanContext>(value, IncludeVersion())`, 33 bytes. `fdb_native_schema::SpanContext` | only to accept the option | The option is optional; the span context in request headers is flatbuffers. |
//! | `GetHealthMetricsReply.serialized`: a `HealthMetrics` pre-serialized with `BinaryWriter(IncludeVersion())` and carried as a string inside the flatbuffers reply, in 7.4 and 8.0 (`CommitProxyInterface.h`, moved to `GrvProxyInterface.h`). `fdb_native_schema::HealthMetrics` | only for `getHealthMetrics` | The one binary blob inside a flatbuffers message a client can receive. `CachedSerialization<ClientDBInfo>` is not another: on the 7.4/8.0 wire it is a flatbuffers message in a string, not binary. |
//! | `IncludeVersion`-prefixed flatbuffers values: for example `LeaderInfo.serializedInfo` (read with `asyncDeserialize`, behind the status JSON, `worker_interfaces` and the configuration database), tenant map entries, and the `GlobalConfig` history | per value: no for a v1 client | Needs this module only for the 8-byte prefix ([`Reader::include_version`], then [`Reader::remaining`] to the flatbuffers reader). |
//! | `\xff/keyServers/` and `\xff/serverTag/` values (`decodeKeyServersValue`, `decodeServerTagValue`): `IncludeVersion`, then `vector<UID> src, vector<UID> dest, UID srcId[, UID destId]` (7.2 and later; older values hold `vector<Tag>`), and a `Tag` (`int8_t`, `uint16_t`) | no | Read only by the locality API (`getAddressesForKey`, `createCheckpoint`) and tools; ordinary reads locate shards with `GetKeyServerLocationsRequest`. |
//! | `\xff/serverList/` values (`decodeServerListValue`): an `IncludeVersion` prefix with the object-serializer flag, then a flatbuffers `StorageServerInterface` | no | Same readers as above. Needs this module only for the 8-byte prefix. |
//! | `\xff\xff/worker_interfaces/` values: `BinaryWriter::toValue(ClientWorkerInterface, IncludeVersion())`, a struct of endpoints in binary form | no | `fdbcli` (kill, reboot, profiler) only. |
//! | `\xff/conf/` values: replication policies (`serializeReplicationPolicy`) and `\xff/conf/regions` (`StatusObject`), both `IncludeVersion`; `\xff\xff/management/` lock and mode values (`UID`, `bool`, `int64`; some behind a versionstamp prefix) | no | Management API and `fdbcli configure`; a data client reads the other `\xff/conf/` keys as text and `\xff\xff/status/json` as JSON. |
//! | Client transaction sampling, `\xff\x02/fdbClientInfo/client_latency/` (`trLogWriter{IncludeVersion()}`, chunk keys with big-endian counters) | no | Written only when the cluster enables sampling; a client may skip it. |
//! | Backup, DR, restore, change-feed, blob-granule, tag-throttle, data-distribution and audit system keys and values (`BackupAgentBase`, `FileBackupAgent`, `SystemData.cpp`, `TagThrottle`) | no | Server and tool state, not on a transaction's path. |
//! | Global configuration, `ClientDBInfo`, coordinator replies, cluster-file and status contents | no | Flatbuffers (`ObjectWriter`), text or JSON. |
//! | Versionstamps, `\xff/metadataVersion`, the `SetVersionstamped*` position suffix, tuple-encoded keys | no | Raw big-endian or little-endian words that are not `BinaryWriter` output, although `bigEndian64(x)` written through a `BinaryWriter` is how C++ builds some of them. |
//!
//! # Reader limits
//!
//! The reader never panics and does work proportional to its input. Reads are
//! bounds-checked; counts are not trusted ([`Reader`]'s docs); nesting through
//! [`Reader::read`] stops at [`MAX_DEPTH`](crate::MAX_DEPTH); values that take no bytes are
//! budgeted;
//! bytes are borrowed, not copied, when the target type is `&[u8]` or `&str`. It is
//! stricter than FDB's in a few places, each a distinct [`DecodeError`]: a `bool`
//! that is not 0 or 1, an enum value the enum does not list, a count above `i32::MAX`, an
//! out-of-range variant index, and (for [`from_slice`]) trailing bytes.
//!
//! # Example
//!
//! A struct is its members back to back, here a UID, a `u64` and a one-byte flag, behind an
//! `IncludeVersion` prefix (that is the shape of `SpanContext`, the value of the
//! `SPAN_PARENT` option):
//!
//! ```
//! use fdb_native_wire::binary::{
//!     Decode, DecodeError, Encode, EncodeError, ReadVersioning, Reader, Versioning, Writer,
//!     from_slice, to_vec,
//! };
//! use fdb_native_wire::{ProtocolVersion, Uid};
//!
//! #[derive(Debug, PartialEq)]
//! struct Span {
//!     trace_id: Uid,
//!     span_id: u64,
//!     flags: u8,
//! }
//!
//! impl Encode for Span {
//!     fn serialize(&self, w: &mut Writer) -> Result<(), EncodeError> {
//!         w.write(&self.trace_id)?;
//!         w.write(&self.span_id)?;
//!         w.write(&self.flags)
//!     }
//! }
//!
//! impl Decode<'_> for Span {
//!     fn deserialize(r: &mut Reader<'_>) -> Result<Self, DecodeError> {
//!         Ok(Span { trace_id: r.read()?, span_id: r.read()?, flags: r.read()? })
//!     }
//! }
//!
//! let span = Span { trace_id: Uid::new(1, 2), span_id: 3, flags: 1 };
//! let value = to_vec(&span, Versioning::Include(ProtocolVersion::V7_4))?;
//! assert_eq!(value.len(), 8 + 25);
//! assert_eq!(&value[..8], &ProtocolVersion::V7_4.0.to_le_bytes());
//! assert_eq!(from_slice::<Span>(&value, ReadVersioning::INCLUDE_8_0)?, span);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod error;
mod impls;
mod reader;
mod traits;
mod version;
mod writer;

pub use error::{DecodeError, EncodeError};
pub use reader::{Reader, from_slice};
pub use traits::{Decode, DecodeOwned, Encode, binary_enum};
pub use version::{ReadVersioning, Versioning};
pub use writer::{Writer, to_vec};
