//! Encoding and decoding the vertical-slice messages for a [`SchemaVersion`].

use fdb_native_wire::{
    Decode, DecodeError, Encode, EncodeError, EnsureTable, ErrorOr, FileIdentified, decode, encode,
};

use crate::{SchemaVersion, common, domain, v7_4, v8_0};

/// A request a client sends, with its reply: the message pairs of the vertical slice, as
/// [`domain`](crate::domain) types, encoded and decoded with the schema set of a
/// [`SchemaVersion`].
///
/// A request travels as its own message (the request table at the root, with the request's
/// file identifier); its reply as `ErrorOr<EnsureTable<Reply>>` (an `Err` is an FDB error
/// the server sent instead of a reply). Client code uses
/// [`encode_request`](Request::encode_request) and [`decode_reply`](Request::decode_reply);
/// the other two are a server's side, for tests and fakes.
///
/// `now` is the local clock (seconds, as FDB's `now()`), used only where a message carries a
/// duration relative to its writer's clock: the tag throttle limits of a
/// [`GetReadVersion`](domain::GetReadVersion) reply. Other replies ignore it.
///
/// | request | to | reply |
/// | --- | --- | --- |
/// | [`OpenDatabaseCoord`](domain::OpenDatabaseCoord) | a coordinator, `UID(-1, 4)` | [`ClientDbInfo`](domain::ClientDbInfo) |
/// | [`GetReadVersion`](domain::GetReadVersion) | a GRV proxy, `getConsistentReadVersion` | [`ReadVersion`](domain::ReadVersion) |
/// | [`GetKeyServerLocations`](domain::GetKeyServerLocations) | a commit proxy, `getKeyServersLocations` | [`KeyServerLocations`](domain::KeyServerLocations) |
/// | [`GetValue`](domain::GetValue) | a storage server, `getValue` | [`GetValueReply`](domain::GetValueReply) (borrows the value) |
/// | [`Commit`](domain::Commit) | a commit proxy, `commit` | [`CommitId`](domain::CommitId) |
///
/// ```
/// use fdb_native_schema::domain::{GetValue, GetValueReply};
/// use fdb_native_schema::{Request, SchemaVersion};
/// use fdb_native_wire::{Uid, VersionVector};
///
/// let request = GetValue {
///     key: b"hello".to_vec(),
///     version: 100,
///     tags: None,
///     reply: Uid::new(1, 2),
///     span_context: Default::default(),
///     options: None,
///     ss_latest_commit_versions: VersionVector::default(),
/// };
/// for version in SchemaVersion::ALL {
///     let bytes = request.encode_request(version)?;
///     assert_eq!(GetValue::decode_request(version, &bytes)?, request);
///
///     let reply = GetValueReply { value: Some(b"world"), ..Default::default() };
///     let bytes = GetValue::encode_reply(version, &Ok(reply), 0.0)?;
///     assert_eq!(GetValue::decode_reply(version, &bytes, 0.0)?, Ok(reply));
/// }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub trait Request: Sized {
    /// The reply, which may borrow from the reply message.
    type Reply<'de>;

    /// The request as a message for a server of `version`.
    fn encode_request(&self, version: SchemaVersion) -> Result<Vec<u8>, EncodeError>;

    /// A request message from a client of `version`.
    fn decode_request(version: SchemaVersion, bytes: &[u8]) -> Result<Self, DecodeError>;

    /// The reply envelope as a server of `version` writes it at `now`.
    fn encode_reply(
        version: SchemaVersion,
        reply: &ErrorOr<Self::Reply<'_>>,
        now: f64,
    ) -> Result<Vec<u8>, EncodeError>;

    /// A reply envelope from a server of `version`, received at `now`.
    fn decode_reply(
        version: SchemaVersion,
        bytes: &[u8],
        now: f64,
    ) -> Result<ErrorOr<Self::Reply<'_>>, DecodeError>;
}

/// [`Request::encode_request`].
pub fn encode_request<R: Request>(
    version: SchemaVersion,
    request: &R,
) -> Result<Vec<u8>, EncodeError> {
    request.encode_request(version)
}

/// [`Request::decode_reply`].
pub fn decode_reply<R: Request>(
    version: SchemaVersion,
    bytes: &[u8],
    now: f64,
) -> Result<ErrorOr<R::Reply<'_>>, DecodeError> {
    R::decode_reply(version, bytes, now)
}

/// Decodes `ErrorOr<EnsureTable<W>>` (checking its file identifier) and converts the reply.
fn decode_envelope<'de, W, D>(
    bytes: &'de [u8],
    convert: impl FnOnce(W) -> Result<D, DecodeError>,
) -> Result<ErrorOr<D>, DecodeError>
where
    W: Decode<'de> + FileIdentified,
{
    match decode::<ErrorOr<EnsureTable<W>>>(bytes)? {
        Ok(EnsureTable(reply)) => convert(reply).map(Ok),
        Err(e) => Ok(Err(e)),
    }
}

/// Encodes `ErrorOr<EnsureTable<W>>`.
fn encode_envelope<'a, D, W: Encode + FileIdentified>(
    reply: &'a ErrorOr<D>,
    convert: impl FnOnce(&'a D) -> W,
) -> Result<Vec<u8>, EncodeError> {
    let envelope: ErrorOr<EnsureTable<W>> = match reply {
        Ok(reply) => Ok(EnsureTable(convert(reply))),
        Err(e) => Err(*e),
    };
    encode(&envelope)
}

impl Request for domain::OpenDatabaseCoord {
    type Reply<'de> = domain::ClientDbInfo;

    /// The same on both versions.
    fn encode_request(&self, _: SchemaVersion) -> Result<Vec<u8>, EncodeError> {
        encode(&common::OpenDatabaseCoordRequest::from(self))
    }

    fn decode_request(_: SchemaVersion, bytes: &[u8]) -> Result<Self, DecodeError> {
        decode::<common::OpenDatabaseCoordRequest>(bytes).map(Into::into)
    }

    fn encode_reply(
        version: SchemaVersion,
        reply: &ErrorOr<domain::ClientDbInfo>,
        _: f64,
    ) -> Result<Vec<u8>, EncodeError> {
        match version {
            SchemaVersion::V7_4 => encode_envelope(reply, v7_4::ClientDBInfo::from),
            SchemaVersion::V8_0 => encode_envelope(reply, v8_0::ClientDBInfo::from),
        }
    }

    fn decode_reply(
        version: SchemaVersion,
        bytes: &[u8],
        _: f64,
    ) -> Result<ErrorOr<domain::ClientDbInfo>, DecodeError> {
        match version {
            SchemaVersion::V7_4 => decode_envelope(bytes, |r: v7_4::ClientDBInfo| Ok(r.into())),
            SchemaVersion::V8_0 => decode_envelope(bytes, |r: v8_0::ClientDBInfo| Ok(r.into())),
        }
    }
}

impl Request for domain::GetReadVersion {
    type Reply<'de> = domain::ReadVersion;

    fn encode_request(&self, version: SchemaVersion) -> Result<Vec<u8>, EncodeError> {
        match version {
            SchemaVersion::V7_4 => encode(&v7_4::GetReadVersionRequest::from(self)),
            SchemaVersion::V8_0 => encode(&v8_0::GetReadVersionRequest::from(self)),
        }
    }

    fn decode_request(version: SchemaVersion, bytes: &[u8]) -> Result<Self, DecodeError> {
        match version {
            SchemaVersion::V7_4 => decode::<v7_4::GetReadVersionRequest>(bytes).map(Into::into),
            SchemaVersion::V8_0 => decode::<v8_0::GetReadVersionRequest>(bytes).map(Into::into),
        }
    }

    fn encode_reply(
        version: SchemaVersion,
        reply: &ErrorOr<domain::ReadVersion>,
        now: f64,
    ) -> Result<Vec<u8>, EncodeError> {
        match version {
            SchemaVersion::V7_4 => {
                encode_envelope(reply, |r| v7_4::GetReadVersionReply::from_domain(r, now))
            }
            SchemaVersion::V8_0 => {
                encode_envelope(reply, |r| v8_0::GetReadVersionReply::from_domain(r, now))
            }
        }
    }

    fn decode_reply(
        version: SchemaVersion,
        bytes: &[u8],
        now: f64,
    ) -> Result<ErrorOr<domain::ReadVersion>, DecodeError> {
        match version {
            SchemaVersion::V7_4 => {
                decode_envelope(bytes, |r: v7_4::GetReadVersionReply| Ok(r.to_domain(now)))
            }
            SchemaVersion::V8_0 => {
                decode_envelope(bytes, |r: v8_0::GetReadVersionReply| Ok(r.to_domain(now)))
            }
        }
    }
}

impl Request for domain::GetKeyServerLocations {
    type Reply<'de> = domain::KeyServerLocations;

    fn encode_request(&self, version: SchemaVersion) -> Result<Vec<u8>, EncodeError> {
        match version {
            SchemaVersion::V7_4 => encode(&v7_4::GetKeyServerLocationsRequest::from(self)),
            SchemaVersion::V8_0 => encode(&v8_0::GetKeyServerLocationsRequest::from(self)),
        }
    }

    fn decode_request(version: SchemaVersion, bytes: &[u8]) -> Result<Self, DecodeError> {
        match version {
            SchemaVersion::V7_4 => {
                decode::<v7_4::GetKeyServerLocationsRequest>(bytes).map(Into::into)
            }
            SchemaVersion::V8_0 => {
                decode::<v8_0::GetKeyServerLocationsRequest>(bytes).map(Into::into)
            }
        }
    }

    /// The same on both versions.
    fn encode_reply(
        _: SchemaVersion,
        reply: &ErrorOr<domain::KeyServerLocations>,
        _: f64,
    ) -> Result<Vec<u8>, EncodeError> {
        encode_envelope(reply, common::GetKeyServerLocationsReply::from_domain)
    }

    /// The same on both versions. The shards are read straight from the message into the
    /// domain type.
    fn decode_reply(
        _: SchemaVersion,
        bytes: &[u8],
        _: f64,
    ) -> Result<ErrorOr<domain::KeyServerLocations>, DecodeError> {
        decode_envelope(bytes, |r: common::GetKeyServerLocationsReply| r.try_into())
    }
}

impl Request for domain::GetValue {
    type Reply<'de> = domain::GetValueReply<'de>;

    fn encode_request(&self, version: SchemaVersion) -> Result<Vec<u8>, EncodeError> {
        match version {
            SchemaVersion::V7_4 => encode(&v7_4::GetValueRequest::from(self)),
            SchemaVersion::V8_0 => encode(&v8_0::GetValueRequest::from(self)),
        }
    }

    fn decode_request(version: SchemaVersion, bytes: &[u8]) -> Result<Self, DecodeError> {
        match version {
            SchemaVersion::V7_4 => decode::<v7_4::GetValueRequest>(bytes).map(Into::into),
            SchemaVersion::V8_0 => decode::<v8_0::GetValueRequest>(bytes).map(Into::into),
        }
    }

    /// The same on both versions.
    fn encode_reply(
        _: SchemaVersion,
        reply: &ErrorOr<domain::GetValueReply<'_>>,
        _: f64,
    ) -> Result<Vec<u8>, EncodeError> {
        encode_envelope(reply, common::GetValueReply::from)
    }

    /// The same on both versions; the value borrows from `bytes`.
    fn decode_reply(
        _: SchemaVersion,
        bytes: &[u8],
        _: f64,
    ) -> Result<ErrorOr<domain::GetValueReply<'_>>, DecodeError> {
        decode_envelope(bytes, |r: common::GetValueReply| Ok(r.into()))
    }
}

impl Request for domain::Commit {
    type Reply<'de> = domain::CommitId;

    fn encode_request(&self, version: SchemaVersion) -> Result<Vec<u8>, EncodeError> {
        match version {
            SchemaVersion::V7_4 => encode(&v7_4::CommitTransactionRequest::from(self)),
            SchemaVersion::V8_0 => encode(&v8_0::CommitTransactionRequest::from(self)),
        }
    }

    fn decode_request(version: SchemaVersion, bytes: &[u8]) -> Result<Self, DecodeError> {
        match version {
            SchemaVersion::V7_4 => decode::<v7_4::CommitTransactionRequest>(bytes).map(Into::into),
            SchemaVersion::V8_0 => decode::<v8_0::CommitTransactionRequest>(bytes).map(Into::into),
        }
    }

    /// The same on both versions.
    fn encode_reply(
        _: SchemaVersion,
        reply: &ErrorOr<domain::CommitId>,
        _: f64,
    ) -> Result<Vec<u8>, EncodeError> {
        encode_envelope(reply, common::CommitID::from)
    }

    /// The same on both versions.
    fn decode_reply(
        _: SchemaVersion,
        bytes: &[u8],
        _: f64,
    ) -> Result<ErrorOr<domain::CommitId>, DecodeError> {
        decode_envelope(bytes, |r: common::CommitID| Ok(r.into()))
    }
}
