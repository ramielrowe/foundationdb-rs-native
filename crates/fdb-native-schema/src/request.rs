//! Encoding and decoding the vertical-slice messages for a [`SchemaVersion`].

use fdb_native_wire::{
    Decode, DecodeError, Encode, EncodeError, EnsureTable, ErrorOr, FileIdentified, Uid, Writer,
    decode, encode,
};

use crate::{SchemaVersion, common, domain, v7_4, v8_0};

/// A request a client sends, with its reply: the message pairs of the vertical slice, as
/// [`domain`](crate::domain) types, encoded and decoded with the schema set of a
/// [`SchemaVersion`].
///
/// A request travels as its own message (the request table at the root, with the request's
/// file identifier) and carries the token of the endpoint its reply goes to (its
/// `ReplyPromise`). That token is not part of the domain request: the caller (the request
/// layer, which registers the reply endpoint) passes it to
/// [`encode_request`](Request::encode_request), so a retried or duplicated request is sent
/// again unchanged with a fresh token. The reply travels as `ErrorOr<EnsureTable<Reply>>`
/// (an `Err` is an FDB error the server sent instead of a reply).
///
/// Client code uses [`encode_request`](Request::encode_request) (or
/// [`encode_request_into`](Request::encode_request_into), to write straight into a frame
/// buffer) and [`decode_reply`](Request::decode_reply); [`decode_request`](Request::decode_request)
/// and [`encode_reply`](Request::encode_reply) are a server's side, for tests and fakes.
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
///     span_context: Default::default(),
///     options: None,
///     ss_latest_commit_versions: VersionVector::default(),
/// };
/// let reply_token = Uid::new(1, 2);
/// for version in SchemaVersion::ALL {
///     let bytes = request.encode_request(version, reply_token)?;
///     assert_eq!(GetValue::decode_request(version, &bytes)?, (request.clone(), reply_token));
///
///     let reply = GetValueReply { value: Some(b"world"), ..Default::default() };
///     let bytes = GetValue::encode_reply(version, &Ok(reply))?;
///     assert_eq!(GetValue::decode_reply(version, &bytes)?, Ok(reply));
/// }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub trait Request: Sized {
    /// The reply, which may borrow from the reply message.
    type Reply<'de>;

    /// Appends the request, as a message for a server of `version` whose reply goes to the
    /// endpoint with token `reply`, to `out` (which may already hold other bytes, such as a
    /// frame header). On an error `out` is left as it was.
    fn encode_request_into(
        &self,
        version: SchemaVersion,
        reply: Uid,
        out: &mut Vec<u8>,
    ) -> Result<(), EncodeError>;

    /// The request as a message for a server of `version`, whose reply goes to the endpoint
    /// with token `reply`.
    fn encode_request(&self, version: SchemaVersion, reply: Uid) -> Result<Vec<u8>, EncodeError> {
        let mut out = Vec::new();
        self.encode_request_into(version, reply, &mut out)?;
        Ok(out)
    }

    /// A request message from a client of `version`, with its reply token.
    fn decode_request(version: SchemaVersion, bytes: &[u8]) -> Result<(Self, Uid), DecodeError>;

    /// The reply envelope as a server of `version` writes it.
    fn encode_reply(
        version: SchemaVersion,
        reply: &ErrorOr<Self::Reply<'_>>,
    ) -> Result<Vec<u8>, EncodeError>;

    /// A reply envelope from a server of `version`.
    fn decode_reply(
        version: SchemaVersion,
        bytes: &[u8],
    ) -> Result<ErrorOr<Self::Reply<'_>>, DecodeError>;
}

/// [`Request::encode_request`].
pub fn encode_request<R: Request>(
    version: SchemaVersion,
    request: &R,
    reply: Uid,
) -> Result<Vec<u8>, EncodeError> {
    request.encode_request(version, reply)
}

/// [`Request::encode_request_into`].
pub fn encode_request_into<R: Request>(
    version: SchemaVersion,
    request: &R,
    reply: Uid,
    out: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    request.encode_request_into(version, reply, out)
}

/// [`Request::decode_reply`].
pub fn decode_reply<R: Request>(
    version: SchemaVersion,
    bytes: &[u8],
) -> Result<ErrorOr<R::Reply<'_>>, DecodeError> {
    R::decode_reply(version, bytes)
}

/// Appends `value` as the only member of a message with `T`'s file identifier.
fn encode_into<T: Encode + FileIdentified>(
    value: &T,
    out: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let mut w = Writer::new();
    let mut root = w.table();
    root.field(value)?;
    let root = root.finish()?;
    w.finish_into(out, root, T::FILE_IDENTIFIER.value())
}

/// Decodes a request of type `W` and its reply token.
fn decode_request<'de, W, D>(
    bytes: &'de [u8],
    token: impl FnOnce(&W) -> Uid,
) -> Result<(D, Uid), DecodeError>
where
    W: Decode<'de> + FileIdentified + Into<D>,
{
    let wire: W = decode(bytes)?;
    let reply = token(&wire);
    Ok((wire.into(), reply))
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
    fn encode_request_into(
        &self,
        _: SchemaVersion,
        reply: Uid,
        out: &mut Vec<u8>,
    ) -> Result<(), EncodeError> {
        encode_into(
            &common::OpenDatabaseCoordRequest::from_domain(self, reply),
            out,
        )
    }

    fn decode_request(_: SchemaVersion, bytes: &[u8]) -> Result<(Self, Uid), DecodeError> {
        decode_request(bytes, |r: &common::OpenDatabaseCoordRequest| {
            r.reply.token()
        })
    }

    fn encode_reply(
        version: SchemaVersion,
        reply: &ErrorOr<domain::ClientDbInfo>,
    ) -> Result<Vec<u8>, EncodeError> {
        match version {
            SchemaVersion::V7_4 => encode_envelope(reply, v7_4::ClientDBInfo::from),
            SchemaVersion::V8_0 => encode_envelope(reply, v8_0::ClientDBInfo::from),
        }
    }

    fn decode_reply(
        version: SchemaVersion,
        bytes: &[u8],
    ) -> Result<ErrorOr<domain::ClientDbInfo>, DecodeError> {
        match version {
            SchemaVersion::V7_4 => decode_envelope(bytes, |r: v7_4::ClientDBInfo| Ok(r.into())),
            SchemaVersion::V8_0 => decode_envelope(bytes, |r: v8_0::ClientDBInfo| Ok(r.into())),
        }
    }
}

impl Request for domain::GetReadVersion {
    type Reply<'de> = domain::ReadVersion;

    fn encode_request_into(
        &self,
        version: SchemaVersion,
        reply: Uid,
        out: &mut Vec<u8>,
    ) -> Result<(), EncodeError> {
        match version {
            SchemaVersion::V7_4 => {
                encode_into(&v7_4::GetReadVersionRequest::from_domain(self, reply), out)
            }
            SchemaVersion::V8_0 => {
                encode_into(&v8_0::GetReadVersionRequest::from_domain(self, reply), out)
            }
        }
    }

    fn decode_request(version: SchemaVersion, bytes: &[u8]) -> Result<(Self, Uid), DecodeError> {
        match version {
            SchemaVersion::V7_4 => {
                decode_request(bytes, |r: &v7_4::GetReadVersionRequest| r.reply.token())
            }
            SchemaVersion::V8_0 => {
                decode_request(bytes, |r: &v8_0::GetReadVersionRequest| r.reply.token())
            }
        }
    }

    fn encode_reply(
        version: SchemaVersion,
        reply: &ErrorOr<domain::ReadVersion>,
    ) -> Result<Vec<u8>, EncodeError> {
        match version {
            SchemaVersion::V7_4 => encode_envelope(reply, v7_4::GetReadVersionReply::from_domain),
            SchemaVersion::V8_0 => encode_envelope(reply, v8_0::GetReadVersionReply::from_domain),
        }
    }

    fn decode_reply(
        version: SchemaVersion,
        bytes: &[u8],
    ) -> Result<ErrorOr<domain::ReadVersion>, DecodeError> {
        match version {
            SchemaVersion::V7_4 => {
                decode_envelope(bytes, |r: v7_4::GetReadVersionReply| Ok(r.to_domain()))
            }
            SchemaVersion::V8_0 => {
                decode_envelope(bytes, |r: v8_0::GetReadVersionReply| Ok(r.to_domain()))
            }
        }
    }
}

impl Request for domain::GetKeyServerLocations {
    type Reply<'de> = domain::KeyServerLocations;

    fn encode_request_into(
        &self,
        version: SchemaVersion,
        reply: Uid,
        out: &mut Vec<u8>,
    ) -> Result<(), EncodeError> {
        match version {
            SchemaVersion::V7_4 => encode_into(
                &v7_4::GetKeyServerLocationsRequest::from_domain(self, reply),
                out,
            ),
            SchemaVersion::V8_0 => encode_into(
                &v8_0::GetKeyServerLocationsRequest::from_domain(self, reply),
                out,
            ),
        }
    }

    fn decode_request(version: SchemaVersion, bytes: &[u8]) -> Result<(Self, Uid), DecodeError> {
        match version {
            SchemaVersion::V7_4 => {
                decode_request(bytes, |r: &v7_4::GetKeyServerLocationsRequest| {
                    r.reply.token()
                })
            }
            SchemaVersion::V8_0 => {
                decode_request(bytes, |r: &v8_0::GetKeyServerLocationsRequest| {
                    r.reply.token()
                })
            }
        }
    }

    /// The same on both versions.
    fn encode_reply(
        _: SchemaVersion,
        reply: &ErrorOr<domain::KeyServerLocations>,
    ) -> Result<Vec<u8>, EncodeError> {
        encode_envelope(reply, common::GetKeyServerLocationsReply::from_domain)
    }

    /// The same on both versions. The shards are read straight from the message into the
    /// domain type.
    fn decode_reply(
        _: SchemaVersion,
        bytes: &[u8],
    ) -> Result<ErrorOr<domain::KeyServerLocations>, DecodeError> {
        decode_envelope(bytes, |r: common::GetKeyServerLocationsReply| r.try_into())
    }
}

impl Request for domain::GetValue {
    type Reply<'de> = domain::GetValueReply<'de>;

    fn encode_request_into(
        &self,
        version: SchemaVersion,
        reply: Uid,
        out: &mut Vec<u8>,
    ) -> Result<(), EncodeError> {
        match version {
            SchemaVersion::V7_4 => {
                encode_into(&v7_4::GetValueRequest::from_domain(self, reply), out)
            }
            SchemaVersion::V8_0 => {
                encode_into(&v8_0::GetValueRequest::from_domain(self, reply), out)
            }
        }
    }

    fn decode_request(version: SchemaVersion, bytes: &[u8]) -> Result<(Self, Uid), DecodeError> {
        match version {
            SchemaVersion::V7_4 => {
                decode_request(bytes, |r: &v7_4::GetValueRequest| r.reply.token())
            }
            SchemaVersion::V8_0 => {
                decode_request(bytes, |r: &v8_0::GetValueRequest| r.reply.token())
            }
        }
    }

    /// The same on both versions.
    fn encode_reply(
        _: SchemaVersion,
        reply: &ErrorOr<domain::GetValueReply<'_>>,
    ) -> Result<Vec<u8>, EncodeError> {
        encode_envelope(reply, common::GetValueReply::from)
    }

    /// The same on both versions; the value borrows from `bytes`.
    fn decode_reply(
        _: SchemaVersion,
        bytes: &[u8],
    ) -> Result<ErrorOr<domain::GetValueReply<'_>>, DecodeError> {
        decode_envelope(bytes, |r: common::GetValueReply| Ok(r.into()))
    }
}

impl Request for domain::Commit {
    type Reply<'de> = domain::CommitId;

    fn encode_request_into(
        &self,
        version: SchemaVersion,
        reply: Uid,
        out: &mut Vec<u8>,
    ) -> Result<(), EncodeError> {
        match version {
            SchemaVersion::V7_4 => encode_into(
                &v7_4::CommitTransactionRequest::from_domain(self, reply),
                out,
            ),
            SchemaVersion::V8_0 => encode_into(
                &v8_0::CommitTransactionRequest::from_domain(self, reply),
                out,
            ),
        }
    }

    fn decode_request(version: SchemaVersion, bytes: &[u8]) -> Result<(Self, Uid), DecodeError> {
        match version {
            SchemaVersion::V7_4 => {
                decode_request(bytes, |r: &v7_4::CommitTransactionRequest| r.reply.token())
            }
            SchemaVersion::V8_0 => {
                decode_request(bytes, |r: &v8_0::CommitTransactionRequest| r.reply.token())
            }
        }
    }

    /// The same on both versions.
    fn encode_reply(
        _: SchemaVersion,
        reply: &ErrorOr<domain::CommitId>,
    ) -> Result<Vec<u8>, EncodeError> {
        encode_envelope(reply, common::CommitID::from)
    }

    /// The same on both versions.
    fn decode_reply(
        _: SchemaVersion,
        bytes: &[u8],
    ) -> Result<ErrorOr<domain::CommitId>, DecodeError> {
        decode_envelope(bytes, |r: common::CommitID| Ok(r.into()))
    }
}
