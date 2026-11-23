//! FlowTransport's own messages and well-known tokens.
//!
//! Citations `7.4:N` are lines of `fdbrpc/FlowTransport.actor.cpp` on `release-7.4` and `8.0:N`
//! of `fdbrpc/FlowTransport.cpp` on `release-8.0`, unless another file is named.

use fdb_native_wire::{EnsureTable, ErrorOr, ReplyPromise, Uid, Void, Wire};

/// FlowTransport's well-known endpoint tokens: `Endpoint::wellKnownToken(id)` is
/// `UID(-1, id)` (`fdbrpc/include/fdbrpc/FlowTransport.h`, 7.4:163 and 8.0:172), so the first
/// half is `u64::MAX` and the second is the index.
///
/// The indices are the `WLTOKEN_*` enums: the first three in `FlowTransport.h` (7.4:146, 8.0:148-153,
/// where 8.0 also `static_assert`s them, `FlowTransport.cpp` 8.0:305-308), the rest in
/// `WellKnownEndpoints.h` (`fdbrpc/include/fdbrpc/` on 7.4, `fdbclient/include/fdbclient/` on 8.0;
/// the same values on both, `WLTOKEN_PROTOCOL_INFO` pinned at 10 by a `static_assert`).
pub mod well_known {
    use fdb_native_wire::Uid;

    /// `Endpoint::wellKnownToken(id)`: `UID(-1, id)`.
    pub const fn token(id: u32) -> Uid {
        Uid::new(u64::MAX, id as u64)
    }

    /// Whether `token` is a well-known token (its first half is `-1`). C++ never answers a
    /// packet for an unknown well-known token with endpoint-not-found
    /// (`destination.token.first() != -1`, `deliver`, 7.4:1303, 8.0:1376).
    pub const fn is_well_known(token: Uid) -> bool {
        token.first == u64::MAX
    }

    /// `WLTOKEN_ENDPOINT_NOT_FOUND` (0): carries the [`Uid`] of an endpoint the sender does not
    /// have.
    pub const ENDPOINT_NOT_FOUND: Uid = token(0);
    /// `WLTOKEN_PING_PACKET` (1): a [`PingRequest`](super::PingRequest).
    pub const PING_PACKET: Uid = token(1);
    /// `WLTOKEN_UNAUTHORIZED_ENDPOINT` (2): the same payload as endpoint-not-found, for an
    /// endpoint the sender may not call.
    pub const UNAUTHORIZED_ENDPOINT: Uid = token(2);
    /// `WLTOKEN_FIRST_AVAILABLE` (3): the first index the application defines.
    pub const FIRST_AVAILABLE: u32 = 3;

    /// `WLTOKEN_CLIENTLEADERREG_GETLEADER` (3), a coordinator's `ClientLeaderRegInterface`.
    pub const CLIENTLEADERREG_GETLEADER: Uid = token(3);
    /// `WLTOKEN_CLIENTLEADERREG_OPENDATABASE` (4).
    pub const CLIENTLEADERREG_OPENDATABASE: Uid = token(4);
    /// `WLTOKEN_LEADERELECTIONREG_CANDIDACY` (5).
    pub const LEADERELECTIONREG_CANDIDACY: Uid = token(5);
    /// `WLTOKEN_LEADERELECTIONREG_ELECTIONRESULT` (6).
    pub const LEADERELECTIONREG_ELECTIONRESULT: Uid = token(6);
    /// `WLTOKEN_LEADERELECTIONREG_LEADERHEARTBEAT` (7).
    pub const LEADERELECTIONREG_LEADERHEARTBEAT: Uid = token(7);
    /// `WLTOKEN_LEADERELECTIONREG_FORWARD` (8).
    pub const LEADERELECTIONREG_FORWARD: Uid = token(8);
    /// `WLTOKEN_GENERATIONREG_READ` (9).
    pub const GENERATIONREG_READ: Uid = token(9);
    /// `WLTOKEN_PROTOCOL_INFO` (10): a coordinator's `ProtocolInfoRequest` stream, "stable and
    /// not to change" across versions.
    pub const PROTOCOL_INFO: Uid = token(10);
    /// `WLTOKEN_GENERATIONREG_WRITE` (11).
    pub const GENERATIONREG_WRITE: Uid = token(11);
    /// `WLTOKEN_CLIENTLEADERREG_DESCRIPTOR_MUTABLE` (12).
    pub const CLIENTLEADERREG_DESCRIPTOR_MUTABLE: Uid = token(12);
    /// `WLTOKEN_CONFIGTXN_GETGENERATION` (13).
    pub const CONFIGTXN_GETGENERATION: Uid = token(13);
    /// `WLTOKEN_CONFIGTXN_GET` (14).
    pub const CONFIGTXN_GET: Uid = token(14);
    /// `WLTOKEN_CONFIGTXN_GETCLASSES` (15).
    pub const CONFIGTXN_GETCLASSES: Uid = token(15);
    /// `WLTOKEN_CONFIGTXN_GETKNOBS` (16).
    pub const CONFIGTXN_GETKNOBS: Uid = token(16);
    /// `WLTOKEN_CONFIGTXN_COMMIT` (17).
    pub const CONFIGTXN_COMMIT: Uid = token(17);
    /// `WLTOKEN_CONFIGFOLLOWER_GETSNAPSHOTANDCHANGES` (18).
    pub const CONFIGFOLLOWER_GETSNAPSHOTANDCHANGES: Uid = token(18);
    /// `WLTOKEN_CONFIGFOLLOWER_GETCHANGES` (19).
    pub const CONFIGFOLLOWER_GETCHANGES: Uid = token(19);
    /// `WLTOKEN_CONFIGFOLLOWER_COMPACT` (20).
    pub const CONFIGFOLLOWER_COMPACT: Uid = token(20);
    /// `WLTOKEN_CONFIGFOLLOWER_ROLLFORWARD` (21).
    pub const CONFIGFOLLOWER_ROLLFORWARD: Uid = token(21);
    /// `WLTOKEN_CONFIGFOLLOWER_GETCOMMITTEDVERSION` (22).
    pub const CONFIGFOLLOWER_GETCOMMITTEDVERSION: Uid = token(22);
    /// `WLTOKEN_PROCESS` (23).
    pub const PROCESS: Uid = token(23);
    /// `WLTOKEN_CONFIGFOLLOWER_LOCK` (24).
    pub const CONFIGFOLLOWER_LOCK: Uid = token(24);
    /// `WLTOKEN_RESERVED_COUNT` (25): the number of well-known indices a client or server
    /// transport reserves.
    pub const RESERVED_COUNT: u32 = 25;
}

/// `TOKEN_STREAM_FLAG` (`FlowTransport.actor.cpp` 7.4:75, `FlowTransport.cpp` 8.0:99): set in
/// the first half of a token that names a stream (a `RequestStream` or `PromiseStream` endpoint), clear for a
/// single reply (`ReplyPromise`, `FlowTransport::addEndpoint`, 7.4:2237-2240,
/// 8.0:2376-2379). Only a packet for an unknown *stream* token is answered with
/// endpoint-not-found.
pub const TOKEN_STREAM_FLAG: u64 = 1;

/// FlowTransport's `PingRequest` (`FlowTransport.actor.cpp` 7.4:238-246, `FlowTransport.cpp`
/// 8.0:341-349; file identifier 4707015, `serializer(ar, reply)`), the same on both releases. Each side's connection monitor
/// sends one to [`well_known::PING_PACKET`] and the receiver answers with a [`PingReply`] to
/// `reply`'s token (`PingReceiver::receive`: `req.reply.send(Void())`, 7.4:255, 8.0:358).
///
/// `reply` is a [`ReplyPromise`], which is only a token on the wire: the receiver sends the
/// reply back on the connection the request came in on (`loadedEndpoint` builds the endpoint
/// from the delivering connection's address, 7.4:2179-2181, 8.0:2318-2320).
#[derive(Wire, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[fdb(file_identifier = 4707015)]
pub struct PingRequest {
    /// Where the reply goes.
    pub reply: ReplyPromise<Void>,
}

/// The reply to a [`PingRequest`]: what `ReplyPromise<Void>::send(Void())` puts on the wire,
/// `ErrorOr<EnsureTable<Void>>` (file identifier `0x021ead4a`), always `Ok`.
pub type PingReply = ErrorOr<EnsureTable<Void>>;

/// The reply a ping receiver sends.
pub const PING_REPLY_OK: PingReply = Ok(EnsureTable(Void));

/// The payload of [`well_known::ENDPOINT_NOT_FOUND`] and
/// [`well_known::UNAUTHORIZED_ENDPOINT`] packets: the token of the endpoint, a bare [`Uid`]
/// as the message root (`SerializeSource<UID>(destination.token)`, 7.4:1297-1314,
/// 8.0:1370-1387; read back with `reader.deserialize(token)`, 7.4:229-233, 8.0:332-336).
pub type EndpointNotFound = Uid;
