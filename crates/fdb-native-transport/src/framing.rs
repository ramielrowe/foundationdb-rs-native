//! FlowTransport's packet framing, as a sans-IO codec.
//!
//! After the [`ConnectPacket`](crate::ConnectPacket) each side of a connection sends a stream
//! of frames. [`FrameConfig`] encodes them; [`FrameDecoder`] (or [`parse_frame`] on a buffer you
//! manage) takes them apart. Nothing here does I/O, allocates from a length the peer sent, or
//! panics on any input.
//!
//! # The frame
//!
//! ```text
//! u32  length     little-endian; the bytes after the checksum: the token and the payload
//! u64  checksum   little-endian XXH3-64 of those bytes; absent on a TLS connection
//! u8   token[16]  the destination endpoint's UID: `first` then `second`, each a u64 LE
//! u8   payload[length - 16]
//! ```
//!
//! The payload is the FDB flatbuffers message (`fdb-native-wire`). A frame is at least 16 bytes
//! long (an empty payload is possible at this layer, see below) and carries no flags or version:
//! the only inputs to the format are whether the connection is TLS (the checksum) and the size
//! limit.
//!
//! # What FDB does, with citations
//!
//! Every statement below was checked in `release-7.4` (`fdbrpc/FlowTransport.actor.cpp`, cited
//! as `7.4:line`) and `release-8.0` (`fdbrpc/FlowTransport.cpp`, `8.0:line`), and the frames
//! this module writes and reads are checked against bytes written by each release's own
//! framing code (`tools/golden-vectors`, `fixtures/framing-release-*`; see the README there).
//! `messages/check_mirrors.py` fails the fixture build if the cited code changes.
//!
//! * **Length.** The writer reserves `PACKET_LEN_WIDTH` (4) bytes, plus 8 for the checksum, with
//!   `writeAhead` (7.4:2328, 8.0:2486), writes the token and the payload, and sets
//!   `len = wr.size() - packetInfoSize` (7.4:2332; 8.0:2490, `len = payloadSize` at 8.0:2501):
//!   the length counts the token and the payload and neither the length nor the checksum. The
//!   reader skips the length and the checksum, then needs `packetLen` more bytes
//!   (7.4:1343-1362, 8.0:1463-1482).
//! * **Byte order.** The reader loads the fields with `*(uint32_t*)p` and `*(XXH64_hash_t*)p`
//!   (7.4:1343,1351; 8.0:1463,1471) and the writer stores them with `SplitBuffer::write` of the
//!   host integers (7.4:2382-2384; 8.0:2551-2553): host order, with a comment that these
//!   unaligned accesses "work on x86 and newer ARM" (8.0:1457), both little-endian. This module
//!   always uses little-endian, like the rest of FDB's wire format.
//! * **Checksum.** `XXH3_64bits(p, packetLen)` (7.4:1402; 8.0:1522) over the token and the
//!   payload: the one-shot function of the xxHash 0.8.0 that `flow/include/flow/xxhash.h`
//!   vendors (`XXH_VERSION_MAJOR.MINOR.RELEASE` 0.8.0 on both branches, line 247-249), so the
//!   default secret and seed 0. The writer uses that one-shot call when the packet is contiguous
//!   in one `PacketBuffer` and the streaming API (`XXH3_64bits_reset`, `_update`, `_digest`)
//!   when it spans several (7.4:2349-2377; 8.0:2518-2546); both give the same value, which the
//!   `*_chunked` and `boundary_*` fixtures confirm against [`xxhash_rust`].
//! * **When there is a checksum.** `checksumEnabled = !peerAddress.isTLS()` when reading
//!   (7.4:1335, 8.0:1451) and `!destination.getPrimaryAddress().isTLS()` when writing
//!   (7.4:2290, 8.0:2447). It is not a knob, and nothing negotiates it. [`Checksum::for_tls`].
//!   Both ends agree only if the address a sender uses for the checksum decision has the same
//!   TLS flag as the connection the receiver reads it on: the receiver always uses the
//!   connection's own `peerAddress`, a sender uses the *destination endpoint's* primary address.
//!   For a request from us the server answers to the reply endpoint we put in the request, so the
//!   connection layer (as8.3) must send reply endpoints without an address (they are then
//!   resolved to the connection's `peerAddress`) or with an address whose TLS flag matches the
//!   connection; otherwise the server may frame its replies in the other mode and every reply
//!   fails the checksum (or is misread as a token).
//! * **Token.** `wr << destination.token` (7.4:2329, 8.0:2487) and `reader >> token`
//!   (7.4:1431, 8.0:1551): a `UID`, which saves as `serializer(ar, part[0], part[1])`
//!   (`flow/include/flow/IRandom.h:98` on 7.4, `:100` on 8.0), with the "can't simply be
//!   versioned" comment: `first`, then `second`, each a `u64` LE, whatever the protocol
//!   version. The reader strips the object-serializer flag from the peer's version before
//!   building the `ArenaReader` (7.4:1428, 8.0:1548); that does not change how the UID reads.
//! * **Limits** (`flow/Knobs.cpp`): `PACKET_LIMIT` is `100 << 20` (7.4:233, 8.0:231) and
//!   `PACKET_WARNING` is `2 << 20` (7.4:234, 8.0:232); [`DEFAULT_PACKET_LIMIT`],
//!   [`DEFAULT_PACKET_WARNING`]. The limit applies to `packetLen`, the token plus the payload.
//!   The warning only logs `LargePacketReceived` / `LargePacketSent` (7.4:1435,2392;
//!   8.0:1555,2556). 8.0 clamps the limit to `INT_MAX` minus the framing bytes
//!   (`maxPacketPayloadSize`, 8.0:89-96; 8 bytes on TLS and 16 otherwise); 7.4 uses the knob as
//!   it is. [`FrameConfig::with_max_packet_len`] applies the 8.0 clamp for both.
//! * **Oversize.** A reader that sees `packetLen > PACKET_LIMIT` logs `PacketLimitExceeded` and
//!   throws `platform_error` (7.4:1355-1360, 8.0:1475-1480), from `scanPackets`, and also from
//!   `getNewBufferSize` (7.4:1477-1482, 8.0:1596-1601), which runs whenever the read buffer has
//!   fewer than `MIN_PACKET_BUFFER_FREE_BYTES` (256) bytes free and its first 4 unprocessed
//!   bytes are a length (7.4:1514, 8.0:1644): the connection is closed and re-established (the error leaves `connectionReader` through the
//!   `wait(connectionWriter || reader || ...)` in `connectionKeeper`, 7.4:954, 8.0:1047). A
//!   writer differs: 7.4 only logs the error and sends the frame anyway (the `throw` is commented
//!   out, 7.4:2387-2391) so the peer drops the connection; 8.0 discards the packet and throws
//!   `platform_error` to the sender (8.0:2491-2500). [`FrameError::Oversize`] covers both, and
//!   the encoder refuses to write such a frame, as 8.0 does.
//! * **Too short.** `packetLen < sizeof(UID)` throws `platform_error` (`PacketTooSmall`;
//!   7.4:1365-1375, 8.0:1485-1495; an `ASSERT` in simulation). FDB checks after the whole frame
//!   has arrived and this module as soon as the length is known; the outcome is the same, a
//!   closed connection. [`FrameError::TooShort`].
//! * **Checksum mismatch.** `ChecksumMismatchUnexp` is logged and `checksum_failed` is thrown
//!   (7.4:1403-1414, 8.0:1523-1534): again the connection is closed (the error counts as a
//!   close with an error, 7.4:964, 8.0:1057). There is no way to resynchronize a stream without
//!   delimiters, so [`FrameDecoder`] stays failed after the first error.
//!   [`FrameError::ChecksumMismatch`].
//! * **Empty payload.** A frame of exactly 16 bytes passes the framing checks and then trips
//!   `ASSERT(!reader.empty())` (7.4:1443, 8.0:1563), which closes the connection too. This
//!   module hands such a frame to you (an empty payload is not a framing error) and the
//!   encoder writes one if asked; a real peer will not accept it, and no FDB message
//!   serializes to nothing.
//! * **After the ConnectPacket.** `connectionReader` starts with `expectConnectPacket = true`
//!   (7.4:1499, 8.0:1628), reads the 44 bytes of the handshake out of the same buffer
//!   (`unprocessed_begin += connectPacketSize`, then `expectConnectPacket = false`; 7.4:1549-1623,
//!   8.0:1679-1755), and calls `scanPackets` on whatever follows in that buffer (7.4:1662-1677,
//!   8.0:1795-1809). So frames start immediately after the handshake bytes and the stream is one
//!   sequence. The handshake itself is not a frame: no checksum, and its length field is the 40
//!   of `ConnectPacket::LENGTH`. **Never push the ConnectPacket's bytes into a [`FrameDecoder`]
//!   made with [`FrameDecoder::new`]**: they would be parsed as a frame (length 40, then
//!   "checksum" and "token" from the version and port). Either decode the handshake first with
//!   [`ConnectPacket::decode`](crate::ConnectPacket::decode) from your own buffer and push only
//!   what follows it, or read everything into a decoder made with
//!   [`FrameDecoder::before_handshake`], [`peek`](FrameDecoder::peek) the handshake and
//!   [`consume`](FrameDecoder::consume) it.
//! * **Incompatible peers.** If the peer's version is incompatible, FDB hangs up on a peer that
//!   lacks `hasInexpensiveMultiVersionClient()` (`throw incompatible_protocol_version()`,
//!   7.4:1597-1607, 8.0:1729-1739). A newer multi-version client's connection stays open but
//!   `compatible` is false: then, unless the peer `hasStableInterfaces()`, FDB drops whatever it
//!   reads instead of parsing frames, and triggers `resetPing` (7.4:1662-1677, 8.0:1795-1809);
//!   and `sendPacket` sends such a peer only frames for the ping token (7.4:2295, 8.0:2452;
//!   everything else is silently not sent). Those are connection-layer decisions; the framing is
//!   what it is for the frames that do flow.
//! * **No per-frame flags, no versioning.** Nothing in the frame says how to read the
//!   payload except the connection's protocol version, and the framing is identical in 7.4 and
//!   8.0 (apart from the 8.0 limit clamp and the writer's oversize handling above).
//! * **Writer buffers.** The writer builds frames in 16 KiB `PacketBuffer`s (16384 minus 48 bytes
//!   of header, `flow/include/flow/serialize.h:818-819` on 7.4, `:825-826` on 8.0); a header,
//!   a token or a payload may straddle two of them. That is invisible on the wire (a frame is
//!   the same bytes however it was assembled), but the fixtures have vectors for each such case.
//!
//! # Design
//!
//! **Decoding returns borrowed payloads.** [`FrameDecoder::next_frame`] hands out
//! [`Frame`]`<'_>`, whose payload is a slice of the decoder's own buffer, valid until the next
//! call on the decoder: the receive path copies each byte once (from the socket buffer into the
//! decoder), the flatbuffers reader (`fdb-native-wire`) decodes from the slice, and nothing is
//! allocated per frame. A caller that must keep a payload calls [`Frame::into_owned`]. A
//! `Vec<u8>` per frame would add an allocation and a copy to every message, including the
//! one-second pings, for the minority of callers that need ownership. [`parse_frame`] does the
//! same on a buffer the caller owns (a memory-mapped or pooled receive buffer), with no copy at
//! all.
//!
//! **Hostile input and memory.** The length is checked against the limit as soon as its four
//! bytes are in (and again in [`parse_frame`] before the checksum is read), before any payload
//! bytes are kept for it. The decoder never allocates *from* a declared length: it grows its
//! buffer in response to bytes the caller pushes or asks room for ([`FrameDecoder::push`],
//! [`FrameDecoder::spare`]), geometrically, and stops doubling at the end of the frame being
//! received (plus the room a `spare` call asked for). Precisely, after any call its capacity is
//! at most `max(U, T + m, MIN_ALLOC)`, where:
//!
//! * `U` is the most the caller has had it hold at once (unconsumed bytes pushed or committed,
//!   plus the `min` of a `spare` call);
//! * `T` is the size (`header + declared length`, at most `max_packet_len + 12`) of the largest
//!   frame that has been at the front since the buffer was last cut back, the frame being
//!   received in the usual case (a frame counts once its length is in, so a frame still
//!   shorter than four bytes does not, and `T` is zero before any frame);
//! * `m` is the largest `min` given to [`FrameDecoder::spare`], at most [`MAX_SPARE`] (a `push`
//!   does not count: it commits all it asks room for);
//! * `MIN_ALLOC` is 4096 bytes, the floor of the first allocation, which is made even for a
//!   one-byte push.
//!
//! The `T + m` term is there because a read of at most `m` bytes may be asked for while the
//! frame is almost complete, and `spare(m)` then needs room past the end of the frame. The
//! buffer grows once, to `T + m`, instead of reallocating for every few bytes committed near
//! the end of the frame, which would copy up to `m` bytes per committed byte with an allocator
//! that cannot grow in place.
//!
//! While the length of the front frame is not known (fewer than four of its bytes are in, or the
//! decoder is waiting for the handshake to be consumed) growth is exact, not geometric: the
//! buffer is made just large enough for the bytes asked for. That is cheap for the four bytes of
//! a length, and the caller must limit the handshake to the 44 bytes of a `ConnectPacket` (after
//! which [`FrameDecoder::consume`] ends the phase), since a decoder made with
//! [`FrameDecoder::before_handshake`] keeps everything it is given until then.
//!
//! A consumed prefix is moved out of the way before the buffer would grow
//! (never doubled over), and a buffer above 256 KiB is cut back to 64 KiB as soon as at most
//! 64 KiB is held and no large frame is pending, so one large frame does not pin its memory.
//! For this to hold the caller must call [`FrameDecoder::next_frame`] until it returns `None`
//! after each push or commit. The arithmetic uses checked operations on lengths that are at most
//! `i32::MAX`.
//!
//! **Reading from a socket.** [`FrameDecoder::spare`] and [`FrameDecoder::commit`] let a read go
//! straight into the decoder's buffer, so the receive path has no intermediate buffer:
//! `let n = socket.read(decoder.spare(4096))?; decoder.commit(n);`. Pass a constant for `min`:
//! it must never be computed from what the peer sent (a declared length, say), or the peer would
//! choose how much memory the decoder allocates. (A `min` above [`MAX_SPARE`] is lowered to it
//! anyway, so a bad value costs at most that much.) [`FrameDecoder::commit`] takes the number of
//! bytes the read actually returned, never more than the slice it read into.
//!
//! **Encoding.** [`FrameConfig::encode_frame`] copies a payload into the output buffer;
//! [`FrameConfig::encode_frame_with`] lets the caller write the payload straight into it
//! (the way FDB's `ObjectWriter` writes into the `PacketWriter`'s buffer; with
//! `fdb_native_wire::Writer::finish_into` a flatbuffers message is serialized in place), and
//! fills in the header afterwards, as `sendPacket` does with `packetInfoBuffer`.

use fdb_native_wire::Uid;
use xxhash_rust::xxh3::xxh3_64;

/// `PACKET_LEN_WIDTH`: the length field.
pub const LENGTH_LEN: usize = 4;
/// `sizeof(XXH64_hash_t)`: the checksum field, when there is one.
pub const CHECKSUM_LEN: usize = 8;
/// `sizeof(UID)`: the token that starts every frame's counted bytes.
pub const TOKEN_LEN: usize = 16;
/// `PACKET_LIMIT` (`flow/Knobs.cpp`): the largest `packetLen` (token and payload) FDB accepts or
/// sends by default, 100 MiB.
pub const DEFAULT_PACKET_LIMIT: u32 = 100 << 20;
/// `PACKET_WARNING` (`flow/Knobs.cpp`): FDB logs frames above this size, 2 MiB. This module
/// does not log; use it with [`Frame::payload`] if you want the same warning.
pub const DEFAULT_PACKET_WARNING: u32 = 2 << 20;

/// Whether a connection's frames carry a checksum.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Checksum {
    /// The frame has a checksum: a plain TCP connection.
    Enabled,
    /// No checksum: a TLS connection, which authenticates the bytes already.
    Disabled,
}

impl Checksum {
    /// The mode FDB uses for a connection: no checksum when the address is a TLS address.
    pub const fn for_tls(tls: bool) -> Checksum {
        if tls {
            Checksum::Disabled
        } else {
            Checksum::Enabled
        }
    }

    const fn field_len(self) -> usize {
        match self {
            Checksum::Enabled => CHECKSUM_LEN,
            Checksum::Disabled => 0,
        }
    }
}

/// Why a frame was refused, when encoding or decoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FrameError {
    /// The length, the token and the payload together, is above the limit (`PacketLimitExceeded`).
    /// When decoding, `len` is the length the peer declared; when encoding, the size of the
    /// token and payload given.
    #[error("packet length {len} is above the limit {limit}")]
    Oversize {
        /// The offending length.
        len: u64,
        /// The limit in force.
        limit: u32,
    },
    /// The length is below 16, too small to hold the token (`PacketTooSmall`).
    #[error("packet length {len} is too short to hold a token")]
    TooShort {
        /// The offending length.
        len: u32,
    },
    /// The checksum does not match the token and payload (`checksum_failed`).
    #[error("checksum mismatch: frame says {stored:#018x}, bytes hash to {computed:#018x}")]
    ChecksumMismatch {
        /// The checksum in the frame.
        stored: u64,
        /// The checksum of the bytes.
        computed: u64,
    },
}

/// How frames are laid out on one connection: whether they carry a checksum, and the largest
/// length accepted or written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameConfig {
    checksum: Checksum,
    max_packet_len: u32,
}

impl FrameConfig {
    /// A connection's framing with FDB's default limit ([`DEFAULT_PACKET_LIMIT`]).
    pub const fn new(checksum: Checksum) -> FrameConfig {
        FrameConfig::new_clamped(checksum, DEFAULT_PACKET_LIMIT)
    }

    /// The same with another limit on the length (the token and the payload), which FDB
    /// configures with the `PACKET_LIMIT` knob. As release-8.0 does
    /// (`maxPacketPayloadSize`), the limit is lowered to what fits in an `i32` together with the
    /// frame's other bytes.
    pub const fn with_max_packet_len(self, max_packet_len: u32) -> FrameConfig {
        FrameConfig::new_clamped(self.checksum, max_packet_len)
    }

    const fn new_clamped(checksum: Checksum, max_packet_len: u32) -> FrameConfig {
        // 2 * PACKET_LEN_WIDTH + (non-TLS ? sizeof(XXH64_hash_t) : 0): the length, the checksum
        // and the next frame's length, as 8.0's packetFramingBytes() counts them.
        let framing = (2 * LENGTH_LEN + checksum.field_len()) as u32;
        let ceiling = i32::MAX as u32 - framing;
        FrameConfig {
            checksum,
            max_packet_len: if max_packet_len > ceiling {
                ceiling
            } else {
                max_packet_len
            },
        }
    }

    /// Whether frames carry a checksum.
    pub const fn checksum(&self) -> Checksum {
        self.checksum
    }

    /// The largest length (token plus payload) accepted and written.
    pub const fn max_packet_len(&self) -> u32 {
        self.max_packet_len
    }

    /// The bytes before the token: the length, and the checksum if there is one.
    pub const fn header_len(&self) -> usize {
        LENGTH_LEN + self.checksum.field_len()
    }

    /// Appends a frame carrying `payload` for `token` to `out`.
    ///
    /// Fails, and leaves `out` as it was, if the token and payload are longer than the limit.
    pub fn encode_frame(
        &self,
        token: Uid,
        payload: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<(), FrameError> {
        // Check before copying anything.
        let len = (TOKEN_LEN as u64).saturating_add(payload.len() as u64);
        if len > u64::from(self.max_packet_len) {
            return Err(FrameError::Oversize {
                len,
                limit: self.max_packet_len,
            });
        }
        out.reserve(self.header_len() + TOKEN_LEN + payload.len());
        self.encode_frame_with(token, out, |out| out.extend_from_slice(payload))
    }

    /// Appends a frame for `token` to `out`, with the payload written by `write_payload`.
    ///
    /// `write_payload` must only append to the vector it is given. This is how a serializer
    /// can write its output in place, without an intermediate buffer; the length and the
    /// checksum are filled in afterwards. If the token and payload together are above the limit,
    /// `out` is truncated back to its length before the call and the error is returned.
    ///
    /// A closure that shortens the vector breaks the contract. It is not a panic: if less than
    /// the token is left of the frame the call fails with [`FrameError::TooShort`] and `out` is
    /// truncated to its original length, or to what the closure left if that is shorter (the
    /// bytes the closure removed from before the frame are gone; they cannot be restored without
    /// a copy of the whole buffer).
    pub fn encode_frame_with<F>(
        &self,
        token: Uid,
        out: &mut Vec<u8>,
        write_payload: F,
    ) -> Result<(), FrameError>
    where
        F: FnOnce(&mut Vec<u8>),
    {
        match self.try_encode_frame_with(token, out, |out| {
            write_payload(out);
            Ok::<(), core::convert::Infallible>(())
        }) {
            Ok(()) => Ok(()),
            Err(FrameWriteError::Frame(e)) => Err(e),
            Err(FrameWriteError::Payload(never)) => match never {},
        }
    }

    /// [`encode_frame_with`](FrameConfig::encode_frame_with) for a payload writer that can fail
    /// (a flatbuffers encoder, say). On an error from `write_payload`, `out` is truncated to its
    /// length before the call and the error is returned as [`FrameWriteError::Payload`].
    pub fn try_encode_frame_with<F, E>(
        &self,
        token: Uid,
        out: &mut Vec<u8>,
        write_payload: F,
    ) -> Result<(), FrameWriteError<E>>
    where
        F: FnOnce(&mut Vec<u8>) -> Result<(), E>,
    {
        let start = out.len();
        let header = self.header_len();
        let counted = start + header;
        out.resize(counted, 0);
        out.extend_from_slice(&token.first.to_le_bytes());
        out.extend_from_slice(&token.second.to_le_bytes());
        if let Err(e) = write_payload(out) {
            out.truncate(start);
            return Err(FrameWriteError::Payload(e));
        }

        // What the payload writer left: if it removed bytes, less than the token may be there.
        let len = out.len().saturating_sub(counted);
        if len < TOKEN_LEN {
            out.truncate(start);
            return Err(FrameWriteError::Frame(FrameError::TooShort {
                len: u32::try_from(len).unwrap_or(0),
            }));
        }
        if len > self.max_packet_len as usize {
            out.truncate(start);
            return Err(FrameWriteError::Frame(FrameError::Oversize {
                len: len as u64,
                limit: self.max_packet_len,
            }));
        }
        // len <= max_packet_len <= i32::MAX, so this cannot fail; the fallback keeps the
        // function total.
        let len32 = u32::try_from(len).unwrap_or(u32::MAX);
        let checksum = match self.checksum {
            Checksum::Enabled => Some(xxh3_64(&out[counted..])),
            Checksum::Disabled => None,
        };
        let header_bytes = &mut out[start..counted];
        header_bytes[..LENGTH_LEN].copy_from_slice(&len32.to_le_bytes());
        if let Some(sum) = checksum {
            header_bytes[LENGTH_LEN..].copy_from_slice(&sum.to_le_bytes());
        }
        Ok(())
    }
}

/// Why [`FrameConfig::try_encode_frame_with`] failed: the frame, or the payload writer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum FrameWriteError<E> {
    /// The frame could not be built around the payload.
    #[error(transparent)]
    Frame(FrameError),
    /// The payload writer failed.
    #[error("writing the payload failed: {0}")]
    Payload(E),
}

/// A decoded frame: the destination endpoint's token and the message payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Frame<'a> {
    /// The destination endpoint's token (`Endpoint::token`).
    pub token: Uid,
    /// The flatbuffers message after the token. Empty only for a frame of exactly 16 bytes,
    /// which FDB itself does not accept (see the module docs).
    pub payload: &'a [u8],
}

impl Frame<'_> {
    /// A copy of the frame that does not borrow the decoder.
    pub fn into_owned(self) -> OwnedFrame {
        OwnedFrame {
            token: self.token,
            payload: self.payload.to_vec(),
        }
    }
}

/// A [`Frame`] that owns its payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedFrame {
    /// The destination endpoint's token.
    pub token: Uid,
    /// The flatbuffers message after the token.
    pub payload: Vec<u8>,
}

/// What [`parse_prefix`] found: where the token and the payload lie in the input, and how many
/// bytes the frame takes.
struct Parsed {
    token: Uid,
    payload_start: usize,
    total: usize,
}

/// The length at the start of `input`, once its four bytes are there, if it is acceptable.
fn check_length(config: &FrameConfig, input: &[u8]) -> Result<Option<u32>, FrameError> {
    let Some(len_bytes) = input.first_chunk::<LENGTH_LEN>() else {
        return Ok(None);
    };
    let len = u32::from_le_bytes(*len_bytes);
    if len > config.max_packet_len {
        return Err(FrameError::Oversize {
            len: u64::from(len),
            limit: config.max_packet_len,
        });
    }
    if (len as usize) < TOKEN_LEN {
        return Err(FrameError::TooShort { len });
    }
    Ok(Some(len))
}

/// Checks the header at the start of `input` and, if the whole frame is there, the checksum.
///
/// `Ok(None)` means more bytes are needed. Errors are reported as soon as they can be known:
/// the length is checked before the checksum field is required.
fn parse_prefix(config: &FrameConfig, input: &[u8]) -> Result<Option<Parsed>, FrameError> {
    let Some(len) = check_length(config, input)? else {
        return Ok(None);
    };
    // len <= i32::MAX, so these sums cannot overflow even a 32-bit usize.
    let header = config.header_len();
    let Some(total) = header.checked_add(len as usize) else {
        return Ok(None);
    };
    if input.len() < total {
        return Ok(None);
    }
    let counted = &input[header..total];
    if config.checksum == Checksum::Enabled {
        let stored = input
            .get(LENGTH_LEN..header)
            .and_then(|b| <[u8; CHECKSUM_LEN]>::try_from(b).ok())
            .map(u64::from_le_bytes)
            .unwrap_or(0);
        let computed = xxh3_64(counted);
        if stored != computed {
            return Err(FrameError::ChecksumMismatch { stored, computed });
        }
    }
    let (token_bytes, _) = counted.split_at(TOKEN_LEN);
    let first = token_bytes
        .first_chunk::<8>()
        .map(|b| u64::from_le_bytes(*b))
        .unwrap_or(0);
    let second = token_bytes
        .last_chunk::<8>()
        .map(|b| u64::from_le_bytes(*b))
        .unwrap_or(0);
    Ok(Some(Parsed {
        token: Uid::new(first, second),
        payload_start: header + TOKEN_LEN,
        total,
    }))
}

/// Parses the frame at the start of `input`, a buffer the caller owns.
///
/// Returns the frame and the number of bytes it occupies, or `Ok(None)` if `input` is only the
/// start of one (more bytes are needed). The payload borrows `input`: nothing is copied.
/// Use [`FrameDecoder`] to accumulate a stream.
pub fn parse_frame<'a>(
    config: &FrameConfig,
    input: &'a [u8],
) -> Result<Option<(Frame<'a>, usize)>, FrameError> {
    let Some(parsed) = parse_prefix(config, input)? else {
        return Ok(None);
    };
    let payload = input.get(parsed.payload_start..parsed.total).unwrap_or(&[]);
    let frame = Frame {
        token: parsed.token,
        payload,
    };
    Ok(Some((frame, parsed.total)))
}

/// Why [`FrameDecoder::consume`] refused. The decoder is unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ConsumeError {
    /// The decoder is not waiting for its handshake to be consumed: it was not made with
    /// [`FrameDecoder::before_handshake`], or `consume` was called already, or it has failed.
    #[error("the decoder is not in its handshake phase")]
    NotInHandshake,
    /// More bytes were asked for than the decoder holds.
    #[error("cannot consume {requested} bytes, only {buffered} are buffered")]
    PastBuffered {
        /// The `n` passed to `consume`.
        requested: usize,
        /// The bytes buffered.
        buffered: usize,
    },
}

/// Takes bytes in chunks of any size and gives back the frames in them.
///
/// ```
/// use fdb_native_transport::{Checksum, FrameConfig, FrameDecoder};
/// use fdb_native_wire::Uid;
///
/// let config = FrameConfig::new(Checksum::Enabled);
/// let mut wire = Vec::new();
/// config.encode_frame(Uid::new(1, 2), b"hello", &mut wire)?;
///
/// let mut decoder = FrameDecoder::new(config);
/// let (a, b) = wire.split_at(7);
/// decoder.push(a);
/// assert!(decoder.next_frame()?.is_none()); // not all there yet
/// decoder.push(b);
/// let frame = decoder.next_frame()?.expect("a whole frame");
/// assert_eq!(frame.token, Uid::new(1, 2));
/// assert_eq!(frame.payload, b"hello");
/// # Ok::<(), fdb_native_transport::FrameError>(())
/// ```
///
/// Reading from a socket without an intermediate buffer:
///
/// ```
/// # use fdb_native_transport::{Checksum, FrameConfig, FrameDecoder};
/// # use fdb_native_wire::Uid;
/// # let config = FrameConfig::new(Checksum::Disabled);
/// # let mut wire = Vec::new();
/// # config.encode_frame(Uid::new(1, 2), b"hello", &mut wire)?;
/// # let mut socket = std::io::Cursor::new(wire);
/// use std::io::Read;
/// let mut decoder = FrameDecoder::new(config);
/// loop {
///     let n = socket.read(decoder.spare(4096))?;
///     if n == 0 {
///         break; // end of stream (or the decoder has failed: see `error`)
///     }
///     decoder.commit(n);
///     while let Some(frame) = decoder.next_frame()? {
///         assert_eq!(frame.payload, b"hello");
///     }
/// }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// The first error is final: FDB closes the connection on any framing error, since a byte stream
/// with no delimiters cannot be resynchronized. Every call after it returns the same error,
/// pushed bytes are discarded and [`spare`](FrameDecoder::spare) gives no room.
///
/// See the [module documentation](self) for how much memory a decoder holds.
#[derive(Debug)]
pub struct FrameDecoder {
    config: FrameConfig,
    /// Storage, all initialized: `buf[start..filled]` is the unconsumed data, the rest of `buf`
    /// is room for more.
    buf: Vec<u8>,
    /// The first unconsumed byte of `buf`.
    start: usize,
    /// The end of the data in `buf`.
    filled: usize,
    failed: Option<FrameError>,
    /// Set by [`FrameDecoder::before_handshake`] until [`FrameDecoder::consume`] is called: the
    /// buffered bytes are not frames yet.
    handshake: bool,
}

/// Capacity above which a decoder holding little is cut back, so that one large frame does not
/// pin its memory for the life of the connection.
const SHRINK_ABOVE: usize = 256 * 1024;
/// What it is cut back to.
const SHRINK_TO: usize = 64 * 1024;
/// The size of the first allocation.
const MIN_ALLOC: usize = 4096;
/// The most room [`FrameDecoder::spare`] makes in one call, 1 MiB: a larger `min` is lowered to
/// this, so that a wrong or hostile value cannot make the decoder allocate gigabytes. A read into
/// the slice never needs more; [`FrameDecoder::push`] splits a larger slice into pieces of this
/// size.
pub const MAX_SPARE: usize = 1 << 20;

impl FrameDecoder {
    /// A decoder for one connection's frames, with nothing buffered. The bytes pushed to it must
    /// start with a frame: see the module documentation about the ConnectPacket.
    pub fn new(config: FrameConfig) -> FrameDecoder {
        FrameDecoder {
            config,
            buf: Vec::new(),
            start: 0,
            filled: 0,
            failed: None,
            handshake: false,
        }
    }

    /// A decoder whose first bytes are the peer's ConnectPacket, which the caller reads with
    /// [`peek`](FrameDecoder::peek) and removes with [`consume`](FrameDecoder::consume). Until
    /// `consume` is called nothing is parsed as a frame ([`next_frame`](FrameDecoder::next_frame)
    /// returns `Ok(None)`), so the handshake bytes are not mistaken for one; the caller bounds the
    /// handshake (it is 44 bytes, and its length field can be checked after four).
    pub fn before_handshake(config: FrameConfig) -> FrameDecoder {
        FrameDecoder {
            handshake: true,
            ..FrameDecoder::new(config)
        }
    }

    /// The framing in force.
    pub fn config(&self) -> &FrameConfig {
        &self.config
    }

    /// Appends received bytes. Call [`next_frame`](FrameDecoder::next_frame) until it returns
    /// `Ok(None)` after each push, so that the buffer holds only the unfinished frame.
    ///
    /// Bytes pushed after an error are discarded. The header of the frame being received is
    /// checked here too, so an oversize length is reported on the next call to `next_frame`
    /// even if more bytes follow, and nothing more is kept.
    pub fn push(&mut self, bytes: &[u8]) {
        let mut rest = bytes;
        while !rest.is_empty() {
            // The room is at most MAX_SPARE beyond what is held, so a huge slice is taken in
            // pieces; it is empty once the decoder has failed.
            let room = self.room(rest.len(), false);
            let n = room.len().min(rest.len());
            if n == 0 {
                return;
            }
            let (now, later) = rest.split_at(n);
            if let Some(dst) = room.get_mut(..n) {
                dst.copy_from_slice(now);
            }
            self.commit(n);
            rest = later;
        }
    }

    /// Room for at least `min` more bytes (at least one, at most [`MAX_SPARE`]), at the end of
    /// the buffered data: read into the start of the slice and tell the decoder how much with
    /// [`commit`](FrameDecoder::commit). The slice may be much longer than `min`; it is the
    /// room the decoder already has, never more than it would allocate for a push of `min`
    /// bytes (see the module documentation: it is not sized from a declared length).
    ///
    /// `min` is clamped to `1..=MAX_SPARE`, so the slice can be shorter than a `min` above
    /// `MAX_SPARE`. Use a constant: **never derive `min` from data the peer sent**, a declared
    /// length for one, which would let the peer choose the decoder's allocation (up to
    /// `MAX_SPARE` here, but the design is that the peer chooses none of it).
    ///
    /// After an error the slice is empty, so a read into it returns 0; check
    /// [`error`](FrameDecoder::error) or call [`next_frame`](FrameDecoder::next_frame).
    pub fn spare(&mut self, min: usize) -> &mut [u8] {
        self.room(min, true)
    }

    /// The room for `min` bytes (clamped to `1..=MAX_SPARE`) after the data. `partial_commits` is
    /// whether the caller may commit fewer bytes than the room it asked for, as a read into
    /// [`spare`](FrameDecoder::spare) does: then the buffer is allowed to hold `min` more bytes
    /// than the frame being received has (see `grow`). A [`push`](FrameDecoder::push) commits
    /// all it asks room for.
    fn room(&mut self, min: usize, partial_commits: bool) -> &mut [u8] {
        if self.failed.is_some() {
            return &mut [];
        }
        let min = min.clamp(1, MAX_SPARE);
        self.trim();
        if self.buf.len() - self.filled < min {
            self.compact();
            if self.buf.len() - self.filled < min {
                self.grow(min, if partial_commits { min } else { 0 });
            }
        }
        self.buf.get_mut(self.filled..).unwrap_or(&mut [])
    }

    /// Records that the first `n` bytes of the slice [`spare`] returned were written. `n` must not
    /// be larger than that slice (use the count the read returned): a larger `n` is a bug in the
    /// caller. It panics in a build with debug assertions, and otherwise is taken to be the
    /// slice's length, which accepts bytes that were never written. The header of the frame
    /// being received is checked now, as in [`push`](FrameDecoder::push).
    ///
    /// [`spare`]: FrameDecoder::spare
    pub fn commit(&mut self, n: usize) {
        if self.failed.is_some() {
            return;
        }
        let room = self.buf.len() - self.filled;
        debug_assert!(
            n <= room,
            "commit({n}) is more than the {room} bytes of room spare() gave"
        );
        self.filled += n.min(room);
        self.check_pending();
    }

    /// The unconsumed bytes: what has been pushed or committed and not yet returned by
    /// [`next_frame`](FrameDecoder::next_frame) or removed by [`consume`](FrameDecoder::consume).
    /// This is where the handshake is read from a decoder made with
    /// [`before_handshake`](FrameDecoder::before_handshake).
    pub fn peek(&self) -> &[u8] {
        self.pending()
    }

    /// Removes the first `n` of the unconsumed bytes, the handshake of a decoder made with
    /// [`before_handshake`](FrameDecoder::before_handshake). This ends the handshake phase: what
    /// follows is parsed as frames, and the header of the first one is checked now.
    ///
    /// It is for that one use. Anywhere else it would cut into the stream and desynchronize
    /// it, so it fails, and leaves the decoder as it was, if the decoder is not waiting for the
    /// handshake to be consumed (it was made with [`new`](FrameDecoder::new), the handshake was
    /// consumed already, or the decoder has failed) or if `n` is more than
    /// [`buffered_len`](FrameDecoder::buffered_len).
    pub fn consume(&mut self, n: usize) -> Result<(), ConsumeError> {
        if !self.handshake {
            return Err(ConsumeError::NotInHandshake);
        }
        let buffered = self.buffered_len();
        if n > buffered {
            return Err(ConsumeError::PastBuffered {
                requested: n,
                buffered,
            });
        }
        self.start += n;
        self.handshake = false;
        self.check_pending();
        Ok(())
    }

    /// The next whole frame, if the buffered bytes contain one.
    ///
    /// The payload borrows the decoder and is valid until the next call that takes `&mut self`
    /// (copy it with [`Frame::into_owned`] to keep it).
    pub fn next_frame(&mut self) -> Result<Option<Frame<'_>>, FrameError> {
        if let Some(e) = self.failed {
            return Err(e);
        }
        if self.handshake {
            return Ok(None);
        }
        self.trim();
        let parsed = match parse_prefix(&self.config, self.pending()) {
            Ok(Some(p)) => p,
            Ok(None) => return Ok(None),
            Err(e) => {
                self.fail(e);
                return Err(e);
            }
        };
        let begin = self.start + parsed.payload_start;
        let end = self.start + parsed.total;
        self.start = end;
        let payload = self.buf.get(begin..end).unwrap_or(&[]);
        Ok(Some(Frame {
            token: parsed.token,
            payload,
        }))
    }

    /// The number of bytes held that are not part of a frame returned yet.
    pub fn buffered_len(&self) -> usize {
        self.filled - self.start
    }

    /// The memory the decoder's buffer has allocated, in bytes.
    pub fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    /// The error that ended the stream, if one did.
    pub fn error(&self) -> Option<FrameError> {
        self.failed
    }

    /// Fails the decoder if the frame at the front has an unacceptable length.
    fn check_pending(&mut self) {
        if self.handshake {
            return;
        }
        if let Err(e) = check_length(&self.config, self.pending()) {
            self.fail(e);
        }
    }

    fn pending(&self) -> &[u8] {
        self.buf.get(self.start..self.filled).unwrap_or(&[])
    }

    fn fail(&mut self, e: FrameError) {
        self.failed = Some(e);
        self.buf = Vec::new();
        self.start = 0;
        self.filled = 0;
    }

    /// Size of the frame at the front (header and declared length), once its length is known and
    /// acceptable.
    fn pending_frame_len(&self) -> Option<usize> {
        if self.handshake {
            return None;
        }
        match check_length(&self.config, self.pending()) {
            Ok(Some(len)) => Some(self.config.header_len() + len as usize),
            _ => None,
        }
    }

    /// Moves the unconsumed bytes to the front of the buffer.
    fn compact(&mut self) {
        if self.start > 0 {
            self.buf.copy_within(self.start..self.filled, 0);
            self.filled -= self.start;
            self.start = 0;
        }
    }

    /// Resets an emptied buffer and cuts a large one that holds little back to `SHRINK_TO`.
    fn trim(&mut self) {
        if self.start == self.filled {
            self.start = 0;
            self.filled = 0;
        }
        if self.buf.len() > SHRINK_ABOVE
            && self.buffered_len() <= SHRINK_TO
            && self.pending_frame_len().is_none_or(|t| t <= SHRINK_TO)
        {
            self.compact();
            self.buf.truncate(SHRINK_TO);
            self.buf.shrink_to(SHRINK_TO);
        }
    }

    /// Makes room for `min` more bytes after the data, which starts at 0 (`compact` was called).
    ///
    /// Doubles the buffer, but not past the end of the frame being received plus `slack` (its
    /// declared size is a limit on growth, never a size to allocate), and never to less than what
    /// is needed. The slack is `min` for a caller that may commit less than the room it asked
    /// for: stopping at the frame's end exactly would make every `spare(min)` in the last `min`
    /// bytes of the frame reallocate again, as `needed` is past the end of the buffer until the
    /// frame is complete. With the slack, a read of up to `min` bytes ending at the frame's end
    /// fits from the first call. While the frame's length is unknown the growth is exact.
    fn grow(&mut self, min: usize, slack: usize) {
        let needed = self.filled.saturating_add(min);
        let target = match self.pending_frame_len() {
            Some(frame) => {
                let doubled = self.buf.len().saturating_mul(2).max(MIN_ALLOC);
                needed.max(doubled.min(frame.saturating_add(slack)))
            }
            None => needed.max(MIN_ALLOC),
        };
        self.buf.reserve_exact(target - self.buf.len());
        self.buf.resize(target, 0);
    }
}
