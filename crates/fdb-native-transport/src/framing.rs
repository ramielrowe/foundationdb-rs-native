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
//!   (7.4:2290, 8.0:2447): it follows whether the connection's address is a TLS address, which
//!   both ends know. It is not a knob, and nothing negotiates it. [`Checksum::for_tls`].
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
//!   `getNewBufferSize` as soon as the 4 length bytes are in (7.4:1477-1482, 8.0:1596-1601): the
//!   connection is closed and re-established (the error leaves `connectionReader` through the
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
//!   sequence: [`FrameDecoder::push`] the bytes left over after the handshake, then the rest. The
//!   handshake itself is not a frame: no checksum, and its length field is the 40 of
//!   `ConnectPacket::LENGTH`. If the peer's version is incompatible (and has no stable
//!   interfaces) FDB does not parse frames at all: it drops what it reads (the `else` branch of
//!   the same block, which also triggers `resetPing`). That is the connection layer's decision.
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
//! **Hostile input.** The length is checked against the limit as soon as its four bytes are in
//! (and again in [`parse_frame`] before the checksum is read), before any payload bytes are
//! kept for it: the decoder's buffer only ever grows with bytes the caller pushed, never
//! to a length the peer declared. [`FrameDecoder::push`] re-checks the pending header on every
//! call, so after an oversize or too-short length it rejects the stream without keeping more.
//! The buffer holds at most the current frame (at most `max_packet_len + 12` bytes) plus whatever
//! the last `push` added, as long as the caller calls [`FrameDecoder::next_frame`] until it
//! returns `None` after each push. The arithmetic uses checked operations on lengths that
//! are at most `i32::MAX`.
//!
//! **Encoding.** [`FrameConfig::encode_frame`] copies a payload into the output buffer;
//! [`FrameConfig::encode_frame_with`] lets the caller write the payload straight into it
//! (the way FDB's `ObjectWriter` writes into the `PacketWriter`'s buffer), and fills in the
//! header afterwards, as `sendPacket` does with `packetInfoBuffer`.

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
    /// checksum are filled in afterwards. If the payload turns out to be too large (the token
    /// and payload together are above the limit) `out` is truncated back to its length before the
    /// call and the error is returned. If `write_payload` shortens the vector below where it
    /// started, which it must not, `out` is truncated to its original length too and the call
    /// fails with [`FrameError::TooShort`].
    pub fn encode_frame_with<F>(
        &self,
        token: Uid,
        out: &mut Vec<u8>,
        write_payload: F,
    ) -> Result<(), FrameError>
    where
        F: FnOnce(&mut Vec<u8>),
    {
        let start = out.len();
        let header = self.header_len();
        let counted = start + header;
        out.resize(counted, 0);
        out.extend_from_slice(&token.first.to_le_bytes());
        out.extend_from_slice(&token.second.to_le_bytes());
        write_payload(out);

        let Some(len) = out.len().checked_sub(counted) else {
            out.truncate(start);
            return Err(FrameError::TooShort { len: 0 });
        };
        if len > self.max_packet_len as usize {
            out.truncate(start);
            return Err(FrameError::Oversize {
                len: len as u64,
                limit: self.max_packet_len,
            });
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
/// The first error is final: FDB closes the connection on any framing error, since a byte stream
/// with no delimiters cannot be resynchronized. Every call after it returns the same error and
/// pushed bytes are discarded.
#[derive(Debug)]
pub struct FrameDecoder {
    config: FrameConfig,
    buf: Vec<u8>,
    /// The first unconsumed byte of `buf`.
    start: usize,
    failed: Option<FrameError>,
}

/// Capacity above which an emptied buffer is shrunk, so that one large frame does not pin its
/// memory for the life of the connection.
const SHRINK_ABOVE: usize = 256 * 1024;
const SHRINK_TO: usize = 64 * 1024;

impl FrameDecoder {
    /// A decoder for one connection's frames, with nothing buffered.
    pub fn new(config: FrameConfig) -> FrameDecoder {
        FrameDecoder {
            config,
            buf: Vec::new(),
            start: 0,
            failed: None,
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
    /// even if more bytes follow.
    pub fn push(&mut self, bytes: &[u8]) {
        if self.failed.is_some() {
            return;
        }
        self.compact();
        self.buf.extend_from_slice(bytes);
        // A bad header: do not keep what follows it.
        let pending = self.buf.get(self.start..).unwrap_or(&[]);
        if let Err(e) = check_length(&self.config, pending) {
            self.fail(e);
        }
    }

    /// The next whole frame, if the pushed bytes contain one.
    ///
    /// The payload borrows the decoder and is valid until the next call that takes `&mut self`
    /// (copy it with [`Frame::into_owned`] to keep it).
    pub fn next_frame(&mut self) -> Result<Option<Frame<'_>>, FrameError> {
        if let Some(e) = self.failed {
            return Err(e);
        }
        self.compact();
        let pending = self.buf.get(self.start..).unwrap_or(&[]);
        let parsed = match parse_prefix(&self.config, pending) {
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
        self.buf.len().saturating_sub(self.start)
    }

    /// The error that ended the stream, if one did.
    pub fn error(&self) -> Option<FrameError> {
        self.failed
    }

    fn fail(&mut self, e: FrameError) {
        self.failed = Some(e);
        self.buf = Vec::new();
        self.start = 0;
    }

    /// Drops the bytes of frames already returned. Amortized: only when at least half the buffer
    /// is consumed, or all of it.
    fn compact(&mut self) {
        if self.start == 0 {
            return;
        }
        if self.start >= self.buf.len() {
            self.buf.clear();
            self.start = 0;
            if self.buf.capacity() > SHRINK_ABOVE {
                self.buf.shrink_to(SHRINK_TO);
            }
        } else if self.start >= self.buf.len() / 2 {
            self.buf.drain(..self.start);
            self.start = 0;
        }
    }
}
