//! The packet framing codec against frames written by each release's own framing code
//! (tools/golden-vectors/fixtures/framing-release-*), hostile input, and property tests.

use std::path::{Path, PathBuf};

use fdb_native_transport::{
    CHECKSUM_LEN, Checksum, ConsumeError, DEFAULT_PACKET_LIMIT, DEFAULT_PACKET_WARNING,
    FrameConfig, FrameDecoder, FrameError, FrameWriteError, LENGTH_LEN, MAX_SPARE, TOKEN_LEN,
    parse_frame,
};
use fdb_native_wire::Uid;
use proptest::prelude::*;
use serde_json::Value;

const RELEASES: [&str; 2] = ["release-7.4", "release-8.0"];

fn fixtures_dir(release: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tools/golden-vectors/fixtures")
        .join(format!("framing-{release}"))
}

fn read_json(path: &Path) -> Value {
    let text =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parsing {}: {e}", path.display()))
}

fn from_hex(s: &str) -> Vec<u8> {
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).expect("hex"))
        .collect()
}

/// The records listed under `key` ("positive" or "negative") of a release's index.
fn records(release: &str, key: &str) -> Vec<Value> {
    let dir = fixtures_dir(release);
    let index = read_json(&dir.join("index.json"));
    let list = index[key].as_array().expect("index list");
    assert!(!list.is_empty(), "{release}: no {key} framing vectors");
    list.iter()
        .map(|f| read_json(&dir.join(f.as_str().expect("file name"))))
        .collect()
}

fn config_of(record: &Value) -> FrameConfig {
    let checksum = Checksum::for_tls(!record["checksum_enabled"].as_bool().expect("flag"));
    FrameConfig::new(checksum)
}

/// A positive vector, decoded.
struct Vector {
    name: String,
    config: FrameConfig,
    bytes: Vec<u8>,
    frames: Vec<ExpectedFrame>,
}

struct ExpectedFrame {
    offset: usize,
    length: u32,
    checksum: Option<u64>,
    token: Uid,
    payload: Vec<u8>,
}

fn pattern(len: usize, mul: u8, add: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(mul).wrapping_add(add))
        .collect()
}

fn vectors(release: &str) -> Vec<Vector> {
    records(release, "positive")
        .into_iter()
        .map(|r| {
            let bytes = from_hex(r["bytes"].as_str().expect("bytes"));
            assert_eq!(bytes.len() as u64, r["size"].as_u64().unwrap());
            let frames = r["frames"]
                .as_array()
                .expect("frames")
                .iter()
                .map(|f| {
                    let len = f["payload_len"].as_u64().unwrap() as usize;
                    let payload = if let Some(h) = f.get("payload_hex") {
                        from_hex(h.as_str().unwrap())
                    } else {
                        let p = &f["payload_pattern"];
                        pattern(
                            len,
                            p["mul"].as_u64().unwrap() as u8,
                            p["add"].as_u64().unwrap() as u8,
                        )
                    };
                    assert_eq!(payload.len(), len);
                    ExpectedFrame {
                        offset: f["offset"].as_u64().unwrap() as usize,
                        length: f["length"].as_u64().unwrap() as u32,
                        checksum: f["checksum"].as_u64(),
                        token: Uid::new(
                            f["token"]["first"].as_u64().unwrap(),
                            f["token"]["second"].as_u64().unwrap(),
                        ),
                        payload,
                    }
                })
                .collect();
            Vector {
                name: r["name"].as_str().unwrap().to_owned(),
                config: config_of(&r),
                bytes,
                frames,
            }
        })
        .collect()
}

/// Feeds `chunks` to a fresh decoder, collecting every frame.
fn decode_chunks<'a>(
    config: FrameConfig,
    chunks: impl IntoIterator<Item = &'a [u8]>,
) -> Result<Vec<(Uid, Vec<u8>)>, FrameError> {
    let mut decoder = FrameDecoder::new(config);
    let mut out = Vec::new();
    for chunk in chunks {
        decoder.push(chunk);
        while let Some(frame) = decoder.next_frame()? {
            out.push((frame.token, frame.payload.to_vec()));
        }
    }
    Ok(out)
}

fn expected(v: &Vector) -> Vec<(Uid, Vec<u8>)> {
    v.frames
        .iter()
        .map(|f| (f.token, f.payload.clone()))
        .collect()
}

fn frame_end(v: &Vector, i: usize) -> usize {
    v.frames
        .get(i + 1)
        .map_or(v.bytes.len(), |next| next.offset)
}

// ---------------------------------------------------------------------------------------
// Golden vectors

#[test]
fn fixtures_describe_the_defaults() {
    for release in RELEASES {
        let index = read_json(&fixtures_dir(release).join("index.json"));
        assert_eq!(
            index["packet_limit"].as_u64(),
            Some(DEFAULT_PACKET_LIMIT.into())
        );
        assert_eq!(
            index["packet_warning"].as_u64(),
            Some(DEFAULT_PACKET_WARNING.into())
        );
    }
}

#[test]
fn fixtures_have_the_documented_layout() {
    for release in RELEASES {
        for v in vectors(release) {
            for f in &v.frames {
                let at = f.offset;
                let header = v.config.header_len();
                let len = u32::from_le_bytes(v.bytes[at..at + LENGTH_LEN].try_into().unwrap());
                assert_eq!(len, f.length, "{release} {}", v.name);
                assert_eq!(len as usize, TOKEN_LEN + f.payload.len());
                if let Some(sum) = f.checksum {
                    let stored = u64::from_le_bytes(
                        v.bytes[at + LENGTH_LEN..at + LENGTH_LEN + CHECKSUM_LEN]
                            .try_into()
                            .unwrap(),
                    );
                    assert_eq!(stored, sum);
                    assert_eq!(header, LENGTH_LEN + CHECKSUM_LEN);
                } else {
                    assert_eq!(header, LENGTH_LEN);
                }
                let token = &v.bytes[at + header..at + header + TOKEN_LEN];
                assert_eq!(token[..8], f.token.first.to_le_bytes());
                assert_eq!(token[8..], f.token.second.to_le_bytes());
            }
        }
    }
}

#[test]
fn decodes_the_fixtures() {
    for release in RELEASES {
        for v in vectors(release) {
            let ctx = format!("{release} {}", v.name);
            // The whole stream at once, through the stateless parser.
            let mut rest = v.bytes.as_slice();
            let mut got = Vec::new();
            while let Some((frame, used)) = parse_frame(&v.config, rest).expect(&ctx) {
                got.push((frame.token, frame.payload.to_vec()));
                rest = &rest[used..];
            }
            assert!(rest.is_empty(), "{ctx}: trailing bytes");
            assert_eq!(got, expected(&v), "{ctx}");
            // And through the decoder.
            let decoded = decode_chunks(v.config, [v.bytes.as_slice()]).expect(&ctx);
            assert_eq!(decoded, expected(&v), "{ctx}");
        }
    }
}

#[test]
fn encodes_the_fixtures_byte_for_byte() {
    for release in RELEASES {
        for v in vectors(release) {
            let ctx = format!("{release} {}", v.name);
            let mut out = Vec::new();
            let mut out_with = Vec::new();
            for f in &v.frames {
                v.config
                    .encode_frame(f.token, &f.payload, &mut out)
                    .expect(&ctx);
                v.config
                    .encode_frame_with(f.token, &mut out_with, |buf| {
                        buf.extend_from_slice(&f.payload)
                    })
                    .expect(&ctx);
            }
            assert!(out == v.bytes, "{ctx}: encode_frame differs");
            assert!(out_with == v.bytes, "{ctx}: encode_frame_with differs");
        }
    }
}

#[test]
fn decodes_the_fixtures_split_at_every_position() {
    for release in RELEASES {
        for v in vectors(release) {
            let ctx = format!("{release} {}", v.name);
            let want = expected(&v);
            let n = v.bytes.len();
            // Every split of a small vector; for a large one every split near each frame edge
            // and near the start and end, and a sample in between.
            let mut positions: Vec<usize> = Vec::new();
            if n <= 1500 {
                positions.extend(0..=n);
            } else {
                for f in &v.frames {
                    for d in 0..=40 {
                        positions.push(f.offset.saturating_add(d));
                        positions.push(f.offset.saturating_sub(d));
                    }
                }
                positions.extend(n.saturating_sub(40)..=n);
                positions.extend((0..=n).step_by(997));
                positions.retain(|p| *p <= n);
                positions.sort_unstable();
                positions.dedup();
            }
            for at in positions {
                let (a, b) = v.bytes.split_at(at);
                let got = decode_chunks(v.config, [a, b])
                    .unwrap_or_else(|e| panic!("{ctx} at {at}: {e}"));
                assert_eq!(got, want, "{ctx} split at {at}");
            }
        }
    }
}

#[test]
fn decodes_the_fixtures_a_byte_at_a_time() {
    for release in RELEASES {
        for v in vectors(release) {
            let ctx = format!("{release} {}", v.name);
            let got = decode_chunks(v.config, v.bytes.chunks(1)).expect(&ctx);
            assert_eq!(got, expected(&v), "{ctx}");
            let got = decode_chunks(v.config, v.bytes.chunks(7)).expect(&ctx);
            assert_eq!(got, expected(&v), "{ctx}");
        }
    }
}

#[test]
fn a_truncated_frame_is_incomplete_never_an_error() {
    for release in RELEASES {
        for v in vectors(release) {
            let ctx = format!("{release} {}", v.name);
            let first_end = frame_end(&v, 0);
            let cuts: Vec<usize> = if first_end <= 2000 {
                (0..first_end).collect()
            } else {
                (0..60)
                    .chain(first_end - 60..first_end)
                    .chain((60..first_end - 60).step_by(1013))
                    .collect()
            };
            for cut in cuts {
                let prefix = &v.bytes[..cut];
                assert!(
                    matches!(parse_frame(&v.config, prefix), Ok(None)),
                    "{ctx}: prefix of {cut} bytes"
                );
                let mut d = FrameDecoder::new(v.config);
                d.push(prefix);
                assert!(matches!(d.next_frame(), Ok(None)), "{ctx}: prefix of {cut}");
                assert_eq!(d.buffered_len(), cut);
            }
        }
    }
}

#[test]
fn refuses_the_negative_fixtures() {
    for release in RELEASES {
        for r in records(release, "negative") {
            let name = r["name"].as_str().unwrap();
            let ctx = format!("{release} {name}");
            let bytes = from_hex(r["bytes"].as_str().unwrap());
            let config = config_of(&r);
            assert_eq!(
                r["packet_limit"].as_i64(),
                Some(i64::from(config.max_packet_len())),
                "{ctx}: the limit in force"
            );
            let kind = r["error"].as_str().unwrap();
            let check = |e: FrameError| match (kind, e) {
                ("checksum_mismatch", FrameError::ChecksumMismatch { stored, computed }) => {
                    assert_ne!(stored, computed)
                }
                ("too_short", FrameError::TooShort { len }) => {
                    assert_eq!(u64::from(len), r["length"].as_u64().unwrap())
                }
                ("oversize", FrameError::Oversize { len, limit }) => {
                    assert_eq!(len, r["length"].as_u64().unwrap());
                    assert_eq!(limit, config.max_packet_len());
                }
                (kind, e) => panic!("{ctx}: expected {kind}, got {e:?}"),
            };
            check(parse_frame(&config, &bytes).expect_err(&ctx));
            // Whatever the chunking, the same error.
            for at in 0..=bytes.len() {
                let (a, b) = bytes.split_at(at);
                check(decode_chunks(config, [a, b]).expect_err(&ctx));
            }
        }
    }
}

// ---------------------------------------------------------------------------------------
// Hostile input

fn tls() -> FrameConfig {
    FrameConfig::new(Checksum::Disabled)
}
fn plain() -> FrameConfig {
    FrameConfig::new(Checksum::Enabled)
}

#[test]
fn zero_length_and_short_lengths_are_refused() {
    for config in [plain(), tls()] {
        for len in 0u32..TOKEN_LEN as u32 {
            // As soon as the length is known, without the rest of the header.
            let bytes = len.to_le_bytes();
            assert_eq!(
                parse_frame(&config, &bytes).unwrap_err(),
                FrameError::TooShort { len }
            );
            let mut d = FrameDecoder::new(config);
            d.push(&bytes);
            assert_eq!(d.next_frame().unwrap_err(), FrameError::TooShort { len });
        }
        // 16 is the smallest frame: a token and nothing else.
        let mut wire = Vec::new();
        config.encode_frame(Uid::new(3, 4), &[], &mut wire).unwrap();
        let (frame, used) = parse_frame(&config, &wire).unwrap().unwrap();
        assert_eq!(
            (frame.token, frame.payload, used),
            (Uid::new(3, 4), &[][..], wire.len())
        );
    }
}

#[test]
fn an_oversize_length_is_refused_before_any_payload_is_kept() {
    for config in [plain(), tls()] {
        let limit = config.max_packet_len();
        for len in [limit + 1, limit + 2, u32::MAX / 2, u32::MAX - 1, u32::MAX] {
            let expect = FrameError::Oversize {
                len: u64::from(len),
                limit,
            };
            assert_eq!(
                parse_frame(&config, &len.to_le_bytes()).unwrap_err(),
                expect
            );

            // Pushing the length and then plenty of bytes: the error stands and nothing is kept.
            let mut d = FrameDecoder::new(config);
            d.push(&len.to_le_bytes());
            d.push(&vec![0xAB; 100_000]);
            assert_eq!(d.next_frame().unwrap_err(), expect);
            assert_eq!(d.buffered_len(), 0);
            d.push(&[1, 2, 3]);
            assert_eq!(d.buffered_len(), 0);
            assert_eq!(d.next_frame().unwrap_err(), expect);
            assert_eq!(d.error(), Some(expect));
        }
    }
}

#[test]
fn the_limit_is_inclusive() {
    for checksum in [Checksum::Enabled, Checksum::Disabled] {
        let config = FrameConfig::new(checksum).with_max_packet_len(100);
        assert_eq!(config.max_packet_len(), 100);
        // A token and 84 bytes is a length of exactly 100.
        let mut wire = Vec::new();
        config
            .encode_frame(Uid::new(1, 1), &[7; 84], &mut wire)
            .unwrap();
        assert_eq!(wire.len(), config.header_len() + 100);
        let (frame, _) = parse_frame(&config, &wire).unwrap().unwrap();
        assert_eq!(frame.payload.len(), 84);
        assert_eq!(decode_chunks(config, [wire.as_slice()]).unwrap().len(), 1);

        // One more byte is refused by the encoder, which leaves the buffer alone...
        let mut out = vec![9, 9, 9];
        let err = config
            .encode_frame(Uid::new(1, 1), &[7; 85], &mut out)
            .unwrap_err();
        assert_eq!(
            err,
            FrameError::Oversize {
                len: 101,
                limit: 100
            }
        );
        assert_eq!(out, [9, 9, 9]);
        let err = config
            .encode_frame_with(Uid::new(1, 1), &mut out, |b| b.extend_from_slice(&[7; 85]))
            .unwrap_err();
        assert_eq!(
            err,
            FrameError::Oversize {
                len: 101,
                limit: 100
            }
        );
        assert_eq!(out, [9, 9, 9]);

        // ... and by the decoder.
        let mut bad = Vec::new();
        FrameConfig::new(checksum)
            .encode_frame(Uid::new(1, 1), &[7; 85], &mut bad)
            .unwrap();
        assert_eq!(
            parse_frame(&config, &bad).unwrap_err(),
            FrameError::Oversize {
                len: 101,
                limit: 100
            }
        );
    }
}

#[test]
fn the_limit_is_clamped_as_release_8_0_does() {
    let max = i32::MAX as u32;
    assert_eq!(
        plain().with_max_packet_len(u32::MAX).max_packet_len(),
        max - 16
    );
    assert_eq!(
        tls().with_max_packet_len(u32::MAX).max_packet_len(),
        max - 8
    );
    assert_eq!(plain().with_max_packet_len(0).max_packet_len(), 0);
    assert_eq!(plain().max_packet_len(), DEFAULT_PACKET_LIMIT);
}

#[test]
fn a_zero_limit_refuses_every_frame_without_panicking() {
    let config = plain().with_max_packet_len(0);
    let mut wire = Vec::new();
    assert!(config.encode_frame(Uid::new(1, 2), &[], &mut wire).is_err());
    assert!(wire.is_empty());
    let good = {
        let mut w = Vec::new();
        plain().encode_frame(Uid::new(1, 2), &[], &mut w).unwrap();
        w
    };
    assert!(parse_frame(&config, &good).is_err());
}

#[test]
fn a_bad_checksum_is_refused_and_the_error_is_final() {
    let config = plain();
    let mut wire = Vec::new();
    config
        .encode_frame(Uid::new(5, 6), b"payload", &mut wire)
        .unwrap();
    let mut second = Vec::new();
    config
        .encode_frame(Uid::new(7, 8), b"fine", &mut second)
        .unwrap();
    wire[LENGTH_LEN + CHECKSUM_LEN + TOKEN_LEN] ^= 1;

    let mut d = FrameDecoder::new(config);
    d.push(&wire);
    d.push(&second);
    let err = d.next_frame().unwrap_err();
    assert!(matches!(err, FrameError::ChecksumMismatch { .. }));
    // The good frame behind it is not delivered: the stream cannot be trusted.
    assert_eq!(d.next_frame().unwrap_err(), err);
    assert_eq!(d.buffered_len(), 0);
}

#[test]
fn a_checksum_is_not_checked_on_a_tls_connection() {
    let mut wire = Vec::new();
    tls()
        .encode_frame(Uid::new(5, 6), b"payload", &mut wire)
        .unwrap();
    wire[LENGTH_LEN + TOKEN_LEN] ^= 1; // corrupt the payload: nothing notices
    let (frame, _) = parse_frame(&tls(), &wire).unwrap().unwrap();
    assert_ne!(frame.payload, b"payload");
}

#[test]
fn frames_read_with_the_wrong_mode_do_not_panic() {
    let mut wire = Vec::new();
    plain()
        .encode_frame(Uid::new(5, 6), b"payload", &mut wire)
        .unwrap();
    // A checksummed frame read as a TLS one: the checksum bytes are taken for the token.
    let _ = parse_frame(&tls(), &wire);
    let mut wire = Vec::new();
    tls()
        .encode_frame(Uid::new(5, 6), b"payload", &mut wire)
        .unwrap();
    let _ = parse_frame(&plain(), &wire);
}

#[test]
fn the_decoder_does_not_allocate_from_the_declared_length() {
    // A header that declares the largest frame, then a trickle: the buffer is as big as what was
    // pushed, not what was promised.
    let config = plain();
    let mut d = FrameDecoder::new(config);
    let mut header = Vec::new();
    header.extend_from_slice(&config.max_packet_len().to_le_bytes());
    header.extend_from_slice(&0u64.to_le_bytes());
    d.push(&header);
    assert!(matches!(d.next_frame(), Ok(None)));
    assert_eq!(d.buffered_len(), header.len());
    for _ in 0..10 {
        d.push(&[0u8; 1000]);
        assert!(matches!(d.next_frame(), Ok(None)));
    }
    assert_eq!(d.buffered_len(), header.len() + 10_000);
    // The declared 100 MiB was never reserved: the buffer holds what arrived, give or take the
    // doubling.
    assert!(d.capacity() < 64 * 1024, "capacity {}", d.capacity());

    // The same asking for room instead: growth is geometric and capped at the frame.
    let mut d = FrameDecoder::new(config);
    d.push(&header);
    for _ in 0..200 {
        let n = d.spare(4096).len().min(4096);
        d.commit(n);
    }
    assert!(
        d.capacity() <= 2 * d.buffered_len().max(4096),
        "capacity {} for {} bytes",
        d.capacity(),
        d.buffered_len()
    );
}

#[test]
fn a_large_frame_does_not_pin_its_memory() {
    let config = plain();
    let payload = vec![0x42u8; 2 << 20];
    let mut wire = Vec::new();
    config
        .encode_frame(Uid::new(1, 2), &payload, &mut wire)
        .unwrap();
    let mut d = FrameDecoder::new(config);
    for chunk in wire.chunks(64 * 1024) {
        d.push(chunk);
    }
    let frame = d.next_frame().unwrap().unwrap();
    assert_eq!(frame.payload.len(), payload.len());
    assert!(matches!(d.next_frame(), Ok(None)));
    d.push(&[]);
    assert_eq!(d.buffered_len(), 0);
    // The 2 MiB buffer is cut back once it is empty.
    assert!(d.capacity() <= 64 * 1024, "capacity {}", d.capacity());
}

#[test]
fn many_small_frames_in_one_chunk_are_all_returned() {
    let config = plain();
    let mut wire = Vec::new();
    for i in 0..1000u64 {
        config
            .encode_frame(Uid::new(i, !i), &i.to_le_bytes(), &mut wire)
            .unwrap();
    }
    let got = decode_chunks(config, [wire.as_slice()]).unwrap();
    assert_eq!(got.len(), 1000);
    for (i, (token, payload)) in got.iter().enumerate() {
        assert_eq!(*token, Uid::new(i as u64, !(i as u64)));
        assert_eq!(payload.as_slice(), (i as u64).to_le_bytes());
    }
}

#[test]
fn encode_frame_with_survives_a_writer_that_shrinks_the_buffer() {
    // A writer that clears the vector removes the caller's bytes too: nothing is left to
    // restore, and the call fails.
    let mut out = vec![1, 2, 3, 4, 5];
    let err = plain()
        .encode_frame_with(Uid::new(1, 2), &mut out, |b| b.clear())
        .unwrap_err();
    assert!(matches!(err, FrameError::TooShort { .. }));
    assert!(out.is_empty());

    // One that cuts into the frame, but not below its start, is undone exactly.
    let framing = plain().header_len() + TOKEN_LEN;
    for cut in 0..framing + 3 {
        let mut out = vec![1, 2, 3, 4, 5];
        let before = out.len();
        let result = plain().encode_frame_with(Uid::new(1, 2), &mut out, |b| {
            b.extend_from_slice(b"abc");
            b.truncate(before + cut);
        });
        if cut < framing {
            // Less than the token is left of the frame.
            assert!(
                matches!(result, Err(FrameError::TooShort { .. })),
                "cut {cut}"
            );
            assert_eq!(out, [1, 2, 3, 4, 5], "cut {cut}");
        } else {
            // Header and token intact, and 0 to 2 payload bytes: a frame.
            assert!(result.is_ok(), "cut {cut}");
            let (frame, _) = parse_frame(&plain(), &out[before..]).unwrap().unwrap();
            assert_eq!(frame.payload, &b"abc"[..cut - framing]);
        }
    }
}

#[test]
fn a_failing_payload_writer_leaves_the_buffer_alone() {
    for config in [plain(), tls()] {
        let mut out = b"keep".to_vec();
        let err = config
            .try_encode_frame_with(Uid::new(1, 2), &mut out, |b| {
                b.extend_from_slice(&[0; 100]);
                Err("boom")
            })
            .unwrap_err();
        assert_eq!(err, FrameWriteError::Payload("boom"));
        assert_eq!(out, b"keep");

        let ok = config.try_encode_frame_with(Uid::new(1, 2), &mut out, |b| {
            b.extend_from_slice(b"hi");
            Ok::<(), &str>(())
        });
        assert!(ok.is_ok());
        let (frame, _) = parse_frame(&config, &out[4..]).unwrap().unwrap();
        assert_eq!(frame.payload, b"hi");

        let small = config.with_max_packet_len(20);
        let mut out = b"keep".to_vec();
        let err = small
            .try_encode_frame_with(Uid::new(1, 2), &mut out, |b| {
                b.extend_from_slice(&[0; 5]);
                Ok::<(), ()>(())
            })
            .unwrap_err();
        assert!(matches!(
            err,
            FrameWriteError::Frame(FrameError::Oversize { len: 21, limit: 20 })
        ));
        assert_eq!(out, b"keep");
    }
}

#[test]
fn a_flatbuffers_message_is_serialized_straight_into_the_frame() {
    use fdb_native_wire::Writer;
    let build = |w: &mut Writer| {
        let mut t = w.table();
        t.field(&0xDEAD_BEEFu32).unwrap();
        t.field(&7u64).unwrap();
        t.finish().unwrap()
    };
    let mut w = Writer::new();
    let root = build(&mut w);
    let message = w.finish(root, 0x1234).unwrap();

    for config in [plain(), tls()] {
        let mut separate = Vec::new();
        config
            .encode_frame(Uid::new(9, 8), &message, &mut separate)
            .unwrap();

        let mut in_place = Vec::new();
        config
            .try_encode_frame_with(Uid::new(9, 8), &mut in_place, |out| {
                let root = build(&mut w);
                w.finish_into(out, root, 0x1234)
            })
            .unwrap();
        assert_eq!(in_place, separate);
    }
}

#[test]
fn encodes_appending_to_existing_bytes() {
    let mut out = b"prefix".to_vec();
    plain()
        .encode_frame(Uid::new(1, 2), b"abc", &mut out)
        .unwrap();
    assert_eq!(&out[..6], b"prefix");
    let (frame, used) = parse_frame(&plain(), &out[6..]).unwrap().unwrap();
    assert_eq!((frame.payload, used), (&b"abc"[..], out.len() - 6));
}

// ---------------------------------------------------------------------------------------
// Properties

fn any_token() -> impl Strategy<Value = Uid> {
    (any::<u64>(), any::<u64>()).prop_map(|(a, b)| Uid::new(a, b))
}

fn any_checksum() -> impl Strategy<Value = Checksum> {
    prop_oneof![Just(Checksum::Enabled), Just(Checksum::Disabled)]
}

fn any_payload() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        3 => prop::collection::vec(any::<u8>(), 0..64),
        2 => prop::collection::vec(any::<u8>(), 0..2048),
        1 => prop::collection::vec(any::<u8>(), 16_000..17_000),
    ]
}

/// Cut points that split `n` bytes into chunks.
fn chunk_sizes() -> impl Strategy<Value = Vec<usize>> {
    prop::collection::vec(1usize..3000, 1..40)
}

fn split<'a>(bytes: &'a [u8], sizes: &[usize]) -> Vec<&'a [u8]> {
    let mut out = Vec::new();
    let mut rest = bytes;
    for &s in sizes.iter().cycle() {
        if rest.is_empty() {
            break;
        }
        let (a, b) = rest.split_at(s.min(rest.len()));
        out.push(a);
        rest = b;
    }
    out
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn frames_round_trip_under_any_chunking(
        checksum in any_checksum(),
        frames in prop::collection::vec((any_token(), any_payload()), 1..6),
        sizes in chunk_sizes(),
    ) {
        let config = FrameConfig::new(checksum);
        let mut wire = Vec::new();
        for (token, payload) in &frames {
            config.encode_frame(*token, payload, &mut wire).unwrap();
        }
        let want: Vec<(Uid, Vec<u8>)> = frames.clone();
        let got = decode_chunks(config, split(&wire, &sizes)).unwrap();
        prop_assert_eq!(got, want);
    }

    #[test]
    fn encode_frame_with_equals_encode_frame(
        checksum in any_checksum(),
        token in any_token(),
        payload in any_payload(),
        prefix in prop::collection::vec(any::<u8>(), 0..20),
    ) {
        let config = FrameConfig::new(checksum);
        let mut a = prefix.clone();
        let mut b = prefix.clone();
        config.encode_frame(token, &payload, &mut a).unwrap();
        // The payload written in two pieces.
        let mid = payload.len() / 2;
        config.encode_frame_with(token, &mut b, |buf| {
            buf.extend_from_slice(&payload[..mid]);
            buf.extend_from_slice(&payload[mid..]);
        }).unwrap();
        prop_assert_eq!(&a, &b);
        prop_assert_eq!(&a[..prefix.len()], prefix.as_slice());
        prop_assert_eq!(a.len(), prefix.len() + config.header_len() + TOKEN_LEN + payload.len());
    }

    #[test]
    fn the_checksum_is_xxh3_of_the_token_and_payload(
        token in any_token(),
        payload in any_payload(),
    ) {
        let config = plain();
        let mut wire = Vec::new();
        config.encode_frame(token, &payload, &mut wire).unwrap();
        let stored = u64::from_le_bytes(wire[4..12].try_into().unwrap());
        prop_assert_eq!(stored, xxhash_rust::xxh3::xxh3_64(&wire[12..]));
        // The streaming API gives the same value.
        let mut hasher = xxhash_rust::xxh3::Xxh3::new();
        for piece in wire[12..].chunks(1000) {
            hasher.update(piece);
        }
        prop_assert_eq!(stored, hasher.digest());
    }

    #[test]
    fn arbitrary_bytes_never_panic_and_never_buffer_more_than_pushed(
        checksum in any_checksum(),
        limit in prop_oneof![Just(100u32), Just(5000), Just(DEFAULT_PACKET_LIMIT)],
        bytes in prop::collection::vec(any::<u8>(), 0..4000),
        sizes in chunk_sizes(),
    ) {
        let config = FrameConfig::new(checksum).with_max_packet_len(limit);
        let mut d = FrameDecoder::new(config);
        let mut pushed = 0;
        for chunk in split(&bytes, &sizes) {
            d.push(chunk);
            pushed += chunk.len();
            prop_assert!(d.buffered_len() <= pushed);
            while let Ok(Some(frame)) = d.next_frame() {
                prop_assert!(frame.payload.len() <= limit as usize);
            }
        }
        let _ = parse_frame(&config, &bytes);
    }

    #[test]
    fn a_single_bit_flip_in_a_checksummed_frame_is_never_accepted(
        token in any_token(),
        payload in prop::collection::vec(any::<u8>(), 0..300),
        bit in any::<prop::sample::Index>(),
    ) {
        let config = plain();
        let mut wire = Vec::new();
        config.encode_frame(token, &payload, &mut wire).unwrap();
        let at = bit.index(wire.len() * 8);
        wire[at / 8] ^= 1 << (at % 8);
        // The flip is in the length (a shorter frame whose checksum is wrong, or a longer one
        // that is incomplete or oversize), the checksum, or the counted bytes.
        let accepted = matches!(parse_frame(&config, &wire), Ok(Some(_)));
        prop_assert!(!accepted, "accepted a corrupted frame");
    }

    #[test]
    fn a_truncated_frame_is_incomplete(
        checksum in any_checksum(),
        token in any_token(),
        payload in any_payload(),
        cut in any::<prop::sample::Index>(),
    ) {
        let config = FrameConfig::new(checksum);
        let mut wire = Vec::new();
        config.encode_frame(token, &payload, &mut wire).unwrap();
        let cut = cut.index(wire.len()); // strictly shorter
        prop_assert!(matches!(parse_frame(&config, &wire[..cut]), Ok(None)));
    }

    #[test]
    fn the_limit_is_exact(
        checksum in any_checksum(),
        limit in 16u32..400,
        extra in 0usize..3,
    ) {
        let config = FrameConfig::new(checksum).with_max_packet_len(limit);
        let payload = vec![0x5Au8; (limit as usize - 16) + extra];
        let mut wire = Vec::new();
        let encoded = config.encode_frame(Uid::new(1, 2), &payload, &mut wire);
        let decoded = {
            let mut w = Vec::new();
            FrameConfig::new(checksum).encode_frame(Uid::new(1, 2), &payload, &mut w).unwrap();
            parse_frame(&config, &w).map(|o| o.is_some())
        };
        if extra == 0 {
            prop_assert!(encoded.is_ok());
            prop_assert_eq!(decoded, Ok(true));
        } else {
            prop_assert!(encoded.is_err());
            prop_assert!(wire.is_empty());
            let oversize = matches!(decoded, Err(FrameError::Oversize { len: _, limit: _ }));
            prop_assert!(oversize);
        }
    }
}

// ---------------------------------------------------------------------------------------
// Memory

/// A frame of `payload_len` bytes.
fn frame_bytes(config: &FrameConfig, payload_len: usize) -> Vec<u8> {
    let mut v = Vec::new();
    config
        .encode_frame(Uid::new(1, 2), &vec![7u8; payload_len], &mut v)
        .unwrap();
    v
}

/// A frame received while an earlier, larger prefix is still in the buffer: the consumed part
/// must be moved out of the way, not doubled over (a frame of 100 MiB would otherwise hold
/// 240 MiB). Sizes are scaled to a 1 MiB limit.
#[test]
fn a_consumed_prefix_is_not_kept_while_a_partial_frame_grows() {
    const MIB: usize = 1 << 20;
    let config = plain().with_max_packet_len(MIB as u32);
    let a = frame_bytes(&config, 600 * 1024);
    let b = frame_bytes(&config, MIB - TOKEN_LEN); // the largest frame
    let t = b.len(); // header + declared length
    assert_eq!(t, MIB + config.header_len());
    let cut = 610 * 1024;

    // One push of [A | first 610 KiB of B], then B's rest in 64 KiB pieces.
    let mut first = a.clone();
    first.extend_from_slice(&b[..cut]);
    let mut d = FrameDecoder::new(config);
    d.push(&first);
    assert_eq!(d.next_frame().unwrap().unwrap().payload.len(), 600 * 1024);
    assert!(d.next_frame().unwrap().is_none());
    // What the caller pushed in one go is what is held: that is its own bound.
    let held_by_first_push = d.capacity();
    assert!(held_by_first_push <= first.len() + 4096);
    for chunk in b[cut..].chunks(64 * 1024) {
        d.push(chunk);
        assert!(
            d.capacity() <= held_by_first_push.max(t),
            "capacity {} after {} buffered",
            d.capacity(),
            d.buffered_len()
        );
        if let Some(frame) = d.next_frame().unwrap() {
            assert_eq!(frame.payload.len(), MIB - TOKEN_LEN);
        }
    }
    assert_eq!(d.buffered_len(), 0);

    // The same frames in 64 KiB pushes throughout: never more than the larger frame.
    let mut d = FrameDecoder::new(config);
    let mut stream = a.clone();
    stream.extend_from_slice(&b);
    let mut got = 0;
    for chunk in stream.chunks(64 * 1024) {
        d.push(chunk);
        assert!(d.capacity() <= t, "capacity {}", d.capacity());
        while d.next_frame().unwrap().is_some() {
            got += 1;
        }
    }
    assert_eq!(got, 2);
}

/// One large frame, then small frames in small reads that never end on a frame boundary: the
/// large buffer is not kept.
#[test]
fn a_buffer_grown_for_a_large_frame_is_cut_back_while_the_stream_continues() {
    let config = plain().with_max_packet_len(8 << 20);
    let mut stream = frame_bytes(&config, 5 << 20);
    let small = frame_bytes(&config, 100);
    for _ in 0..2000 {
        stream.extend_from_slice(&small);
    }
    let mut d = FrameDecoder::new(config);
    let mut frames = 0;
    let mut peak = 0;
    for chunk in stream.chunks(4093) {
        d.push(chunk);
        peak = peak.max(d.capacity());
        while d.next_frame().unwrap().is_some() {
            frames += 1;
        }
    }
    assert_eq!(frames, 2001);
    assert!(peak >= 5 << 20);
    assert!(d.capacity() <= 64 * 1024, "capacity {}", d.capacity());

    // The same through spare/commit.
    let mut d = FrameDecoder::new(config);
    for chunk in stream.chunks(4093) {
        let room = d.spare(chunk.len());
        room[..chunk.len()].copy_from_slice(chunk);
        d.commit(chunk.len());
        while d.next_frame().unwrap().is_some() {}
    }
    assert!(d.capacity() <= 64 * 1024, "capacity {}", d.capacity());
}

// ---------------------------------------------------------------------------------------
// Reading into the decoder

#[test]
fn spare_and_commit_decode_what_push_decodes() {
    let config = plain();
    let mut wire = Vec::new();
    for i in 0..50u64 {
        config
            .encode_frame(
                Uid::new(i, i * 3),
                &vec![i as u8; (i * 37) as usize],
                &mut wire,
            )
            .unwrap();
    }
    for chunk_size in [1usize, 5, 13, 100, 4096, 100_000] {
        let mut d = FrameDecoder::new(config);
        let mut got = Vec::new();
        for chunk in wire.chunks(chunk_size) {
            let room = d.spare(chunk.len());
            assert!(room.len() >= chunk.len());
            room[..chunk.len()].copy_from_slice(chunk);
            d.commit(chunk.len());
            while let Some(f) = d.next_frame().unwrap() {
                got.push((f.token, f.payload.to_vec()));
            }
        }
        assert_eq!(got.len(), 50, "chunk {chunk_size}");
        for (i, (token, payload)) in got.iter().enumerate() {
            assert_eq!(*token, Uid::new(i as u64, i as u64 * 3));
            assert_eq!(payload.len(), i * 37);
        }
    }
}

#[test]
fn spare_honors_the_limit_and_final_errors() {
    let config = plain().with_max_packet_len(1000);
    let mut d = FrameDecoder::new(config);
    // A declared length above the limit, written straight into the buffer.
    let room = d.spare(4);
    room[..4].copy_from_slice(&1001u32.to_le_bytes());
    d.commit(4);
    let oversize = FrameError::Oversize {
        len: 1001,
        limit: 1000,
    };
    assert_eq!(d.next_frame().unwrap_err(), oversize);
    assert!(d.spare(100).is_empty());
    d.commit(10); // ignored
    assert_eq!(d.buffered_len(), 0);
    assert_eq!(d.error(), Some(oversize));

    // A commit of exactly the room is fine.
    let mut d = FrameDecoder::new(plain());
    let room = d.spare(10);
    room[..4].copy_from_slice(&16u32.to_le_bytes());
    let room = room.len();
    d.commit(room);
    assert_eq!(d.buffered_len(), room);
}

/// A commit larger than the room is a bug in the caller: it trips a debug assertion, and in a
/// release build is taken to be the room.
#[test]
fn a_commit_larger_than_the_room_is_a_caller_bug() {
    let mut d = FrameDecoder::new(plain());
    let room = d.spare(10);
    room[..4].copy_from_slice(&16u32.to_le_bytes());
    let room = room.len();
    if cfg!(debug_assertions) {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            d.commit(room + 1_000_000);
        }));
        assert!(result.is_err(), "no debug assertion");
    } else {
        d.commit(room + 1_000_000);
        assert_eq!(d.buffered_len(), room);
    }
}

#[test]
fn a_huge_min_is_clamped_and_never_panics() {
    for min in [
        MAX_SPARE + 1,
        1 << 30,
        isize::MAX as usize,
        isize::MAX as usize + 1,
        usize::MAX,
    ] {
        let mut d = FrameDecoder::new(plain());
        let room = d.spare(min).len();
        assert!(room >= MAX_SPARE, "spare({min}) gave {room}");
        assert!(
            d.capacity() <= MAX_SPARE + 4096,
            "capacity {}",
            d.capacity()
        );

        // With bytes already held.
        let mut d = FrameDecoder::new(plain());
        d.push(&[1, 2, 3, 4, 5]);
        let room = d.spare(min).len();
        assert!(room >= MAX_SPARE, "spare({min}) gave {room}");
        assert!(
            d.capacity() <= MAX_SPARE + 4096,
            "capacity {}",
            d.capacity()
        );
    }
    // A frame pending: the clamp still applies.
    let mut d = FrameDecoder::new(plain());
    d.push(&(DEFAULT_PACKET_LIMIT).to_le_bytes());
    let _ = d.spare(usize::MAX);
    assert!(
        d.capacity() <= MAX_SPARE + 4096,
        "capacity {}",
        d.capacity()
    );
}

/// A push larger than `MAX_SPARE` is taken whole, in pieces.
#[test]
fn a_push_larger_than_max_spare_is_kept_whole() {
    let config = plain();
    let payload = vec![0x5a; 3 * MAX_SPARE + 17];
    let mut wire = Vec::new();
    config
        .encode_frame(Uid::new(1, 2), &payload, &mut wire)
        .unwrap();
    let mut d = FrameDecoder::new(config);
    d.push(&wire);
    assert_eq!(d.buffered_len(), wire.len());
    let frame = d.next_frame().unwrap().unwrap();
    assert_eq!(frame.payload, payload.as_slice());
}

/// The capacity changes while a large frame arrives through `spare(4096)`/`commit(1)`: the
/// buffer grows to the end of the frame plus the room asked for, once, not once per byte near the
/// end (each of which would copy the whole buffer with an allocator that cannot extend in place).
#[test]
fn trickling_the_end_of_a_large_frame_does_not_reallocate_per_byte() {
    const MIN: usize = 4096;
    let config = plain().with_max_packet_len(8 << 20);
    let frames = 3;
    let mut wire = Vec::new();
    for i in 0..frames {
        wire.extend_from_slice(&frame_bytes_for(&config, 4 << 20, i));
    }
    let frame_len = wire.len() / frames;

    let mut d = FrameDecoder::new(config);
    let mut last = d.capacity();
    let mut changes_total = 0usize;
    let mut changes_near_end = 0usize;
    let mut got = 0;
    for (at, byte) in wire.iter().enumerate() {
        d.spare(MIN)[0] = *byte;
        d.commit(1);
        let cap = d.capacity();
        if cap != last {
            changes_total += 1;
            // In the last `MIN` bytes of a frame (the old code changed it for every byte).
            if frame_len - at % frame_len <= MIN {
                changes_near_end += 1;
            }
            last = cap;
        }
        while let Some(frame) = d.next_frame().unwrap() {
            assert_eq!(
                frame.payload.len(),
                frame_len - config.header_len() - TOKEN_LEN
            );
            got += 1;
        }
    }
    assert_eq!(got, frames);
    // Doubling from 4 KiB to 4 MiB is 10 steps, and one more for the frame end and `MIN`; the
    // next frames reuse what is there or shrink and grow again, so allow some for each.
    assert!(
        changes_near_end <= frames,
        "{changes_near_end} capacity changes in the last {MIN} bytes of {frames} frames"
    );
    assert!(
        changes_total <= frames * 14,
        "{changes_total} capacity changes for {frames} frames of {frame_len} bytes"
    );
    // The documented bound, with T + m.
    assert!(d.capacity() <= frame_len + 12 + MIN);
}

/// A frame of `total` bytes (header included) for the token `(i, i)`.
fn frame_bytes_for(config: &FrameConfig, total: usize, i: usize) -> Vec<u8> {
    let payload = vec![i as u8; total - config.header_len() - TOKEN_LEN];
    let mut out = Vec::new();
    config
        .encode_frame(Uid::new(i as u64, i as u64), &payload, &mut out)
        .unwrap();
    out
}

#[test]
fn the_handshake_can_be_read_from_the_same_buffer() {
    use fdb_native_transport::ConnectPacket;
    use fdb_native_wire::ProtocolVersion;
    let hello = ConnectPacket::client(ProtocolVersion::V7_4)
        .encode()
        .unwrap();
    assert_eq!(hello.len(), ConnectPacket::SIZE);
    let config = plain();
    let mut frames = Vec::new();
    config
        .encode_frame(Uid::new(4, 5), b"first", &mut frames)
        .unwrap();
    config
        .encode_frame(Uid::new(6, 7), b"second", &mut frames)
        .unwrap();
    let mut wire = hello.clone();
    wire.extend_from_slice(&frames);

    for chunk_size in [1usize, 7, 44, 45, 1000] {
        let mut d = FrameDecoder::before_handshake(config);
        let mut seen_hello = false;
        let mut got = Vec::new();
        for chunk in wire.chunks(chunk_size) {
            d.push(chunk);
            if !seen_hello {
                // Nothing is parsed as a frame before the handshake is consumed.
                assert!(d.next_frame().unwrap().is_none());
                if d.peek().len() >= ConnectPacket::SIZE {
                    assert_eq!(&d.peek()[..ConnectPacket::SIZE], hello.as_slice());
                    d.consume(ConnectPacket::SIZE).unwrap();
                    seen_hello = true;
                }
            }
            if seen_hello {
                while let Some(f) = d.next_frame().unwrap() {
                    got.push((f.token, f.payload.to_vec()));
                }
            }
        }
        assert!(seen_hello, "chunk {chunk_size}");
        assert_eq!(
            got,
            vec![
                (Uid::new(4, 5), b"first".to_vec()),
                (Uid::new(6, 7), b"second".to_vec())
            ],
            "chunk {chunk_size}"
        );
    }
}

#[test]
fn consume_is_only_for_the_handshake_and_misuse_is_an_error() {
    let config = plain();
    let mut frames = Vec::new();
    config
        .encode_frame(Uid::new(6, 7), b"second", &mut frames)
        .unwrap();

    // A decoder made with `new` has no handshake to consume, and is left as it was.
    let mut d = FrameDecoder::new(config);
    d.push(&frames[..10]);
    assert_eq!(d.consume(3), Err(ConsumeError::NotInHandshake));
    assert_eq!(d.consume(0), Err(ConsumeError::NotInHandshake));
    assert_eq!(d.buffered_len(), 10);
    d.push(&frames[10..]);
    assert_eq!(d.next_frame().unwrap().unwrap().payload, b"second");

    // More than is buffered: an error, and the decoder still waits for its handshake.
    let mut d = FrameDecoder::before_handshake(config);
    d.push(&[0; 10]);
    assert_eq!(
        d.consume(100),
        Err(ConsumeError::PastBuffered {
            requested: 100,
            buffered: 10
        })
    );
    assert_eq!(
        d.consume(usize::MAX),
        Err(ConsumeError::PastBuffered {
            requested: usize::MAX,
            buffered: 10
        })
    );
    assert_eq!(d.buffered_len(), 10);
    assert!(d.next_frame().unwrap().is_none()); // still the handshake phase
    d.push(&[0; 34]);
    d.push(&frames);
    d.consume(44).unwrap();
    assert_eq!(d.next_frame().unwrap().unwrap().payload, b"second");

    // A second consume, mid-stream, would cut into the frames.
    let mut d = FrameDecoder::before_handshake(config);
    d.push(&[0; 44]);
    d.push(&frames[..10]);
    d.consume(44).unwrap();
    assert_eq!(d.consume(3), Err(ConsumeError::NotInHandshake));
    assert_eq!(d.buffered_len(), 10);
    d.push(&frames[10..]);
    assert_eq!(d.next_frame().unwrap().unwrap().payload, b"second");

    // After a failure.
    let mut d = FrameDecoder::before_handshake(config);
    d.push(&[0; 44]);
    d.push(&u32::MAX.to_le_bytes());
    d.consume(44).unwrap();
    assert!(d.error().is_some());
    assert_eq!(d.consume(0), Err(ConsumeError::NotInHandshake));
}
