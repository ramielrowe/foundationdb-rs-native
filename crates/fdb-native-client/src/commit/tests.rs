//! The request construction, without a network.

use fdb_native_schema::SchemaVersion;
use fdb_native_schema::domain::{Commit, KeyRange, Mutation, MutationType};
use fdb_native_wire::Uid;

use super::limits::{KEY_RANGE_REF_SIZE, MUTATION_REF_SIZE, expected_size};
use super::prepare::intersects;
use super::*;
use crate::error::Error;
use crate::error::codes as error_code;

fn limits() -> CommitLimits {
    CommitLimits::for_schema(SchemaVersion::V7_4)
}

fn uid() -> Uid {
    Uid::new(1, 2)
}

/// `prepare` without extra read conflict ranges.
fn prepare(
    read_version: i64,
    m: Vec<Mutation>,
    r: Vec<KeyRange>,
    w: Vec<KeyRange>,
    options: &CommitOptions,
    limits: &CommitLimits,
    uid: impl FnOnce() -> Uid,
) -> Result<Option<Commit>, Error> {
    super::prepare::prepare(read_version, m, r, w, Vec::new(), options, limits, uid)
}

fn build(buffer: CommitBuffer, options: &CommitOptions) -> Result<Option<Commit>, Error> {
    let (m, r, w, extra) = buffer.into_parts_with_extra();
    super::prepare::prepare(100, m, r, w, extra, options, &limits(), uid)
}

fn r(begin: &str, end: &str) -> KeyRange {
    KeyRange::new(begin, end).unwrap()
}

#[test]
fn set_adds_a_single_key_write_range_and_a_self_conflict() {
    let mut b = CommitBuffer::new(limits());
    b.set(b"k", b"v").unwrap();
    let c = build(b, &CommitOptions::default()).unwrap().unwrap();
    assert_eq!(
        c.mutations,
        vec![Mutation::new(MutationType::SET_VALUE, "k", "v").unwrap()]
    );
    assert_eq!(c.read_snapshot, 100);
    let sc = fdb_native_schema::self_conflict_key(uid()).unwrap();
    assert_eq!(sc.len(), 21);
    // The write range of the set, then the self-conflict key, which is also a read range.
    assert_eq!(
        c.write_conflict_ranges,
        vec![KeyRange::single_key(b"k"), KeyRange::single_key(&sc)]
    );
    assert_eq!(c.read_conflict_ranges, vec![KeyRange::single_key(&sc)]);
    assert_eq!(c.flags, 0);
    assert!(!c.lock_aware && !c.report_conflicting_keys);
}

#[test]
fn causal_write_risky_adds_no_self_conflict() {
    let mut b = CommitBuffer::new(limits());
    b.set(b"k", b"v").unwrap();
    let options = CommitOptions {
        causal_write_risky: true,
        ..CommitOptions::default()
    };
    let c = build(b, &options).unwrap().unwrap();
    assert!(c.read_conflict_ranges.is_empty());
    assert_eq!(c.write_conflict_ranges, vec![KeyRange::single_key(b"k")]);
}

#[test]
fn an_intersecting_read_range_makes_a_self_conflict_unnecessary() {
    let mut b = CommitBuffer::new(limits());
    b.set(b"m", b"v").unwrap();
    b.add_read_conflict_range(b"a", b"z").unwrap();
    let c = build(b, &CommitOptions::default()).unwrap().unwrap();
    assert_eq!(c.read_conflict_ranges, vec![r("a", "z")]);
    assert_eq!(c.write_conflict_ranges, vec![KeyRange::single_key(b"m")]);

    // A read range next to the write range, not overlapping it ([m, m\0) vs [m\0, z)).
    let mut b = CommitBuffer::new(limits());
    b.set(b"m", b"v").unwrap();
    b.add_read_conflict_range(b"m\0", b"z").unwrap();
    let c = build(b, &CommitOptions::default()).unwrap().unwrap();
    assert_eq!(c.read_conflict_ranges.len(), 2);
    assert!(c.read_conflict_ranges[1].begin.starts_with(b"\xff/SC/"));
}

#[test]
fn clear_and_clear_range() {
    let mut b = CommitBuffer::new(limits());
    b.clear(b"k");
    b.clear_range(b"a", b"c").unwrap();
    b.clear_range(b"x", b"x").unwrap(); // empty: dropped
    let options = CommitOptions {
        causal_write_risky: true,
        ..CommitOptions::default()
    };
    let c = build(b, &options).unwrap().unwrap();
    assert_eq!(
        c.mutations,
        vec![
            Mutation::new(MutationType::CLEAR_RANGE, "k", "k\0").unwrap(),
            Mutation::new(MutationType::CLEAR_RANGE, "a", "c").unwrap(),
        ]
    );
    assert_eq!(
        c.write_conflict_ranges,
        vec![KeyRange::single_key(b"k"), r("a", "c")]
    );
    assert_eq!(
        CommitBuffer::new(limits())
            .clear_range(b"b", b"a")
            .unwrap_err()
            .code(),
        error_code::INVERTED_RANGE
    );
}

#[test]
fn oversized_clear_keys_are_truncated_or_skipped() {
    let long = vec![b'a'; 20_000];
    let mut b = CommitBuffer::new(limits());
    b.clear(&long); // skipped
    b.clear_range(b"a", &long).unwrap();
    let options = CommitOptions {
        causal_write_risky: true,
        ..CommitOptions::default()
    };
    let c = build(b, &options).unwrap().unwrap();
    assert_eq!(c.mutations.len(), 1);
    // 7.4: 10000 + 8 (the tenant prefix size), plus one byte.
    assert_eq!(c.mutations[0].param2.len(), 10_009);
    assert_eq!(c.write_conflict_ranges.len(), 1);

    // 8.0: 10000 + 1.
    let l = CommitLimits::for_schema(SchemaVersion::V8_0);
    let c = prepare(
        1,
        vec![],
        vec![],
        vec![KeyRange::new("a", long).unwrap()],
        &options,
        &l,
        uid,
    )
    .unwrap()
    .unwrap();
    assert_eq!(c.write_conflict_ranges[0].end.len(), 10_001);

    // A range whose ends become equal is dropped, and with it the transaction is empty.
    let ends = vec![b'a'; 10_100];
    let mut more = ends.clone();
    more.extend_from_slice(b"zzz");
    let empty = prepare(
        1,
        vec![],
        vec![],
        vec![KeyRange::new(ends, more).unwrap()],
        &options,
        &l,
        uid,
    )
    .unwrap();
    assert!(empty.is_none());
}

#[test]
fn key_and_value_limits() {
    let mut b = CommitBuffer::new(limits());
    let e = b.set(&vec![b'k'; 10_001], b"v").unwrap_err();
    assert_eq!(e.code(), error_code::KEY_TOO_LARGE);
    assert_eq!(
        e.message(),
        Some("a key of 10001 bytes exceeds the limit of 10000")
    );
    assert!(b.set(&vec![b'k'; 10_000], b"v").is_ok());
    // System keys have a larger limit.
    let mut sys = vec![0xff];
    sys.resize(30_000, b'k');
    assert!(b.set(&sys, b"v").is_ok());
    sys.push(b'k');
    assert_eq!(
        b.set(&sys, b"v").unwrap_err().code(),
        error_code::KEY_TOO_LARGE
    );
    assert!(b.set(b"k", &vec![0; 100_000]).is_ok());
    let e = b.set(b"k", &vec![0; 100_001]).unwrap_err();
    assert_eq!(e.code(), error_code::VALUE_TOO_LARGE);
    // Raw mutations are checked as well.
    let m = Mutation::new(MutationType::SET_VALUE, vec![b'k'; 10_001], "v").unwrap();
    let e = prepare(
        1,
        vec![m],
        vec![],
        vec![],
        &CommitOptions::default(),
        &limits(),
        uid,
    )
    .unwrap_err();
    assert_eq!(e.code(), error_code::KEY_TOO_LARGE);
}

#[test]
fn transaction_size() {
    // 1 mutation of 44 + 2 bytes, 1 write range of 24 + 3 bytes.
    let m = Mutation::new(MutationType::SET_VALUE, "k", "v").unwrap();
    assert_eq!(
        expected_size(&[m], &[], &[KeyRange::single_key(b"k")]),
        MUTATION_REF_SIZE + 2 + KEY_RANGE_REF_SIZE + 3
    );
    // 123456 sets of 4-byte keys with no value is the largest that fits 10 000 000 (measured
    // against libfdb_c 7.4.10), 123457 does not.
    let sets = |n: u32| {
        let mut b = CommitBuffer::new(limits());
        for i in 0..n {
            b.set(&(i * 2).to_be_bytes(), b"").unwrap();
        }
        b
    };
    let options = CommitOptions {
        causal_write_risky: true,
        ..CommitOptions::default()
    };
    assert!(build(sets(123_456), &options).unwrap().is_some());
    let e = build(sets(123_457), &options).unwrap_err();
    assert_eq!(e.code(), error_code::TRANSACTION_TOO_LARGE);
    assert_eq!(
        e.message(),
        Some("10000017 bytes exceed the limit of 10000000")
    );

    // The size option.
    let mut small = CommitOptions {
        size_limit: 100,
        ..options.clone()
    };
    let mut b = CommitBuffer::new(limits());
    b.set(b"k", &[0; 30]).unwrap(); // 44 + 31 + 24 + 3 = 102
    assert_eq!(build(b.clone(), &small).unwrap_err().code(), 2101);
    small.size_limit = 102;
    assert!(build(b.clone(), &small).unwrap().is_some());
    // `getKey` / `getRange` conflict ranges do not count (C++'s `extraConflictRanges`), and
    // are sent as they are, after the others; an empty one is left out.
    let mut extra = b.clone();
    extra.add_extra_read_conflict_range(r("a", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"));
    extra.add_extra_read_conflict_range(r("q", "q"));
    let c = build(extra.clone(), &small).unwrap().unwrap();
    assert_eq!(
        c.read_conflict_ranges,
        vec![r("a", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")]
    );
    // `into_parts` hands them over with the others (counted by `Committer::commit`).
    assert_eq!(extra.into_parts().1.len(), 1);
    small.size_limit = 31;
    assert_eq!(build(b.clone(), &small).unwrap_err().code(), 2006);
    small.size_limit = 10_000_001;
    assert_eq!(build(b, &small).unwrap_err().code(), 2006);
}

#[test]
fn the_self_conflict_key_is_added_after_the_size_check() {
    // Exactly at the limit without it; the added ranges must not make it fail.
    let mut b = CommitBuffer::new(limits());
    b.set(b"k", b"v").unwrap(); // 46 + 27 = 73
    let options = CommitOptions {
        size_limit: 73,
        ..CommitOptions::default()
    };
    let c = build(b, &options).unwrap().unwrap();
    assert_eq!(c.read_conflict_ranges.len(), 1);
}

#[test]
fn nothing_to_commit() {
    let b = CommitBuffer::new(limits());
    assert!(b.is_empty());
    assert!(build(b, &CommitOptions::default()).unwrap().is_none());
    // A read conflict range alone is read-only too.
    let mut b = CommitBuffer::new(limits());
    b.add_read_conflict_range(b"a", b"b").unwrap();
    assert!(build(b, &CommitOptions::default()).unwrap().is_none());
    // A write conflict range alone is a commit.
    let mut b = CommitBuffer::new(limits());
    b.add_write_conflict_range(b"a", b"b").unwrap();
    assert!(!b.is_empty());
    assert!(build(b, &CommitOptions::default()).unwrap().is_some());
}

#[test]
fn read_only_option() {
    let options = CommitOptions {
        read_only: true,
        ..CommitOptions::default()
    };
    let mut b = CommitBuffer::new(limits());
    assert!(build(b.clone(), &options).unwrap().is_none());
    b.set(b"k", b"v").unwrap();
    assert_eq!(
        build(b, &options).unwrap_err().code(),
        error_code::TRANSACTION_READ_ONLY
    );
}

#[test]
fn flags() {
    let mut b = CommitBuffer::new(limits());
    b.set(b"k", b"v").unwrap();
    let options = CommitOptions {
        lock_aware: true,
        first_in_batch: true,
        bypass_storage_quota: true,
        report_conflicting_keys: true,
        ..CommitOptions::default()
    };
    let c = build(b, &options).unwrap().unwrap();
    assert_eq!(
        c.flags,
        Commit::FLAG_IS_LOCK_AWARE
            | Commit::FLAG_FIRST_IN_BATCH
            | Commit::FLAG_BYPASS_STORAGE_QUOTA
    );
    assert!(c.report_conflicting_keys);
    assert!(
        !c.lock_aware,
        "transaction.lock_aware is not set by a client"
    );
}

#[test]
fn atomic_ops() {
    let mut b = CommitBuffer::new(limits());
    b.atomic_op(b"c", &1i64.to_le_bytes(), MutationType::ADD_VALUE)
        .unwrap();
    b.atomic_op(b"m", b"x", MutationType::MIN).unwrap();
    b.atomic_op(b"a", b"x", MutationType::AND).unwrap();
    let c = build(b, &CommitOptions::default()).unwrap().unwrap();
    assert_eq!(
        c.mutations.iter().map(|m| m.kind).collect::<Vec<_>>(),
        vec![
            MutationType::ADD_VALUE,
            MutationType::MIN_V2,
            MutationType::AND_V2
        ]
    );
    assert_eq!(c.write_conflict_ranges.len(), 3 + 1);
    // Not atomic operations (`isAtomicOp`): `invalid_mutation_type`.
    let mut b = CommitBuffer::new(limits());
    for t in [
        MutationType(3),
        MutationType(5),
        MutationType(21),
        MutationType::SET_VALUE,
        MutationType::CLEAR_RANGE,
    ] {
        assert_eq!(
            b.atomic_op(b"k", b"v", t).unwrap_err().code(),
            error_code::INVALID_MUTATION_TYPE,
            "{t:?}"
        );
    }
    assert!(b.is_empty());
}

/// `placeholder` bytes at `at`, then `tail`, then the 4-byte offset `offset`.
fn stamped(at: usize, tail: usize, offset: i32) -> Vec<u8> {
    let mut v = vec![b'p'; at];
    v.extend_from_slice(&[0xff; 10]);
    v.extend(std::iter::repeat_n(b't', tail));
    v.extend_from_slice(&offset.to_le_bytes());
    v
}

#[test]
fn versionstamp_offsets_are_validated_as_cpp_does() {
    use MutationType as T;
    let code = |kind, key: &[u8], value: &[u8]| {
        let mut b = CommitBuffer::new(limits());
        b.atomic_op(key, value, kind).map_err(|e| e.code())
    };
    let bad = Err(error_code::CLIENT_INVALID_OPERATION);
    // A key: `offset + 10 <= len - 4`, `offset >= 0`, `len >= 4`.
    for (key, ok) in [
        (stamped(0, 0, 0), true),
        (stamped(3, 5, 3), true),
        (stamped(3, 0, 3), true),
        (stamped(3, 0, 4), false), // one past the end
        (stamped(3, 0, 2), true),  // any offset that fits: the bytes are replaced
        (stamped(0, 0, -1), false),
        (stamped(0, 0, i32::MIN), false),
        (stamped(0, 0, i32::MAX), false),
        (stamped(0, 0, 1), false),
        (vec![1, 2, 3], false),
        (vec![], false),
        (vec![0, 0, 0, 0], false), // offset 0 but no room for ten bytes
        (vec![0; 13], false),
        (vec![0; 14], true),
    ] {
        let expected = if ok { Ok(()) } else { bad };
        assert_eq!(
            code(T::SET_VERSIONSTAMPED_KEY, &key, b"v"),
            expected,
            "{key:?}"
        );
        // The same bytes as a value.
        assert_eq!(
            code(T::SET_VERSIONSTAMPED_VALUE, b"k", &key),
            expected,
            "{key:?}"
        );
    }
    // Only the versionstamped parameter is looked at: a key that is no valid value, and the
    // other way round.
    assert_eq!(
        code(T::SET_VERSIONSTAMPED_KEY, &stamped(0, 0, 0), b""),
        Ok(())
    );
    assert_eq!(
        code(T::SET_VERSIONSTAMPED_VALUE, b"", &stamped(0, 0, 0)),
        Ok(())
    );
    assert_eq!(
        code(T::SET_VERSIONSTAMPED_VALUE, &stamped(0, 0, 0), b"x"),
        bad
    );
    // A plain atomic operation does not look at its operand's tail.
    assert_eq!(code(T::ADD_VALUE, b"k", &[1, 2]), Ok(()));
    // The offset is little-endian.
    let mut key = vec![0u8; 300];
    key.extend_from_slice(&[0x2c, 0x01, 0, 0]); // 300
    assert_eq!(code(T::SET_VERSIONSTAMPED_KEY, &key, b""), bad);
    let mut key = vec![0u8; 400];
    key.extend_from_slice(&[0x2c, 0x01, 0, 0]);
    assert_eq!(code(T::SET_VERSIONSTAMPED_KEY, &key, b""), Ok(()));
    // The limits come first (the whole key counts, offset included), as in `atomicOp`.
    assert_eq!(
        code(T::SET_VERSIONSTAMPED_KEY, &vec![0xaa; 10_001], b"").unwrap_err(),
        error_code::KEY_TOO_LARGE
    );
    assert_eq!(
        code(T::SET_VERSIONSTAMPED_KEY, &stamped(1, 9_985, 1), b""),
        Ok(())
    );
    assert_eq!(
        code(T::SET_VERSIONSTAMPED_KEY, &stamped(1, 9_986, 1), b"").unwrap_err(),
        error_code::KEY_TOO_LARGE
    );
    assert_eq!(
        code(
            T::SET_VERSIONSTAMPED_VALUE,
            b"k",
            &stamped(0, 100_000 - 14, 0)
        ),
        Ok(())
    );
    assert_eq!(
        code(
            T::SET_VERSIONSTAMPED_VALUE,
            b"k",
            &stamped(0, 100_000 - 13, 0)
        )
        .unwrap_err(),
        error_code::VALUE_TOO_LARGE
    );
    // A system key has the larger limit.
    let mut key = stamped(1, 20_000, 1);
    key[0] = 0xff;
    assert_eq!(code(T::SET_VERSIONSTAMPED_KEY, &key, b""), Ok(()));
}

#[test]
fn a_failed_versionstamp_operation_buffers_nothing() {
    let mut b = CommitBuffer::new(limits());
    assert!(b.set_versionstamped_key(b"abc", b"v").is_err());
    assert!(b.set_versionstamped_value(b"k", b"abc").is_err());
    assert!(b.is_empty());
    assert!(build(b, &CommitOptions::default()).unwrap().is_none());
}

#[test]
fn a_versionstamped_key_has_no_write_conflict_range_a_value_has_one() {
    let key = stamped(4, 3, 4);
    let value = stamped(1, 2, 1);
    let mut b = CommitBuffer::new(limits());
    b.set_versionstamped_key(&key, b"v").unwrap();
    assert!(!b.is_empty(), "a mutation is something to commit");
    let options = CommitOptions {
        causal_write_risky: true,
        ..CommitOptions::default()
    };
    // The request carries the key with its offset, as the proxy expects it (it strips it).
    let c = build(b.clone(), &options).unwrap().unwrap();
    assert_eq!(
        c.mutations,
        vec![Mutation::new(MutationType::SET_VERSIONSTAMPED_KEY, key.clone(), "v").unwrap()]
    );
    assert!(c.write_conflict_ranges.is_empty() && c.read_conflict_ranges.is_empty());

    // Without `causal_write_risky` the transaction is a blind write: the self-conflicting key.
    let c = build(b, &CommitOptions::default()).unwrap().unwrap();
    let sc = fdb_native_schema::self_conflict_key(uid()).unwrap();
    assert_eq!(c.write_conflict_ranges, vec![KeyRange::single_key(&sc)]);
    assert_eq!(c.read_conflict_ranges, vec![KeyRange::single_key(&sc)]);
    assert_eq!(c.mutations.len(), 1);

    // The value operation writes a known key: the usual single-key range, and the mutation
    // keeps the value's offset.
    let mut b = CommitBuffer::new(limits());
    b.set_versionstamped_value(b"k", &value).unwrap();
    let c = build(b, &options).unwrap().unwrap();
    assert_eq!(
        c.mutations,
        vec![Mutation::new(MutationType::SET_VERSIONSTAMPED_VALUE, "k", value).unwrap()]
    );
    assert_eq!(c.write_conflict_ranges, vec![KeyRange::single_key(b"k")]);

    // Both, with a read of the value's key: it intersects the value's write range, no
    // self-conflict; and the key adds nothing.
    let mut b = CommitBuffer::new(limits());
    b.set_versionstamped_key(&key, b"v").unwrap();
    b.set_versionstamped_value(b"k", &stamped(0, 0, 0)).unwrap();
    b.add_read_conflict_range(b"k", b"l").unwrap();
    let c = build(b, &CommitOptions::default()).unwrap().unwrap();
    assert_eq!(c.write_conflict_ranges, vec![KeyRange::single_key(b"k")]);
    assert_eq!(c.read_conflict_ranges, vec![r("k", "l")]);
    assert_eq!(
        c.mutations.iter().map(|m| m.kind).collect::<Vec<_>>(),
        vec![
            MutationType::SET_VERSIONSTAMPED_KEY,
            MutationType::SET_VERSIONSTAMPED_VALUE
        ]
    );
    // An explicit write conflict range is kept as given.
    let mut b = CommitBuffer::new(limits());
    b.set_versionstamped_key(&key, b"v").unwrap();
    b.add_write_conflict_range(b"log/", b"log0").unwrap();
    let c = build(b, &options).unwrap().unwrap();
    assert_eq!(c.write_conflict_ranges, vec![r("log/", "log0")]);
}

#[test]
fn versionstamped_mutations_count_their_offset_in_the_size() {
    let key = stamped(2, 6, 2); // 22 bytes
    let value = b"vv".to_vec();
    let mut b = CommitBuffer::new(limits());
    b.set_versionstamped_key(&key, &value).unwrap();
    let options = |size_limit| CommitOptions {
        causal_write_risky: true,
        size_limit,
        ..CommitOptions::default()
    };
    // No conflict ranges at all: the size is the mutation's.
    let size = MUTATION_REF_SIZE + 22 + 2;
    assert!(build(b.clone(), &options(size)).is_ok());
    assert_eq!(
        build(b.clone(), &options(size - 1)).unwrap_err().code(),
        error_code::TRANSACTION_TOO_LARGE
    );

    // A versionstamped value also has its key's write range (two key copies).
    let mut b = CommitBuffer::new(limits());
    b.set_versionstamped_value(b"kk", &key).unwrap();
    let size = MUTATION_REF_SIZE + 2 + 22 + KEY_RANGE_REF_SIZE + 2 + 3;
    assert!(build(b.clone(), &options(size)).is_ok());
    assert_eq!(
        build(b, &options(size - 1)).unwrap_err().code(),
        error_code::TRANSACTION_TOO_LARGE
    );
}

#[test]
fn raw_mutations_are_checked_when_the_request_is_built() {
    // `Committer::commit` takes mutations that never went through the buffer.
    let bad = Mutation::new(MutationType::SET_VERSIONSTAMPED_KEY, "abc", "v").unwrap();
    let e = prepare(
        100,
        vec![bad],
        vec![],
        vec![],
        &CommitOptions::default(),
        &limits(),
        uid,
    )
    .unwrap_err();
    assert_eq!(e.code(), error_code::CLIENT_INVALID_OPERATION);
    let good = Mutation::new(
        MutationType::SET_VERSIONSTAMPED_VALUE,
        "k",
        stamped(0, 0, 0),
    )
    .unwrap();
    let c = prepare(
        100,
        vec![good.clone()],
        vec![],
        vec![],
        &CommitOptions::default(),
        &limits(),
        uid,
    )
    .unwrap()
    .unwrap();
    assert_eq!(c.mutations, vec![good]);
}

#[test]
fn with_versionstamp_offset_appends_four_little_endian_bytes() {
    assert_eq!(
        with_versionstamp_offset(b"ab".to_vec(), 0x0102),
        b"ab\x02\x01\0\0"
    );
    let key = with_versionstamp_offset([&b"x"[..], &[0xff; 10]].concat(), 1);
    assert!(
        CommitBuffer::new(limits())
            .set_versionstamped_key(&key, b"")
            .is_ok()
    );
}

#[test]
fn intersects_sorts_and_finds_the_first_overlap() {
    let mut w = vec![r("m", "p"), r("a", "b")];
    let mut rd = vec![r("o", "z"), r("a", "a\0")];
    assert_eq!(intersects(&mut w, &mut rd), Some(r("a", "a\0")));
    assert_eq!(w, vec![r("a", "b"), r("m", "p")]);
    assert_eq!(rd, vec![r("a", "a\0"), r("o", "z")]);
    let mut w = vec![r("a", "b")];
    let mut rd = vec![r("b", "c")];
    assert_eq!(intersects(&mut w, &mut rd), None);
    assert_eq!(intersects(&mut [], &mut rd), None);
}

#[test]
fn errors_and_codes() {
    use fdb_native_transport::rpc::RequestError;
    use fdb_native_wire::FdbError;
    let reply = |code| Error::from_commit_request(RequestError::Fdb(FdbError::new(code)));
    let unknown = Error::new(1021);
    assert!(unknown.is_maybe_committed() && unknown.is_retryable());
    let nc = reply(1020);
    assert_eq!(nc.code(), 1020);
    assert!(nc.is_retryable_not_committed() && !nc.is_maybe_committed());
    assert_eq!(reply(1007).code(), 1007);
    for code in [1038, 1042, 1051, 1037, 1213, 1223, 1235, 1242, 1078, 1009] {
        assert!(reply(code).is_retryable(), "{code}");
        assert!(reply(code).transaction_retry().is_some(), "{code}");
    }
    assert!(!reply(2101).is_retryable());
    assert!(!super::error::key_too_large(1, 0).is_retryable());
    assert_eq!(super::error::closed().code(), 1101);
}

#[test]
fn versionstamp_is_big_endian() {
    use fdb_native_schema::domain::CommitId;
    let id = CommitId {
        version: 0x0102_0304_0506_0708,
        txn_batch_id: 0x0a0b,
        ..CommitId::default()
    };
    assert_eq!(versionstamp(&id), [1, 2, 3, 4, 5, 6, 7, 8, 0x0a, 0x0b]);
    assert_eq!(CommitOutcome::NothingToCommit.version(), -1);
    assert_eq!(CommitOutcome::NothingToCommit.versionstamp(), None);
    assert_eq!(
        CommitOutcome::Committed(id).version(),
        0x0102_0304_0506_0708
    );
}

#[test]
fn raw_min_and_and_become_the_v2_operations() {
    let ms = vec![
        Mutation::new(MutationType::MIN, "a", "x").unwrap(),
        Mutation::new(MutationType::AND, "b", "x").unwrap(),
        Mutation::new(MutationType::MAX, "c", "x").unwrap(),
    ];
    let c = prepare(
        1,
        ms,
        vec![],
        vec![],
        &CommitOptions::default(),
        &limits(),
        uid,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        c.mutations.iter().map(|m| m.kind).collect::<Vec<_>>(),
        vec![
            MutationType::MIN_V2,
            MutationType::AND_V2,
            MutationType::MAX
        ]
    );
}

#[test]
fn not_sent_is_retryable_and_safe() {
    use fdb_native_transport::rpc::RequestError;
    let e = Error::from_commit_request(RequestError::NotSent(None));
    assert_eq!(e.code(), error_code::CONNECTION_FAILED);
    assert!(e.is_retryable() && e.is_retryable_not_committed() && !e.is_maybe_committed());
    assert_eq!(
        e.transaction_retry(),
        Some(crate::TransactionRetry::Backoff)
    );
    // A plain `connection_failed` is not retried.
    assert_eq!(
        Error::new(error_code::CONNECTION_FAILED).transaction_retry(),
        None
    );
}

#[test]
fn the_dummy_range_survives_a_truncated_key() {
    let limits = CommitLimits::for_schema(SchemaVersion::V8_0);
    let key = vec![b'a'; limits.key_size_limit + 1];
    let overlap = KeyRange::new(key.clone(), "b").unwrap();
    // The single-key range of a truncated key is cut to nothing...
    assert_eq!(
        super::limits::normalize_range(&KeyRange::single_key(&key), &limits).unwrap(),
        None
    );
    // ...so the overlap is used.
    assert_eq!(super::committer::dummy_range(&overlap, &limits), overlap);
    let short = KeyRange::new("k", "z").unwrap();
    assert_eq!(
        super::committer::dummy_range(&short, &limits),
        KeyRange::single_key(b"k")
    );
}
