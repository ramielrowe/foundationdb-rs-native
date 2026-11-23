//! The request construction, without a network.

use fdb_native_schema::SchemaVersion;
use fdb_native_schema::domain::{Commit, KeyRange, Mutation, MutationType};
use fdb_native_wire::Uid;

use super::error::error_code;
use super::limits::{KEY_RANGE_REF_SIZE, MUTATION_REF_SIZE, expected_size};
use super::prepare::{intersects, prepare};
use super::*;

fn limits() -> CommitLimits {
    CommitLimits::for_schema(SchemaVersion::V7_4)
}

fn uid() -> Uid {
    Uid::new(1, 2)
}

fn build(buffer: CommitBuffer, options: &CommitOptions) -> Result<Option<Commit>, CommitError> {
    let (m, r, w) = buffer.into_parts();
    prepare(100, m, r, w, options, &limits(), uid)
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
            .unwrap_err(),
        CommitError::InvertedRange
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
    assert!(matches!(
        b.set(&vec![b'k'; 10_001], b"v"),
        Err(CommitError::KeyTooLarge {
            size: 10_001,
            limit: 10_000
        })
    ));
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
    assert_eq!(
        e,
        CommitError::TransactionTooLarge {
            size: 10_000_017,
            limit: 10_000_000
        }
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
        build(b, &options).unwrap_err(),
        CommitError::TransactionReadOnly
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
    let mut b = CommitBuffer::new(limits());
    for t in [
        MutationType::SET_VERSIONSTAMPED_KEY,
        MutationType::SET_VERSIONSTAMPED_VALUE,
        MutationType(5),
        MutationType::SET_VALUE,
    ] {
        assert_eq!(b.atomic_op(b"k", b"v", t).unwrap_err().code(), 2000);
    }
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
    use fdb_native_wire::FdbError;
    assert_eq!(CommitError::CommitUnknownResult.code(), 1021);
    assert!(CommitError::CommitUnknownResult.maybe_committed());
    assert!(!CommitError::CommitUnknownResult.is_safe_to_retry());
    let nc = CommitError::from_reply_code(FdbError::new(1020));
    assert_eq!(nc.code(), 1020);
    assert!(nc.is_retryable() && nc.is_safe_to_retry());
    assert_eq!(
        CommitError::from_reply_code(FdbError::new(1007)),
        CommitError::TransactionTooOld
    );
    for code in [1038, 1042, 1051, 1037, 1213, 1223, 1235, 1242, 1078, 1009] {
        assert!(CommitError::from_reply_code(FdbError::new(code)).is_retryable());
    }
    assert!(!CommitError::from_reply_code(FdbError::new(2101)).is_retryable());
    assert!(!CommitError::KeyTooLarge { size: 1, limit: 0 }.is_retryable());
    assert_eq!(CommitError::Closed.code(), 1101);
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
