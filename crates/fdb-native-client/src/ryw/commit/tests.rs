//! Tests of `write_range_to_native`: write maps built with the write map's own operations,
//! and the mutations and write conflict ranges C++'s `writeRangeToNativeTransaction` gives
//! the native transaction for them (worked out from `ReadYourWrites.actor.cpp`
//! 7.4:1997-2072 and `NativeAPI.actor.cpp` 7.4:6277-6400: clears first, then conflict runs
//! and operation stacks bottom to top, every mutation with `AddConflictRange::False`).

use fdb_native_schema::SchemaVersion;
use fdb_native_schema::domain::{KeyRange, Mutation, MutationType as M};

use super::*;
use crate::commit::CommitLimits;

fn buffer() -> CommitBuffer {
    CommitBuffer::new(CommitLimits::for_schema(SchemaVersion::V8_0))
}

/// The mutations and write conflict ranges `writes` gives over `[begin, end)`.
fn native_range(writes: &WriteMap, begin: &[u8], end: &[u8]) -> (Vec<Mutation>, Vec<KeyRange>) {
    let mut native = buffer();
    write_range_to_native(writes, begin, end, &mut native).expect("written");
    assert!(!native.skip_write_conflict(), "the flag is put back");
    let (mutations, reads, conflicts) = native.into_parts();
    assert!(reads.is_empty());
    (mutations, conflicts)
}

/// ... over the whole key space, as the commit does.
fn native(writes: &WriteMap) -> (Vec<Mutation>, Vec<KeyRange>) {
    native_range(writes, b"", b"\xff\xff")
}

fn set(k: &[u8], v: &[u8]) -> Mutation {
    Mutation::new(M::SET_VALUE, k, v).unwrap()
}

fn clear(b: &[u8], e: &[u8]) -> Mutation {
    Mutation::new(M::CLEAR_RANGE, b, e).unwrap()
}

fn op(kind: M, k: &[u8], v: &[u8]) -> Mutation {
    Mutation::new(kind, k, v).unwrap()
}

fn range(b: &[u8], e: &[u8]) -> KeyRange {
    KeyRange::new(b, e).unwrap()
}

#[test]
fn an_empty_map_sends_nothing() {
    assert_eq!(native(&WriteMap::new()), (vec![], vec![]));
}

#[test]
fn sets_in_key_order_with_coalesced_conflict_ranges() {
    let mut w = WriteMap::new();
    w.mutate(b"b", M::SET_VALUE, b"2", true).unwrap();
    w.mutate(b"a", M::SET_VALUE, b"1", true).unwrap();
    w.mutate(b"a\x00", M::SET_VALUE, b"3", true).unwrap();
    w.mutate(b"a", M::SET_VALUE, b"4", true).unwrap();
    // `[a, a\0)` and `[a\0, a\0\0)` touch: one run.
    assert_eq!(
        native(&w),
        (
            vec![set(b"a", b"4"), set(b"a\x00", b"3"), set(b"b", b"2")],
            vec![range(b"a", b"a\x00\x00"), range(b"b", b"b\x00")],
        )
    );
}

#[test]
fn clears_come_first_and_stop_at_the_keys_set_in_them() {
    let mut w = WriteMap::new();
    w.mutate(b"b", M::SET_VALUE, b"1", true).unwrap();
    w.clear(b"a", b"c", true);
    w.mutate(b"b", M::SET_VALUE, b"2", true).unwrap();
    w.clear(b"x", b"y", true);
    // The set of `b` is an operation inside the cleared run: the clear goes around it.
    assert_eq!(
        native(&w),
        (
            vec![
                clear(b"a", b"b"),
                clear(b"b\x00", b"c"),
                clear(b"x", b"y"),
                set(b"b", b"2"),
            ],
            vec![range(b"a", b"c"), range(b"x", b"y")],
        )
    );
}

#[test]
fn atomic_operations_coalesce_or_stack_bottom_to_top() {
    let mut w = WriteMap::new();
    w.mutate(b"k", M::ADD_VALUE, &[1], true).unwrap();
    w.mutate(b"k", M::ADD_VALUE, &[2], true).unwrap();
    // Another operation: on top.
    w.mutate(b"k", M::OR, &[8], true).unwrap();
    // `AddValue` with operands of other lengths does not coalesce either.
    w.mutate(b"n", M::ADD_VALUE, &[1], true).unwrap();
    w.mutate(b"n", M::ADD_VALUE, &[1, 0], true).unwrap();
    // `ByteMax` is associative: it coalesces whatever the lengths.
    w.mutate(b"m", M::BYTE_MAX, b"ab", true).unwrap();
    w.mutate(b"m", M::BYTE_MAX, b"b", true).unwrap();
    assert_eq!(
        native(&w),
        (
            vec![
                op(M::ADD_VALUE, b"k", &[3]),
                op(M::OR, b"k", &[8]),
                op(M::BYTE_MAX, b"m", b"b"),
                op(M::ADD_VALUE, b"n", &[1]),
                op(M::ADD_VALUE, b"n", &[1, 0]),
            ],
            vec![
                range(b"k", b"k\x00"),
                range(b"m", b"m\x00"),
                range(b"n", b"n\x00")
            ],
        )
    );
}

#[test]
fn atomic_operations_fold_into_sets_and_clears() {
    let mut w = WriteMap::new();
    w.mutate(b"a", M::SET_VALUE, &[1], true).unwrap();
    w.mutate(b"a", M::ADD_VALUE, &[2], true).unwrap();
    // A `CompareAndClear` that matches makes the set a clear of the key.
    w.mutate(b"c", M::SET_VALUE, b"x", true).unwrap();
    w.mutate(b"c", M::COMPARE_AND_CLEAR, b"x", true).unwrap();
    // One that does not leaves the set.
    w.mutate(b"d", M::SET_VALUE, b"x", true).unwrap();
    w.mutate(b"d", M::COMPARE_AND_CLEAR, b"y", true).unwrap();
    // An atomic operation on a cleared key is a set of its result.
    w.clear(b"m", b"p", true);
    w.mutate(b"n", M::APPEND_IF_FITS, b"z", true).unwrap();
    assert_eq!(
        native(&w),
        (
            vec![
                clear(b"m", b"n"),
                clear(b"n\x00", b"p"),
                set(b"a", &[3]),
                clear(b"c", b"c\x00"),
                set(b"d", b"x"),
                set(b"n", b"z"),
            ],
            vec![
                range(b"a", b"a\x00"),
                range(b"c", b"c\x00"),
                range(b"d", b"d\x00"),
                range(b"m", b"p"),
            ],
        )
    );
}

#[test]
fn writes_without_a_conflict_range_and_conflict_ranges_without_writes() {
    let mut w = WriteMap::new();
    w.mutate(b"a", M::SET_VALUE, b"1", false).unwrap();
    w.clear(b"c", b"e", false);
    w.add_conflict_range(b"x", b"z");
    // A later write with a conflict range in the cleared range: the range is cut there.
    w.mutate(b"d", M::SET_VALUE, b"2", true).unwrap();
    // A set without one keeps the conflict range the key had.
    w.mutate(b"y", M::SET_VALUE, b"3", false).unwrap();
    assert_eq!(
        native(&w),
        (
            vec![
                clear(b"c", b"d"),
                clear(b"d\x00", b"e"),
                set(b"a", b"1"),
                set(b"d", b"2"),
                set(b"y", b"3"),
            ],
            vec![range(b"d", b"d\x00"), range(b"x", b"z")],
        )
    );
}

#[test]
fn a_clear_without_a_conflict_range_keeps_the_conflict_ranges_in_it() {
    let mut w = WriteMap::new();
    w.mutate(b"b", M::SET_VALUE, b"1", true).unwrap();
    w.add_conflict_range(b"d", b"f");
    w.clear(b"a", b"e", false);
    assert_eq!(
        native(&w),
        (
            vec![clear(b"a", b"e")],
            vec![range(b"b", b"b\x00"), range(b"d", b"f")],
        )
    );
}

#[test]
fn a_part_of_the_map_is_cut_at_its_bounds() {
    let mut w = WriteMap::new();
    w.clear(b"a", b"z", true);
    w.mutate(b"b", M::SET_VALUE, b"1", true).unwrap();
    w.mutate(b"m", M::SET_VALUE, b"2", true).unwrap();
    w.add_conflict_range(b"z", b"zz");
    assert_eq!(
        native_range(&w, b"c", b"n"),
        (
            vec![clear(b"c", b"m"), clear(b"m\x00", b"n"), set(b"m", b"2")],
            vec![range(b"c", b"n")],
        )
    );
    // A bound inside an operation's segment `[k, k\0)` is not possible (keys are whole);
    // a bound at `k\0` leaves `k` out.
    assert_eq!(
        native_range(&w, b"b\x00", b"c"),
        (vec![clear(b"b\x00", b"c")], vec![range(b"b\x00", b"c")])
    );
}

#[test]
fn versionstamped_operations_go_as_they_are() {
    let mut w = WriteMap::new();
    let mut key = b"v".to_vec();
    key.extend([0; 10]);
    key.extend(1i32.to_le_bytes());
    w.mutate(&key, M::SET_VERSIONSTAMPED_KEY, b"x", false)
        .unwrap();
    let mut value = vec![0; 10];
    value.extend(0i32.to_le_bytes());
    w.mutate(b"w", M::SET_VERSIONSTAMPED_VALUE, &value, true)
        .unwrap();
    // A set of an unreadable key stacks on the versionstamped operation.
    w.mutate(b"w", M::SET_VALUE, b"after", true).unwrap();
    assert_eq!(
        native(&w),
        (
            vec![
                op(M::SET_VERSIONSTAMPED_KEY, &key, b"x"),
                op(M::SET_VERSIONSTAMPED_VALUE, b"w", &value),
                set(b"w", b"after"),
            ],
            vec![range(b"w", b"w\x00")],
        )
    );
}

#[test]
fn the_skip_flag_of_the_buffer_is_kept() {
    let mut w = WriteMap::new();
    w.add_conflict_range(b"a", b"b");
    let mut native = buffer();
    native.set_skip_write_conflict(true);
    write_range_to_native(&w, b"", b"\xff\xff", &mut native).unwrap();
    assert!(native.skip_write_conflict());
    // `add_write_conflict_range` is not affected by the flag.
    let (_, _, conflicts) = native.into_parts();
    assert_eq!(conflicts, vec![range(b"a", b"b")]);
}
