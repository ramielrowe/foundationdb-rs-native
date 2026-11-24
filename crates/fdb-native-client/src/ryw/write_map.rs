//! The write map: a transaction's own writes, as segments of the key space. A port of
//! `WriteMap` (`fdbclient/include/fdbclient/WriteMap.h` and `fdbclient/WriteMap.cpp` on
//! `release-7.4`; `fdbclient/WriteMap.h` and `fdbclient/WriteMap.cpp` on `release-8.0`).
//!
//! # What it holds
//!
//! The key space `["", \xff\xff)` is cut into **segments**; every key is in exactly one:
//!
//! * an **operation** at a single key `k` (the segment `[k, k\x00)`): the key's
//!   [`OperationStack`], the transaction's set / atomic operations on it, coalesced as far as
//!   they can be. It is *independent* if the value it gives does not depend on the key's value
//!   in the database (it starts with a set, or the key was cleared first), *dependent*
//!   otherwise (atomic operations on a value the transaction has not written);
//! * a **range** of keys with no operation, either *cleared* by the transaction or
//!   *unmodified* by it.
//!
//! Each segment also says whether its keys are in the transaction's **write conflict ranges**
//! and whether they are **unreadable**: their values depend on the commit's versionstamp (the
//! keys a `SetVersionstampedKey` may write, a key with a `SetVersionstampedValue`), so reading
//! them is `accessed_unreadable` (1036) until the commit.
//!
//! # Representation
//!
//! As in C++, the map is a sorted set of **entries**, one per segment boundary. An entry at
//! key `k` holds `k`'s operation stack (empty if `k` has none) and five flags: whether the keys
//! after `k` (after `k\x00` if `k` has an operation), up to the next entry, are cleared, in
//! the write conflict ranges, and unreadable (`following_keys_*`), and whether `k`'s own
//! operation is in the conflict ranges and unreadable (`is_conflict`, `is_unreadable`). Three
//! sentinel entries, at `""`, `\xff\xff` and `\xff\xff\x00`, bound the key space.
//!
//! C++ keeps the entries in a persistent treap (`PTree`, `VersionedMap.h`) and every
//! iterator is a snapshot of one version: writes made while an iterator lives do not change
//! what it sees (the read path relies on this: a read that waits for a storage server keeps
//! its iterator over the waits). Here the entries are an [`rpds::RedBlackTreeMapSync`], a
//! persistent ordered map (a red-black tree whose nodes are shared by reference count): a
//! [`WriteMapIter`] holds a clone of the map (a reference count), and a write while it lives copies only the nodes on
//! the path to the entry it changes (O(log n)), not the whole map; with no iterator alive a
//! write changes the nodes in place. An entry's operation stack is behind an [`Arc`] too, so
//! copying an entry is cheap. The iterator also keeps copies of the entries of its segment and
//! of the one after it, made when it moves, so its accessors do no lookups. Keys and values are
//! `Arc<[u8]>` (cloning one is a reference count, not a copy), in place of C++'s arena
//! references. Segment bounds are the snapshot cache's [`ExtKey`] (`ExtStringRef`).
//!
//! # Use by the read-your-writes layer
//!
//! * Writes (`ReadYourWritesTransaction::set`, `clear`, `atomicOp`,
//!   `addWriteConflictRange`): [`WriteMap::mutate`], [`WriteMap::clear`],
//!   [`WriteMap::add_conflict_range`], and for a `SetVersionstampedKey`
//!   [`WriteMap::add_unmodified_and_unreadable_range`] over
//!   [`versionstamp_key_range`](crate::atomic::versionstamp_key_range) (after writing that
//!   range's existing writes to the commit: `writeRangeToNativeTransaction`).
//! * Reads (`RYWIterator`, which walks a [`WriteMapIter`] and a snapshot cache iterator
//!   together): [`skip`](WriteMapIter::skip), [`advance`](WriteMapIter::advance),
//!   [`retreat`](WriteMapIter::retreat), [`segment_type`](WriteMapIter::segment_type),
//!   [`is_unreadable`](WriteMapIter::is_unreadable), [`begin_key`](WriteMapIter::begin_key)
//!   and [`end_key`](WriteMapIter::end_key), and for an operation
//!   [`WriteMap::coalesce_under`] of its [`op`](WriteMapIter::op) over the value read from
//!   the database (`RYWIterator::kv`).
//! * Read conflict ranges (`updateConflictMap`): a read adds a read conflict range only where
//!   its keys are unmodified or have a dependent operation
//!   ([`is_unmodified_range`](WriteMapIter::is_unmodified_range),
//!   [`is_operation`](WriteMapIter::is_operation),
//!   [`is_independent`](WriteMapIter::is_independent)).
//! * The commit (`writeRangeToNativeTransaction`, `getWriteConflicts`): the cleared ranges,
//!   then each operation stack's mutations in order, and the conflict ranges
//!   ([`is_cleared_range`](WriteMapIter::is_cleared_range), [`op`](WriteMapIter::op),
//!   [`is_conflict_range`](WriteMapIter::is_conflict_range)).
//!
//! The write map keeps no size: C++'s transaction size estimate (`approximateSize`) is
//! counted by `ReadYourWritesTransaction` from each call's arguments, not from the map.
//!
//! # 7.4 and 8.0
//!
//! The two releases have the same write map. 8.0 moved `WriteMap.h` from
//! `fdbclient/include/fdbclient/` to `fdbclient/`, passes `WriteMapEntry`'s flags as
//! `FDB_BOOLEAN_PARAM`s (`FollowingKeysCleared`, ...) instead of `bool`s, copies instead of
//! moving the entries it inserts, and adds braces; the logic is unchanged. The
//! `/fdbclient/WriteMap/*` unit tests (in `RYWIterator.cpp`) are the same in both, but 8.0
//! moved their random key helpers to `RandomTestUtils.h`.
//!
//! # Line references
//!
//! `WriteMap.cpp` 7.4 (8.0): `OperationStack` 23-76 (23-77), `mutate` 87-158 (88-164),
//! `clear` 160-203 (166-223), `addUnmodifiedAndUnreadableRange` 205-242 (225-276),
//! `addConflictRange` 244-303 (278-342), `iterator::type` 305-310 (344-349), `operator++`
//! 312-322 (351-361), `operator--` 323-333 (362-372), `skip` 335-347 (374-386), `coalesce`
//! 357-478 (396-518), `coalesceOver` 480-496 (520-536), `coalesceUnder` 498-508 (538-548),
//! `clearNoConflict` 527-596 (567-645). `WriteMap.h` 7.4 (8.0): `RYWMutation` 30-39,
//! `OperationStack` 41-67, `WriteMapEntry` 69-98 (75-104), `WriteMap` and its iterator
//! 122-226 (128-254).

use std::cmp::Ordering;
use std::fmt;
use std::ops::Bound;
use std::sync::Arc;

use fdb_native_schema::domain::MutationType;
use rpds::RedBlackTreeMapSync;

use crate::atomic::{self, VALUE_SIZE_LIMIT};
use crate::error::Error;
use crate::error::codes::{INVALID_MUTATION_TYPE, KEY_OUTSIDE_LEGAL_RANGE};
use crate::ryw::ExtKey;

/// `operation_failed` (1000): `WriteMap::coalesce` of two operations it cannot combine.
const OPERATION_FAILED: u16 = 1000;

/// `allKeys.end`: the end of the key space the write map covers.
const ALL_KEYS_END: &[u8] = b"\xff\xff";

/// `afterAllKeys`: the last sentinel entry.
const AFTER_ALL_KEYS: &[u8] = b"\xff\xff\x00";

/// `MutationRef::ATOMIC_MASK` (`CommitTransaction.h`, the same on 7.4 and 8.0): the atomic
/// operations, versionstamped sets included.
fn is_atomic_op(op: MutationType) -> bool {
    matches!(
        op,
        MutationType::ADD_VALUE
            | MutationType::AND
            | MutationType::OR
            | MutationType::XOR
            | MutationType::APPEND_IF_FITS
            | MutationType::MAX
            | MutationType::MIN
            | MutationType::SET_VERSIONSTAMPED_KEY
            | MutationType::SET_VERSIONSTAMPED_VALUE
            | MutationType::BYTE_MIN
            | MutationType::BYTE_MAX
            | MutationType::MIN_V2
            | MutationType::AND_V2
            | MutationType::COMPARE_AND_CLEAR
    )
}

/// `MutationRef::NON_ASSOCIATIVE_MASK`: the operations that are associative only when their
/// operands have the same length.
fn is_non_associative_op(op: MutationType) -> bool {
    matches!(
        op,
        MutationType::ADD_VALUE
            | MutationType::OR
            | MutationType::XOR
            | MutationType::MAX
            | MutationType::MIN
            | MutationType::SET_VERSIONSTAMPED_KEY
            | MutationType::SET_VERSIONSTAMPED_VALUE
            | MutationType::MIN_V2
            | MutationType::COMPARE_AND_CLEAR
    )
}

/// `equalsKeyAfter(key, compare_key)`: whether `compare_key` is `key\x00`.
fn equals_key_after(key: &[u8], compare_key: &[u8]) -> bool {
    compare_key.len() == key.len() + 1
        && compare_key[key.len()] == 0
        && compare_key.starts_with(key)
}

/// `RYWMutation`: one operation on a key: its type and its parameter (`None` only for a
/// `SetValue` that clears the key, the result of coalescing a `CompareAndClear` or a
/// cleared key's base).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RywMutation {
    /// The value or operand (`None`: the key is cleared).
    pub(crate) value: Option<Arc<[u8]>>,
    /// The operation.
    pub(crate) kind: MutationType,
}

impl RywMutation {
    /// The operation `kind` with `value`.
    pub(crate) fn new(kind: MutationType, value: Option<&[u8]>) -> RywMutation {
        RywMutation {
            value: value.map(Arc::from),
            kind,
        }
    }

    /// A `SetValue` of `value`, or a clear of the key if `None`.
    fn set(value: Option<Arc<[u8]>>) -> RywMutation {
        RywMutation {
            value,
            kind: MutationType::SET_VALUE,
        }
    }
}

/// `OperationStack`: a key's operations, oldest first, as far as they could be coalesced.
///
/// The first operation decides whether the stack is dependent on the key's value in the
/// database (see [`is_dependent`](Self::is_dependent)); the ones above it are atomic
/// operations that could not be folded into the one below (different operations, operands
/// of different lengths for the non-associative ones), or any operation on an unreadable key.
/// C++ keeps the first operation apart from a vector of the others to save an allocation;
/// here it is one vector.
///
/// The vector is behind an [`Arc`] (and an empty stack has none): cloning a stack, as copying
/// a write map entry does, is a reference count, and the first change to a shared stack copies
/// it (a few operations).
#[derive(Clone, Default)]
pub(crate) struct OperationStack {
    ops: Option<Arc<Vec<RywMutation>>>,
}

impl OperationStack {
    /// A stack of `initial` (`OperationStack(RYWMutation)`).
    pub(crate) fn new(initial: RywMutation) -> OperationStack {
        OperationStack {
            ops: Some(Arc::new(vec![initial])),
        }
    }

    /// The operations, to change them (a copy if the stack is shared).
    fn ops_mut(&mut self) -> &mut Vec<RywMutation> {
        Arc::make_mut(self.ops.get_or_insert_with(Arc::default))
    }

    /// `reset`: the stack becomes `initial` alone.
    pub(crate) fn reset(&mut self, initial: RywMutation) {
        *self = OperationStack::new(initial);
    }

    /// `poppush`: replaces the top operation (pushes onto an empty stack).
    pub(crate) fn poppush(&mut self, entry: RywMutation) {
        let ops = self.ops_mut();
        ops.pop();
        ops.push(entry);
    }

    /// `push`: adds an operation on top.
    pub(crate) fn push(&mut self, entry: RywMutation) {
        self.ops_mut().push(entry);
    }

    /// `isDependent`: whether the value the stack gives depends on the key's value in the
    /// database: the first operation is not a `SetValue` (or `ClearRange`), nor a
    /// versionstamped set. An empty stack is not dependent.
    pub(crate) fn is_dependent(&self) -> bool {
        match self.as_slice().first() {
            None => false,
            Some(first) => !matches!(
                first.kind,
                MutationType::SET_VALUE
                    | MutationType::CLEAR_RANGE
                    | MutationType::SET_VERSIONSTAMPED_VALUE
                    | MutationType::SET_VERSIONSTAMPED_KEY
            ),
        }
    }

    /// `top`: the newest operation.
    pub(crate) fn top(&self) -> Option<&RywMutation> {
        self.as_slice().last()
    }

    /// `at(n)`: the `n`th operation, oldest first.
    pub(crate) fn at(&self, n: usize) -> Option<&RywMutation> {
        self.as_slice().get(n)
    }

    /// `size`: the number of operations.
    pub(crate) fn len(&self) -> usize {
        self.as_slice().len()
    }

    /// Whether there are no operations (a default-constructed stack, as the entries of
    /// ranges have).
    pub(crate) fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }

    /// The operations, oldest first.
    pub(crate) fn as_slice(&self) -> &[RywMutation] {
        self.ops.as_deref().map_or(&[], Vec::as_slice)
    }

    /// The operations, oldest first.
    pub(crate) fn iter(&self) -> std::slice::Iter<'_, RywMutation> {
        self.as_slice().iter()
    }
}

impl PartialEq for OperationStack {
    fn eq(&self, other: &OperationStack) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for OperationStack {}

impl fmt::Debug for OperationStack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OperationStack")
            .field("ops", &self.as_slice())
            .finish()
    }
}

impl<'a> IntoIterator for &'a OperationStack {
    type Item = &'a RywMutation;
    type IntoIter = std::slice::Iter<'a, RywMutation>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// `WriteMapEntry` without its key (the map's key): a segment boundary.
#[derive(Clone, Debug, Default)]
struct Entry {
    /// The key's operations (empty if it has none: the entry only starts a range).
    stack: OperationStack,
    /// The keys after this one (after `key\x00` if it has operations), to the next entry,
    /// are cleared.
    following_keys_cleared: bool,
    /// ... are in the write conflict ranges.
    following_keys_conflict: bool,
    /// The key's operation is in the write conflict ranges.
    is_conflict: bool,
    /// The keys after this one are unreadable.
    following_keys_unreadable: bool,
    /// The key's operation is unreadable.
    is_unreadable: bool,
}

impl Entry {
    /// An entry with no operation whose own flags are its range's (as all of C++'s range
    /// entries but `addConflictRange`'s are).
    fn range(cleared: bool, conflict: bool, unreadable: bool) -> Entry {
        Entry {
            stack: OperationStack::default(),
            following_keys_cleared: cleared,
            following_keys_conflict: conflict,
            is_conflict: conflict,
            following_keys_unreadable: unreadable,
            is_unreadable: unreadable,
        }
    }

    /// The entry has a conflict range on its key and after it.
    fn all_conflict(&self) -> bool {
        self.following_keys_conflict && self.is_conflict
    }
}

/// The entries by key: a persistent map, so cloning it (a [`WriteMapIter`] does) is a reference
/// count, and a write to a map that is shared copies only the path to the entry it changes.
type Tree = RedBlackTreeMapSync<Arc<[u8]>, Entry>;

/// `WriteMap`: a transaction's writes (see the module documentation).
#[derive(Clone)]
pub(crate) struct WriteMap {
    /// The entries; the iterators that were made from them hold their own versions.
    tree: Tree,
    /// `writeMapEmpty`: no write has been made (`addUnmodifiedAndUnreadableRange` does not
    /// count, as in C++).
    empty: bool,
    /// `addUnmodifiedAndUnreadableRange` was called.
    unreadable: bool,
}

impl Default for WriteMap {
    fn default() -> WriteMap {
        WriteMap::new()
    }
}

impl WriteMap {
    /// An empty write map: one unmodified range over the whole key space.
    ///
    /// Every transaction makes one: it is a clone of one built once per thread (the tree is
    /// persistent, so the first write copies the path it changes).
    pub(crate) fn new() -> WriteMap {
        thread_local! {
            static EMPTY: WriteMap = WriteMap::build();
        }
        EMPTY
            .try_with(WriteMap::clone)
            .unwrap_or_else(|_| WriteMap::build())
    }

    fn build() -> WriteMap {
        let mut tree = Tree::new_sync();
        for key in [&b""[..], ALL_KEYS_END, AFTER_ALL_KEYS] {
            tree.insert_mut(Arc::from(key), Entry::default());
        }
        WriteMap {
            tree,
            empty: true,
            unreadable: false,
        }
    }

    /// `empty`: whether no write has been made.
    pub(crate) fn is_empty(&self) -> bool {
        self.empty
    }

    /// Whether the map is as [`new`](Self::new) made it: one unmodified, readable range
    /// over the whole key space (no write, clear, conflict range or unreadable range was
    /// added). A read then sees the snapshot cache alone (`RYWIterator`'s types are the
    /// cache's), and every key it reads is unmodified for its read conflict range.
    pub(crate) fn is_untouched(&self) -> bool {
        self.empty && !self.unreadable
    }

    /// An iterator over a snapshot of the map (later writes do not change what it sees), at
    /// the first segment.
    pub(crate) fn iter(&self) -> WriteMapIter {
        self.iter_at(b"")
    }

    /// An iterator over a snapshot of the map, at the segment of `key` ([`WriteMapIter::skip`]).
    pub(crate) fn iter_at(&self, key: &[u8]) -> WriteMapIter {
        WriteMapIter::at(self.tree.clone(), key)
    }

    /// The entries, to change them (the nodes an iterator still holds are copied on the way).
    fn tree_mut(&mut self) -> &mut Tree {
        &mut self.tree
    }

    /// `PTreeImpl::remove(begin, end)`: removes the entries in `[begin, end)`.
    fn remove_range(&mut self, begin: ExtKey<'_>, end: ExtKey<'_>) {
        if begin >= end {
            return;
        }
        let (begin, end) = (begin.to_vec(), end.to_vec());
        let doomed: Vec<Arc<[u8]>> = self
            .tree
            .range::<[u8], _>((Bound::Included(&begin[..]), Bound::Excluded(&end[..])))
            .map(|(k, _)| Arc::clone(k))
            .collect();
        if doomed.is_empty() {
            return;
        }
        let tree = self.tree_mut();
        for key in doomed {
            tree.remove_mut(&key);
        }
    }

    /// `mutate`: adds the operation `op` with parameter `param` on `key`, with a write
    /// conflict range on the key if `add_conflict` (a write without one keeps the conflict
    /// range an earlier write left).
    ///
    /// `op` is a `SetValue` or an atomic operation (versionstamped sets included); a
    /// `SetVersionstampedKey`'s key and a `SetVersionstampedValue`'s key become unreadable.
    /// The operation is coalesced with the key's earlier ones where C++ does (see
    /// [`coalesce_over`](Self::coalesce_over)): a set replaces a readable key's operations,
    /// an atomic operation on a cleared key becomes a set of its result.
    ///
    /// Errors: `key_outside_legal_range` (2004) for a key at or after `\xff\xff` (the
    /// transaction checks this first); `invalid_mutation_type` (2018) for another `op`;
    /// `operation_failed` (1000) if the operations cannot be coalesced, which the rules above
    /// never ask for. On an error the map is unchanged.
    pub(crate) fn mutate(
        &mut self,
        key: &[u8],
        op: MutationType,
        param: &[u8],
        add_conflict: bool,
    ) -> Result<(), Error> {
        if key >= ALL_KEYS_END {
            return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
        }
        if op != MutationType::SET_VALUE && !is_atomic_op(op) {
            return Err(Error::with_message(
                INVALID_MUTATION_TYPE,
                format!("mutation type {} is not a set or an atomic operation", op.0),
            ));
        }
        self.empty = false;
        let is_versionstamp = matches!(
            op,
            MutationType::SET_VERSIONSTAMPED_VALUE | MutationType::SET_VERSIONSTAMPED_KEY
        );
        let is_dependent = op != MutationType::SET_VALUE && !is_versionstamp;
        let new_op = RywMutation::new(op, Some(param));

        let it = self.iter_at(key);
        let entry = &it.cur;
        let is_cleared = entry.following_keys_cleared;
        let following_conflict = entry.following_keys_conflict;
        let is_conflict = add_conflict || it.is_conflict_range();
        let following_unreadable = entry.following_keys_unreadable;
        let is_unreadable = it.is_unreadable() || is_versionstamp;

        if *it.begin != *key {
            // A key with no entry yet: in the range after the entry before it.
            let (stack, cleared) = if it.is_cleared_range() && is_dependent {
                let mut stack = OperationStack::new(RywMutation::set(None));
                Self::coalesce_over(&mut stack, new_op)?;
                (stack, true)
            } else {
                (OperationStack::new(new_op), is_cleared)
            };
            drop(it);
            self.tree_mut().insert_mut(
                Arc::from(key),
                Entry {
                    stack,
                    following_keys_cleared: cleared,
                    following_keys_conflict: following_conflict,
                    is_conflict,
                    following_keys_unreadable: following_unreadable,
                    is_unreadable,
                },
            );
        } else if !it.is_unreadable()
            && (op == MutationType::SET_VALUE || op == MutationType::SET_VERSIONSTAMPED_VALUE)
        {
            // A set of a readable key replaces its operations.
            let begin = Arc::clone(&it.begin);
            drop(it);
            self.tree_mut().insert_mut(
                begin,
                Entry {
                    stack: OperationStack::new(new_op),
                    following_keys_cleared: is_cleared,
                    following_keys_conflict: following_conflict,
                    is_conflict,
                    following_keys_unreadable: following_unreadable,
                    is_unreadable,
                },
            );
        } else {
            let mut stack = entry.stack.clone();
            if stack.is_empty() && it.is_cleared_range() && is_dependent {
                stack.push(RywMutation::set(None));
                Self::coalesce_over(&mut stack, new_op)?;
            } else if !is_unreadable && !stack.is_empty() {
                Self::coalesce_over(&mut stack, new_op)?;
            } else {
                stack.push(new_op);
            }
            let begin = Arc::clone(&it.begin);
            let mut entry = it.cur.clone();
            drop(it);
            entry.stack = stack;
            entry.is_conflict = is_conflict;
            entry.is_unreadable = is_unreadable;
            self.tree_mut().insert_mut(begin, entry);
        }
        Ok(())
    }

    /// `end`, but not past `\xff\xff`: the map's key space.
    fn clamp_end(end: &[u8]) -> &[u8] {
        if end > ALL_KEYS_END {
            ALL_KEYS_END
        } else {
            end
        }
    }

    /// `clear`: clears `[begin, end)`, with a write conflict range over it if `add_conflict`
    /// (without one, the conflict ranges earlier writes left in it stay). The range's keys
    /// lose their operations and become readable.
    ///
    /// The transaction never clears an empty range (C++ returns first); here an empty range,
    /// and the part of a range past `\xff\xff`, is ignored.
    pub(crate) fn clear(&mut self, begin: &[u8], end: &[u8], add_conflict: bool) {
        let end = Self::clamp_end(end);
        if begin >= end {
            return;
        }
        self.empty = false;
        if !add_conflict {
            self.clear_no_conflict(begin, end);
            return;
        }
        let end_key = ExtKey::from(end);

        let mut it = self.iter_at(begin);
        let insert_begin = !it.is_cleared_range() || !it.is_conflict_range() || it.is_unreadable();

        match it.end_key().cmp(&end_key) {
            Ordering::Equal => {
                let _ = it.advance();
            }
            Ordering::Less => it.skip(end),
            Ordering::Greater => {}
        }

        let insert_end =
            (it.is_unmodified_range() || !it.is_conflict_range() || it.is_unreadable())
                && (!it.key_at_begin() || it.begin_key() != end_key);
        let end_coalesce_clear = it.is_cleared_range()
            && it.begin_key() == end_key
            && it.is_conflict_range()
            && !it.is_unreadable();
        let end_conflict = it.is_conflict_range();
        let end_cleared = it.is_cleared_range();
        let end_unreadable = it.is_unreadable();
        drop(it);

        self.remove_range(
            ExtKey::new(begin, usize::from(!insert_begin)),
            ExtKey::new(end, usize::from(end_coalesce_clear)),
        );
        if insert_begin {
            self.tree_mut()
                .insert_mut(Arc::from(begin), Entry::range(true, true, false));
        }
        if insert_end {
            self.tree_mut().insert_mut(
                Arc::from(end),
                Entry::range(end_cleared, end_conflict, end_unreadable),
            );
        }
    }

    /// `addUnmodifiedAndUnreadableRange`: `[begin, end)` becomes unmodified (its operations
    /// and clears are dropped: the transaction writes them to the commit first), unreadable,
    /// and without write conflict ranges. For a `SetVersionstampedKey`, over the keys it may
    /// write. As in C++, this does not make the map non-[empty](Self::is_empty).
    ///
    /// An empty range, and the part of a range past `\xff\xff`, is ignored.
    pub(crate) fn add_unmodified_and_unreadable_range(&mut self, begin: &[u8], end: &[u8]) {
        let end = Self::clamp_end(end);
        if begin >= end {
            return;
        }
        self.unreadable = true;
        let end_key = ExtKey::from(end);

        let mut it = self.iter_at(begin);
        let insert_begin =
            !it.is_unmodified_range() || it.is_conflict_range() || !it.is_unreadable();

        match it.end_key().cmp(&end_key) {
            Ordering::Equal => {
                let _ = it.advance();
            }
            Ordering::Less => it.skip(end),
            Ordering::Greater => {}
        }

        let insert_end = (it.is_cleared_range() || it.is_conflict_range() || !it.is_unreadable())
            && (!it.key_at_begin() || it.begin_key() != end_key);
        let end_coalesce_unmodified = it.is_unmodified_range()
            && it.begin_key() == end_key
            && !it.is_conflict_range()
            && it.is_unreadable();
        let end_conflict = it.is_conflict_range();
        let end_cleared = it.is_cleared_range();
        let end_unreadable = it.is_unreadable();
        drop(it);

        self.remove_range(
            ExtKey::new(begin, usize::from(!insert_begin)),
            ExtKey::new(end, usize::from(end_coalesce_unmodified)),
        );
        if insert_begin {
            self.tree_mut()
                .insert_mut(Arc::from(begin), Entry::range(false, false, true));
        }
        if insert_end {
            self.tree_mut().insert_mut(
                Arc::from(end),
                Entry::range(end_cleared, end_conflict, end_unreadable),
            );
        }
    }

    /// `addConflictRange`: adds `[begin, end)` to the write conflict ranges, leaving the
    /// writes in it as they are.
    ///
    /// The transaction never adds an empty range (C++ returns first); here an empty range,
    /// and the part of a range past `\xff\xff`, is ignored.
    pub(crate) fn add_conflict_range(&mut self, begin: &[u8], end: &[u8]) {
        let end = Self::clamp_end(end);
        if begin >= end {
            return;
        }
        self.empty = false;
        let end_key = ExtKey::from(end);

        // C++ removes each entry it replaces before inserting the new one (`PTreeImpl::insert`
        // does not replace); `BTreeMap::insert` does, so only the insertions are kept.
        let mut insertions: Vec<(Arc<[u8]>, Entry)> = Vec::new();

        let mut it = self.iter_at(begin);
        let entry = it.entry();
        if !entry.all_conflict() {
            insertions.push((
                Arc::from(begin),
                Entry {
                    stack: if it.is_operation() {
                        entry.stack.clone()
                    } else {
                        OperationStack::default()
                    },
                    following_keys_cleared: entry.following_keys_cleared,
                    following_keys_conflict: true,
                    is_conflict: true,
                    following_keys_unreadable: entry.following_keys_unreadable,
                    is_unreadable: entry.is_unreadable,
                },
            ));
        }

        while it.end_key() < end_key {
            let _ = it.advance();
            let entry = it.entry();
            if it.key_at_begin() && !entry.all_conflict() {
                let mut e = entry.clone();
                e.following_keys_conflict = true;
                e.is_conflict = true;
                insertions.push((Arc::clone(&it.begin), e));
            }
        }

        debug_assert!(it.begin_key() != end_key);
        let entry = it.entry();
        if !entry.all_conflict() {
            let is_cleared = entry.following_keys_cleared;
            let is_unreadable = entry.is_unreadable;
            let following_unreadable = entry.following_keys_unreadable;
            let _ = it.advance();
            if !it.key_at_begin() || it.begin_key() != end_key {
                insertions.push((
                    Arc::from(end),
                    Entry {
                        stack: OperationStack::default(),
                        following_keys_cleared: is_cleared,
                        following_keys_conflict: false,
                        is_conflict: false,
                        following_keys_unreadable: following_unreadable,
                        is_unreadable,
                    },
                ));
            }
        }
        drop(it);

        if insertions.is_empty() {
            return;
        }
        let tree = self.tree_mut();
        for (key, entry) in insertions {
            tree.insert_mut(key, entry);
        }
    }

    /// `clearNoConflict`: [`clear`](Self::clear) without a conflict range: the cleared range
    /// gets an entry wherever the conflict ranges in it start or end.
    fn clear_no_conflict(&mut self, begin: &[u8], end: &[u8]) {
        let end_key = ExtKey::from(end);
        let mut it = self.iter_at(begin);

        let insert_begin = !it.is_cleared_range() || it.is_unreadable();
        let mut last_conflicted = it.is_conflict_range();
        let mut conflicted = last_conflicted;
        let mut conflict_ranges: Vec<Vec<u8>> = Vec::new();

        if insert_begin {
            conflict_ranges.push(begin.to_vec());
        } else {
            conflicted = !conflicted;
        }

        while it.end_key() < end_key {
            let _ = it.advance();
            if last_conflicted != it.is_conflict_range() {
                conflict_ranges.push(it.begin_key().to_vec());
                last_conflicted = it.is_conflict_range();
            }
        }

        if it.end_key() == end_key {
            let _ = it.advance();
        }

        debug_assert!(it.begin_key() <= end_key && end_key < it.end_key());

        let entry = it.entry();
        let insert_end = ((it.is_unmodified_range() || it.is_unreadable())
            && (!it.key_at_begin() || it.begin_key() != end_key))
            || (entry.is_conflict
                && !entry.following_keys_conflict
                && it.begin_key() == end_key
                && !it.key_at_begin());
        let end_cleared = it.is_cleared_range();
        let end_coalesce_clear = it.is_cleared_range()
            && it.begin_key() == end_key
            && it.is_conflict_range() == last_conflicted
            && !it.is_unreadable();
        let end_conflict = it.is_conflict_range();
        let end_unreadable = it.is_unreadable();
        drop(it);

        self.remove_range(
            ExtKey::new(begin, usize::from(!insert_begin)),
            ExtKey::new(end, usize::from(end_coalesce_clear)),
        );

        for key in conflict_ranges {
            self.tree_mut()
                .insert_mut(Arc::from(key), Entry::range(true, conflicted, false));
            conflicted = !conflicted;
        }

        debug_assert!(conflicted != last_conflicted);

        if insert_end {
            self.tree_mut().insert_mut(
                Arc::from(end),
                Entry::range(end_cleared, end_conflict, end_unreadable),
            );
        }
    }

    /// `coalesce`: the one operation that does what `existing` then `new` do.
    ///
    /// A `SetValue` or `SetVersionstampedValue` replaces `existing`; an atomic operation over
    /// a `SetValue` is the `SetValue` of its result, over the same operation it is that
    /// operation with the two operands combined; a `CompareAndClear` over a `SetValue` keeps
    /// or clears it. Anything else (and a `new` with no value) is `operation_failed` (1000):
    /// [`coalesce_over`](Self::coalesce_over) asks only for these.
    pub(crate) fn coalesce(existing: RywMutation, new: RywMutation) -> Result<RywMutation, Error> {
        let failed = || {
            Error::with_message(
                OPERATION_FAILED,
                format!(
                    "cannot coalesce mutation type {} over {}",
                    new.kind.0, existing.kind.0
                ),
            )
        };
        let Some(operand) = new.value.as_deref() else {
            return Err(failed());
        };
        match new.kind {
            MutationType::SET_VALUE | MutationType::SET_VERSIONSTAMPED_VALUE => Ok(new),
            MutationType::COMPARE_AND_CLEAR => {
                if existing.kind != MutationType::SET_VALUE {
                    return Err(failed());
                }
                if atomic::compare_and_clear(existing.value.as_deref(), operand).is_some() {
                    Ok(existing)
                } else {
                    Ok(RywMutation::set(None))
                }
            }
            MutationType::ADD_VALUE
            | MutationType::APPEND_IF_FITS
            | MutationType::AND
            | MutationType::OR
            | MutationType::XOR
            | MutationType::MAX
            | MutationType::MIN
            | MutationType::BYTE_MIN
            | MutationType::BYTE_MAX
            | MutationType::MIN_V2
            | MutationType::AND_V2 => {
                if existing.kind != MutationType::SET_VALUE && existing.kind != new.kind {
                    return Err(failed());
                }
                let value = atomic::apply(
                    new.kind,
                    existing.value.as_deref(),
                    operand,
                    VALUE_SIZE_LIMIT,
                )?;
                Ok(RywMutation {
                    value: value.map(Arc::from),
                    kind: existing.kind,
                })
            }
            _ => Err(failed()),
        }
    }

    /// `coalesceOver`: adds `new` on top of `stack`, folded into the top operation when the
    /// result is one operation: the same operation (unless it is a `CompareAndClear`, or a
    /// non-associative one whose operands' lengths differ), or anything over a non-atomic
    /// operation (a `SetValue`). Two different atomic operations stay apart. On an error the
    /// stack is unchanged.
    pub(crate) fn coalesce_over(stack: &mut OperationStack, new: RywMutation) -> Result<(), Error> {
        // C++ reads the default (`NoOp`) operation of an empty stack; the write map never
        // coalesces over one.
        let Some(existing) = stack.top().cloned() else {
            stack.push(new);
            return Ok(());
        };
        if existing.kind == new.kind && new.kind != MutationType::COMPARE_AND_CLEAR {
            let lengths_differ = match (&existing.value, &new.value) {
                (Some(e), Some(n)) => e.len() != n.len(),
                (Some(e), None) => !e.is_empty(),
                (None, _) => false,
            };
            if is_non_associative_op(existing.kind) && lengths_differ {
                stack.push(new);
            } else {
                stack.poppush(Self::coalesce(existing, new)?);
            }
        } else if is_atomic_op(new.kind) && is_atomic_op(existing.kind) {
            stack.push(new);
        } else {
            stack.poppush(Self::coalesce(existing, new)?);
        }
        Ok(())
    }

    /// `coalesceUnder`: the one operation `stack` comes to over a key whose value is `value`
    /// (`None`: the key is not set): a `SetValue` of the result (`None`: cleared), or the
    /// stack's only operation if it is independent.
    ///
    /// The read path uses it for an operation segment: over `None` if the segment is
    /// independent or the key is known not to be set, over the key's value otherwise.
    /// `operation_failed` (1000) for a stack that has a versionstamped operation under
    /// another one (an unreadable key, which reads do not get to).
    pub(crate) fn coalesce_under(
        stack: &OperationStack,
        value: Option<&[u8]>,
    ) -> Result<RywMutation, Error> {
        if !stack.is_dependent() && stack.len() == 1 {
            return Ok(stack.as_slice()[0].clone());
        }
        let mut current = RywMutation::set(value.map(Arc::from));
        for op in stack {
            current = Self::coalesce(current, op.clone())?;
        }
        Ok(current)
    }
}

impl fmt::Debug for WriteMap {
    /// The segments (`WriteMap::dump`).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut list = f.debug_list();
        let mut it = self.iter();
        while it.begin_key() < ExtKey::from(ALL_KEYS_END) {
            list.entry(&it);
            let _ = it.advance();
        }
        list.finish()
    }
}

/// `WriteMap::iterator::SEGMENT_TYPE`: what a segment holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum WriteSegmentType {
    /// `UNMODIFIED_RANGE`: keys the transaction has not written.
    UnmodifiedRange,
    /// `CLEARED_RANGE`: keys the transaction cleared (and has not written since).
    ClearedRange,
    /// `INDEPENDENT_WRITE`: a key whose operations give a value whatever its value in the
    /// database.
    IndependentWrite,
    /// `DEPENDENT_WRITE`: a key whose operations need its value in the database.
    DependentWrite,
}

/// `WriteMap::iterator`: a position among the segments of a snapshot of a [`WriteMap`].
///
/// It is always at a segment: [`WriteMap::iter`] starts at the first, `["", ...)`. The
/// segments of the key space end at `\xff\xff`; one more, `[\xff\xff, \xff\xff\x00)`, is the
/// last (loops stop when [`begin_key`](Self::begin_key) reaches `\xff\xff`), and
/// [`advance`](Self::advance) from it reaches the terminal position at `\xff\xff\x00`, as
/// C++'s `++` reaches the `afterAllKeys` entry. It holds a
/// snapshot: later writes to the map do not change what it sees. The method names follow
/// the snapshot cache's iterator ([`SnapshotCacheIter`](crate::ryw::SnapshotCacheIter)),
/// which the read path walks together with this one.
///
/// The iterator keeps copies of its segment's entry and of the one after it (an entry is its
/// flags and a reference counted operation stack), made when it moves, so the accessors read
/// them without looking in the map; a move is one lookup in the snapshot.
#[derive(Clone)]
pub(crate) struct WriteMapIter {
    /// The snapshot.
    tree: Tree,
    /// The key of the segment's entry (`entry()`).
    begin: Arc<[u8]>,
    /// The key of the entry after it (`nextEntry()`).
    end: Arc<[u8]>,
    /// The entry at `begin`.
    cur: Entry,
    /// The entry at `end`.
    next: Entry,
    /// `false`: the segment is the entry's operation; `true`: the range after it.
    offset: bool,
}

impl WriteMapIter {
    /// An iterator over `tree` at the segment of `key` ([`skip`](Self::skip)).
    fn at(tree: Tree, key: &[u8]) -> WriteMapIter {
        let (begin, cur, end, next, offset) = Self::locate(&tree, key);
        WriteMapIter {
            tree,
            begin,
            end,
            cur,
            next,
            offset,
        }
    }

    /// The segment of `key` in `tree`: its entry's key and entry, the next entry's, and
    /// whether it is the range after the entry (see `offset`). A key past `\xff\xff` is taken
    /// as `\xff\xff`.
    fn locate(tree: &Tree, key: &[u8]) -> (Arc<[u8]>, Entry, Arc<[u8]>, Entry, bool) {
        let key = if key > ALL_KEYS_END {
            ALL_KEYS_END
        } else {
            key
        };
        // `upper_bound`: `\xff\xff\x00` is after any key up to `\xff\xff`, and `""` is at or
        // before any key.
        let (end, next) = Self::entry_after(tree, key)
            .expect("the last sentinel is after every key up to \\xff\\xff");
        let (begin, cur) =
            Self::entry_before(tree, &end).expect("the first sentinel is before every other entry");
        let offset = cur.stack.is_empty() || *begin != *key;
        (begin, cur, end, next, offset)
    }

    /// `entry()`: the segment's entry.
    fn entry(&self) -> &Entry {
        &self.cur
    }

    /// The first entry after `key` in `tree`, if there is one: its key and a copy of it.
    fn entry_after(tree: &Tree, key: &[u8]) -> Option<(Arc<[u8]>, Entry)> {
        tree.range::<[u8], _>((Bound::Excluded(key), Bound::Unbounded))
            .next()
            .map(|(k, e)| (Arc::clone(k), e.clone()))
    }

    /// The last entry before `key` in `tree`, if there is one: its key and a copy of it.
    fn entry_before(tree: &Tree, key: &[u8]) -> Option<(Arc<[u8]>, Entry)> {
        tree.range::<[u8], _>((Bound::Unbounded, Bound::Excluded(key)))
            .next_back()
            .map(|(k, e)| (Arc::clone(k), e.clone()))
    }

    /// `keyAtBegin`: the segment starts at its entry's key (it is an operation, or the
    /// range after an entry with no operation).
    fn key_at_begin(&self) -> bool {
        !self.offset || self.entry().stack.is_empty()
    }

    /// `type`: what the segment holds.
    pub(crate) fn segment_type(&self) -> WriteSegmentType {
        let entry = self.entry();
        if self.offset {
            if entry.following_keys_cleared {
                WriteSegmentType::ClearedRange
            } else {
                WriteSegmentType::UnmodifiedRange
            }
        } else if entry.stack.is_dependent() {
            WriteSegmentType::DependentWrite
        } else {
            WriteSegmentType::IndependentWrite
        }
    }

    /// `is_cleared_range`: the segment is a range of keys the transaction cleared.
    pub(crate) fn is_cleared_range(&self) -> bool {
        self.offset && self.entry().following_keys_cleared
    }

    /// `is_unmodified_range`: the segment is a range of keys the transaction did not write.
    pub(crate) fn is_unmodified_range(&self) -> bool {
        self.offset && !self.entry().following_keys_cleared
    }

    /// `is_operation`: the segment is one key's operations ([`op`](Self::op)).
    pub(crate) fn is_operation(&self) -> bool {
        !self.offset
    }

    /// `is_conflict_range`: the segment's keys are in the write conflict ranges.
    pub(crate) fn is_conflict_range(&self) -> bool {
        let entry = self.entry();
        if self.offset {
            entry.following_keys_conflict
        } else {
            entry.is_conflict
        }
    }

    /// `is_unreadable`: the segment's keys are unreadable until the commit.
    pub(crate) fn is_unreadable(&self) -> bool {
        let entry = self.entry();
        if self.offset {
            entry.following_keys_unreadable
        } else {
            entry.is_unreadable
        }
    }

    /// `is_independent`: an operation's value does not depend on the key's value in the
    /// database (its stack is independent, or the key was cleared before it). `None` if the
    /// segment is not an operation (C++ asserts).
    pub(crate) fn is_independent(&self) -> Option<bool> {
        if self.offset {
            return None;
        }
        let entry = self.entry();
        Some(entry.following_keys_cleared || !entry.stack.is_dependent())
    }

    /// `beginKey`: the segment's first key (an operation's key, or the key after it for the
    /// range after an operation).
    pub(crate) fn begin_key(&self) -> ExtKey<'_> {
        let extra = self.offset && !self.entry().stack.is_empty();
        ExtKey::new(&self.begin, usize::from(extra))
    }

    /// `endKey`: the key after the segment's last key.
    pub(crate) fn end_key(&self) -> ExtKey<'_> {
        if self.offset {
            ExtKey::new(&self.end, 0)
        } else {
            ExtKey::new(&self.begin, 1)
        }
    }

    /// `op`: the operations of an operation segment; `None` for a range (C++ asserts).
    pub(crate) fn op(&self) -> Option<&OperationStack> {
        if self.offset {
            None
        } else {
            Some(&self.entry().stack)
        }
    }

    /// `operator++`: moves to the next segment. From the last one, `[\xff\xff, \xff\xff\x00)`,
    /// it moves (as C++ does) onto the `\xff\xff\x00` sentinel entry, a terminal position whose
    /// [`begin_key`](Self::begin_key) is `\xff\xff\x00` (what ends a loop over a range up to
    /// `keyAfter(\xff\xff)`), and returns `false`; from there it stays put, returning `false`.
    #[must_use]
    pub(crate) fn advance(&mut self) -> bool {
        if !self.offset && !equals_key_after(&self.begin, &self.end) {
            self.offset = true;
            return true;
        }
        let Some((key, entry)) = Self::entry_after(&self.tree, &self.end) else {
            if self.begin != self.end {
                // The sentinel: `end` is `\xff\xff\x00`, which has no entry after it.
                self.begin = Arc::clone(&self.end);
                self.cur = self.next.clone();
                self.offset = true;
            }
            return false;
        };
        self.begin = std::mem::replace(&mut self.end, key);
        self.cur = std::mem::replace(&mut self.next, entry);
        self.offset = self.cur.stack.is_empty();
        true
    }

    /// `operator--`: moves to the previous segment. Returns `false`, without moving, at the
    /// first one.
    #[must_use]
    pub(crate) fn retreat(&mut self) -> bool {
        if self.offset && !self.cur.stack.is_empty() {
            self.offset = false;
            return true;
        }
        let Some((key, entry)) = Self::entry_before(&self.tree, &self.begin) else {
            return false;
        };
        self.end = std::mem::replace(&mut self.begin, key);
        self.next = std::mem::replace(&mut self.cur, entry);
        self.offset = self.cur.stack.is_empty() || !equals_key_after(&self.begin, &self.end);
        true
    }

    /// `skip`: moves to the segment that contains `key` (`begin_key() <= key < end_key()`).
    /// A key past `\xff\xff` is taken as `\xff\xff`.
    pub(crate) fn skip(&mut self, key: &[u8]) {
        (self.begin, self.cur, self.end, self.next, self.offset) = Self::locate(&self.tree, key);
    }
}

impl PartialEq for WriteMapIter {
    /// `operator==`: the same segment (of iterators over the same snapshot).
    fn eq(&self, other: &WriteMapIter) -> bool {
        self.offset == other.offset && self.begin == other.begin
    }
}

impl Eq for WriteMapIter {}

impl fmt::Debug for WriteMapIter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("Segment");
        s.field("begin", &self.begin_key().to_vec())
            .field("end", &self.end_key().to_vec())
            .field("type", &self.segment_type())
            .field("conflict", &self.is_conflict_range())
            .field("unreadable", &self.is_unreadable());
        if let Some(op) = self.op() {
            s.field("op", op);
        }
        s.finish()
    }
}

#[cfg(test)]
mod tests;
