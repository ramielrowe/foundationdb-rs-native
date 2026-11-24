//! The read-your-writes layer (`fdbclient/ReadYourWrites.actor.cpp` and its data structures).

pub(crate) mod commit;
mod conflicts;
mod iterator;
pub(crate) mod read;
// A few helpers of the two data structures are used only by their tests (and the
// structures' internals are being reworked for performance, ygr.6).
#[allow(dead_code, reason = "helpers used by the tests only")]
mod snapshot_cache;
#[allow(dead_code, reason = "helpers used by the tests only")]
mod write_map;

pub(crate) use conflicts::ReadConflicts;
pub(crate) use snapshot_cache::{
    ExtKey, RangeReadOutcome, SegmentType, SelectorBound, SnapshotCache, SnapshotCacheIter,
};
pub(crate) use write_map::{WriteMap, WriteMapIter, WriteSegmentType};
