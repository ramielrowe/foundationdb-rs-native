//! The read-your-writes layer (`fdbclient/ReadYourWrites.actor.cpp` and its data structures).

mod snapshot_cache;

pub(crate) use snapshot_cache::{
    ExtKey, RangeReadOutcome, SegmentType, SelectorBound, SnapshotCache, SnapshotCacheIter,
    known_key_range, known_key_range_back,
};
