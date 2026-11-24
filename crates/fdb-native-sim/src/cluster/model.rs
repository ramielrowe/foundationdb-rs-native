//! The cluster's data: a version clock, a multi-version key-value map, a resolver and the
//! log of every commit applied. It is the ground truth the invariants check the client's
//! observations against.
//!
//! * **Versions** follow a clock, as FDB's master hands them out: a commit gets
//!   `max(last commit + 1, clock)`, where the clock is a microsecond count of virtual time
//!   (a recovery can make it jump, as FDB's does by `MAX_VERSIONS_IN_FLIGHT`). A read
//!   version is `max(last commit, clock - 1)`, so no commit can ever get a version a read
//!   version was already given out for, and every commit acknowledged before a read version
//!   was asked for is at or below it.
//! * **Storage lag**: each version is noted with the time it was given out; a storage
//!   server `lag` behind serves the versions noted at least `lag` ago.
//! * **MVCC window**: versions more than 5 000 000 (5 s) below the clock are too old to read
//!   or to resolve against (`MAX_READ_TRANSACTION_LIFE_VERSIONS`).
//! * **Resolver**: a commit with read conflict ranges whose snapshot is too old is
//!   `transaction_too_old`; one whose read conflict ranges intersect the write conflict
//!   ranges of a commit at a version above its snapshot conflicts (`not_committed`).

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use fdb_native_schema::domain::{Commit, KeyRange, Mutation, MutationType};
use fdb_native_transport::runtime::Instant;

use crate::START;

/// `MAX_READ_TRANSACTION_LIFE_VERSIONS`.
pub const MVCC_WINDOW: i64 = 5_000_000;

/// One commit the cluster applied.
#[derive(Clone, Debug)]
pub struct LogEntry {
    /// Its commit version.
    pub version: i64,
    /// Its read snapshot.
    pub read_snapshot: i64,
    /// The recovery generation of the proxy that applied it.
    pub generation: u64,
    /// When it was applied (virtual time).
    pub applied_at: Instant,
    /// Its mutations, as the client sent them.
    pub mutations: Vec<Mutation>,
}

/// What the resolver says about a commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Resolution {
    /// Its snapshot is older than the MVCC window.
    TooOld,
    /// It read something written after its snapshot: the indices of the read conflict
    /// ranges that conflict.
    Conflict(Vec<i32>),
    /// It may commit.
    Commit,
}

/// Why a commit could not be applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Unsupported(pub(crate) MutationType);

pub(crate) struct Model {
    /// The clock's version at [`START`].
    base: i64,
    last_commit: i64,
    /// `(when, version)`: each version given out (read or commit), increasing.
    noted: Vec<(Instant, i64)>,
    kv: BTreeMap<Vec<u8>, BTreeMap<i64, Option<Vec<u8>>>>,
    /// `(commit version, write conflict ranges)`, in version order.
    history: VecDeque<(i64, Vec<KeyRange>)>,
    pub(crate) log: Vec<LogEntry>,
}

fn intersects(a: &KeyRange, b: &KeyRange) -> bool {
    a.begin < b.end && b.begin < a.end
}

/// FDB's `doLittleEndianAdd`: the operand's length, carried over the existing value.
pub(crate) fn little_endian_add(existing: Option<&[u8]>, operand: &[u8]) -> Vec<u8> {
    let Some(existing) = existing else {
        return operand.to_vec();
    };
    let mut out = Vec::with_capacity(operand.len());
    let mut carry = 0u16;
    for (i, o) in operand.iter().enumerate() {
        let e = existing.get(i).copied().unwrap_or(0);
        let sum = u16::from(e) + u16::from(*o) + carry;
        out.push(sum as u8);
        carry = sum >> 8;
    }
    out
}

impl Model {
    pub(crate) fn new() -> Model {
        Model {
            base: 1_000_000,
            last_commit: 0,
            noted: Vec::new(),
            kv: BTreeMap::new(),
            history: VecDeque::new(),
            log: Vec::new(),
        }
    }

    /// The version clock now.
    pub(crate) fn clock(&self, now: Instant) -> i64 {
        self.base + (now - START).as_micros() as i64
    }

    /// The oldest version that can still be read or resolved against.
    pub(crate) fn oldest(&self, now: Instant) -> i64 {
        self.clock(now) - MVCC_WINDOW
    }

    fn note(&mut self, now: Instant, version: i64) {
        if self.noted.last().is_none_or(|(_, v)| *v < version) {
            self.noted.push((now, version));
        }
    }

    /// A read version (`getConsistentReadVersion`).
    pub(crate) fn read_version(&mut self, now: Instant) -> i64 {
        let v = self.last_commit.max(self.clock(now) - 1);
        self.note(now, v);
        v
    }

    /// The newest version a storage server `lag` behind has.
    pub(crate) fn storage_version(&self, now: Instant, lag: Duration) -> i64 {
        let horizon = Instant::from_origin(now.since_origin().saturating_sub(lag));
        let i = self.noted.partition_point(|(t, _)| *t <= horizon);
        if i == 0 { 0 } else { self.noted[i - 1].1 }
    }

    /// The largest version given out so far.
    pub(crate) fn max_version(&self) -> i64 {
        self.noted.last().map_or(0, |(_, v)| *v)
    }

    /// Makes the version clock jump by `by` (a recovery).
    pub(crate) fn jump(&mut self, by: i64) {
        self.base += by;
    }

    pub(crate) fn resolve(&self, now: Instant, commit: &Commit) -> Resolution {
        if commit.read_conflict_ranges.is_empty() {
            return Resolution::Commit;
        }
        if commit.read_snapshot < self.oldest(now) {
            return Resolution::TooOld;
        }
        let mut conflicts = Vec::new();
        for (i, read) in commit.read_conflict_ranges.iter().enumerate() {
            let hit = self
                .history
                .iter()
                .rev()
                .take_while(|(v, _)| *v > commit.read_snapshot)
                .any(|(_, writes)| writes.iter().any(|w| intersects(w, read)));
            if hit {
                conflicts.push(i as i32);
            }
        }
        if conflicts.is_empty() {
            Resolution::Commit
        } else {
            Resolution::Conflict(conflicts)
        }
    }

    /// Applies `commit` (which the resolver let through) at a new version.
    pub(crate) fn apply(
        &mut self,
        now: Instant,
        commit: &Commit,
        generation: u64,
    ) -> Result<i64, Unsupported> {
        for m in &commit.mutations {
            if ![
                MutationType::SET_VALUE,
                MutationType::CLEAR_RANGE,
                MutationType::ADD_VALUE,
            ]
            .contains(&m.kind)
            {
                return Err(Unsupported(m.kind));
            }
        }
        let version = (self.last_commit + 1).max(self.clock(now));
        for m in &commit.mutations {
            match m.kind {
                MutationType::SET_VALUE => {
                    self.kv
                        .entry(m.param1.clone())
                        .or_default()
                        .insert(version, Some(m.param2.clone()));
                }
                MutationType::CLEAR_RANGE => {
                    let keys: Vec<Vec<u8>> = self
                        .kv
                        .range(m.param1.clone()..m.param2.clone())
                        .map(|(k, _)| k.clone())
                        .collect();
                    for k in keys {
                        self.kv.get_mut(&k).expect("present").insert(version, None);
                    }
                }
                _ => {
                    let old = self.get(&m.param1, version);
                    let new = little_endian_add(old.as_deref(), &m.param2);
                    self.kv
                        .entry(m.param1.clone())
                        .or_default()
                        .insert(version, Some(new));
                }
            }
        }
        self.last_commit = version;
        self.note(now, version);
        self.history
            .push_back((version, commit.write_conflict_ranges.clone()));
        let oldest = self.oldest(now);
        while self.history.front().is_some_and(|(v, _)| *v < oldest - 1) {
            self.history.pop_front();
        }
        self.log.push(LogEntry {
            version,
            read_snapshot: commit.read_snapshot,
            generation,
            applied_at: now,
            mutations: commit.mutations.clone(),
        });
        Ok(version)
    }

    /// The value of `key` at `version`.
    pub(crate) fn get(&self, key: &[u8], version: i64) -> Option<Vec<u8>> {
        self.kv
            .get(key)
            .and_then(|h| h.range(..=version).next_back())
            .and_then(|(_, v)| v.clone())
    }

    /// The keys present at `version` in `[begin, end)`, in order.
    pub(crate) fn keys(&self, begin: &[u8], end: &[u8], version: i64) -> Vec<Vec<u8>> {
        if begin >= end {
            return Vec::new();
        }
        self.kv
            .range(begin.to_vec()..end.to_vec())
            .filter(|(_, h)| {
                h.range(..=version)
                    .next_back()
                    .is_some_and(|(_, v)| v.is_some())
            })
            .map(|(k, _)| k.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(b: &[u8], e: &[u8]) -> KeyRange {
        KeyRange {
            begin: b.to_vec(),
            end: e.to_vec(),
        }
    }

    fn commit(snapshot: i64, reads: Vec<KeyRange>, writes: Vec<KeyRange>) -> Commit {
        Commit {
            read_conflict_ranges: reads,
            write_conflict_ranges: writes.clone(),
            mutations: writes
                .iter()
                .map(|w| {
                    Mutation::new(MutationType::SET_VALUE, w.begin.clone(), b"v".to_vec()).unwrap()
                })
                .collect(),
            read_snapshot: snapshot,
            report_conflicting_keys: false,
            lock_aware: false,
            transaction_span_context: None,
            flags: 0,
            debug_id: None,
            commit_cost_estimation: None,
            tag_set: None,
            span_context: Default::default(),
            idempotency_id: None,
        }
    }

    #[test]
    fn little_endian_add_carries_and_takes_the_operand_length() {
        assert_eq!(little_endian_add(None, &[1, 2]), vec![1, 2]);
        assert_eq!(little_endian_add(Some(&[0xff, 0]), &[1, 0]), vec![0, 1]);
        assert_eq!(
            little_endian_add(Some(&[0xff, 0xff, 7]), &[1, 0]),
            vec![0, 0]
        );
        assert_eq!(little_endian_add(Some(&[5]), &[1, 0, 0]), vec![6, 0, 0]);
    }

    #[test]
    fn resolver_and_versions() {
        let mut m = Model::new();
        let t0 = START + Duration::from_secs(1);
        let rv = m.read_version(t0);
        let v1 = m
            .apply(
                t0,
                &commit(rv, vec![range(b"a", b"b")], vec![range(b"a", b"a\0")]),
                1,
            )
            .unwrap();
        assert!(v1 > rv);
        assert_eq!(m.get(b"a", v1), Some(b"v".to_vec()));
        assert_eq!(m.get(b"a", rv), None);
        // A transaction that read `a` at `rv` conflicts; one at `v1` does not.
        assert_eq!(
            m.resolve(t0, &commit(rv, vec![range(b"a", b"c")], vec![])),
            Resolution::Conflict(vec![0])
        );
        assert_eq!(
            m.resolve(t0, &commit(v1, vec![range(b"a", b"c")], vec![])),
            Resolution::Commit
        );
        // Too old once the clock moved 5 s past the snapshot.
        let later = t0 + Duration::from_secs(6);
        assert_eq!(
            m.resolve(later, &commit(v1, vec![range(b"x", b"y")], vec![])),
            Resolution::TooOld
        );
        // Read versions never go back and cover every commit.
        let rv2 = m.read_version(t0);
        assert!(rv2 >= v1);
        assert_eq!(m.keys(b"", b"\xff", v1), vec![b"a".to_vec()]);
        // A storage server 1 s behind does not have what was noted just now.
        assert!(m.storage_version(t0, Duration::from_secs(1)) < v1);
        assert_eq!(m.storage_version(t0, Duration::ZERO), rv2);
    }
}
