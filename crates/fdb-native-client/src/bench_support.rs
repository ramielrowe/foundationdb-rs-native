//! A facade over the read-your-writes data structures for `benches/ryw.rs`, behind the
//! `bench-internals` feature. Not part of the public API: it exists because a bench target
//! cannot see the crate's `pub(crate)` items.

use fdb_native_schema::domain::MutationType;

use crate::ryw;

/// `ryw::WriteMap`.
#[derive(Clone, Debug, Default)]
pub struct WriteMap {
    inner: ryw::WriteMap,
}

impl WriteMap {
    /// An empty write map.
    pub fn new() -> WriteMap {
        WriteMap::default()
    }

    /// `set(key, value)`, with a write conflict range.
    pub fn set(&mut self, key: &[u8], value: &[u8]) {
        self.inner
            .mutate(key, MutationType::SET_VALUE, value, true)
            .expect("legal key");
    }

    /// An atomic `add` of `operand` on `key`, with a write conflict range.
    pub fn add(&mut self, key: &[u8], operand: &[u8]) {
        self.inner
            .mutate(key, MutationType::ADD_VALUE, operand, true)
            .expect("legal key");
    }

    /// `clear(begin, end)`, with a write conflict range.
    pub fn clear(&mut self, begin: &[u8], end: &[u8]) {
        self.inner.clear(begin, end, true);
    }

    /// An iterator over a snapshot of the map, at the segment of `key`: the state a read that
    /// is waiting for a storage server keeps.
    pub fn cursor_at(&self, key: &[u8]) -> Cursor {
        Cursor {
            inner: self.inner.iter_at(key),
        }
    }

    /// The number of segments (a walk of the whole map).
    pub fn segments(&self) -> usize {
        let mut it = self.inner.iter();
        let mut n = 0;
        while it.begin_key() < ryw::ExtKey::from(&b"\xff\xff"[..]) {
            n += 1;
            if !it.advance() {
                break;
            }
        }
        n
    }
}

/// `ryw::WriteMapIter`.
#[derive(Clone, Debug)]
pub struct Cursor {
    inner: ryw::WriteMapIter,
}

impl Cursor {
    /// `++`.
    pub fn advance(&mut self) -> bool {
        self.inner.advance()
    }

    /// `--`.
    pub fn retreat(&mut self) -> bool {
        self.inner.retreat()
    }

    /// `skip`.
    pub fn skip(&mut self, key: &[u8]) {
        self.inner.skip(key);
    }

    /// What the segment says, folded into a number (so the work of reading it is not optimized
    /// away): the lengths of its bounds, its flags and the length of its operation stack.
    pub fn digest(&self) -> usize {
        let it = &self.inner;
        it.begin_key().size()
            + it.end_key().size()
            + usize::from(it.is_conflict_range())
            + usize::from(it.is_unreadable())
            + usize::from(it.is_cleared_range())
            + usize::from(it.is_operation())
            + it.op().map_or(0, |stack| stack.len())
    }
}
