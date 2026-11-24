//! The simulation trace: every decision the simulator makes (which task runs, when time
//! moves, what the network carries, which faults strike, what the servers apply) is folded
//! into a running hash, so that two runs of a seed can be compared with one number. With
//! [`TraceLevel::Events`] or above the events are also printed to stderr as they happen.

use std::fmt::Write as _;

use fdb_native_transport::runtime::Instant;

/// How much of the trace is printed (it is always hashed).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum TraceLevel {
    /// Nothing is printed.
    #[default]
    Quiet,
    /// Faults, connections, server-side decisions and workload outcomes.
    Events,
    /// Also every task poll, timer and network segment.
    Verbose,
}

impl TraceLevel {
    /// The level the `FDB_SIM_TRACE` environment variable asks for: unset or `0` quiet, `1`
    /// events, `2` (or anything else) verbose.
    pub fn from_env() -> TraceLevel {
        match std::env::var("FDB_SIM_TRACE").ok().as_deref() {
            None | Some("") | Some("0") => TraceLevel::Quiet,
            Some("1") => TraceLevel::Events,
            Some(_) => TraceLevel::Verbose,
        }
    }
}

/// FNV-1a, 64 bits: stable across runs, platforms and Rust releases (unlike `DefaultHasher`).
#[derive(Clone, Copy, Debug)]
pub(crate) struct Fnv(u64);

impl Fnv {
    pub(crate) const fn new() -> Fnv {
        Fnv(0xcbf2_9ce4_8422_2325)
    }

    pub(crate) fn bytes(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 ^= u64::from(*b);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    pub(crate) fn u64(&mut self, v: u64) {
        self.bytes(&v.to_le_bytes());
    }

    pub(crate) fn finish(self) -> u64 {
        self.0
    }
}

/// The hash of `bytes` (FNV-1a).
pub fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut h = Fnv::new();
    h.bytes(bytes);
    h.finish()
}

/// The running trace of one simulation.
#[derive(Debug)]
pub(crate) struct Trace {
    hash: Fnv,
    events: u64,
    level: TraceLevel,
    seed: u64,
}

impl Trace {
    pub(crate) fn new(seed: u64, level: TraceLevel) -> Trace {
        let mut hash = Fnv::new();
        hash.u64(seed);
        Trace {
            hash,
            events: 0,
            level,
            seed,
        }
    }

    pub(crate) fn level(&self) -> TraceLevel {
        self.level
    }

    /// Records an event: `kind` and `args` go into the hash; `detail` is only formatted when
    /// the event is printed (at `level` or above).
    pub(crate) fn record(
        &mut self,
        now: Instant,
        level: TraceLevel,
        kind: &str,
        args: &[u64],
        detail: impl FnOnce() -> String,
    ) {
        self.events += 1;
        self.hash.bytes(kind.as_bytes());
        for a in args {
            self.hash.u64(*a);
        }
        if self.level >= level {
            let t = now.since_origin();
            let mut line = String::new();
            let _ = write!(
                line,
                "[sim {:>6}] {:>4}.{:06} {kind}",
                self.seed,
                t.as_secs(),
                t.subsec_micros()
            );
            let detail = detail();
            if !detail.is_empty() {
                line.push(' ');
                line.push_str(&detail);
            }
            eprintln!("{line}");
        }
    }

    pub(crate) fn hash(&self) -> u64 {
        self.hash.finish()
    }

    pub(crate) fn events(&self) -> u64 {
        self.events
    }
}
