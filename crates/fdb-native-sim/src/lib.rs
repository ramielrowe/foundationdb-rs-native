//! Deterministic simulation of the native FoundationDB client (dev-only, not published).
//!
//! The real client ([`fdb_native_client::Database`]) runs unchanged on a simulated
//! [`Runtime`](fdb_native_transport::runtime::Runtime), against a model cluster, through a
//! network that injects faults; every decision is drawn from one seed, so a failure found by
//! a seed sweep replays exactly.
//!
//! * [`Sim`]: a single-threaded executor that picks which ready task runs next with a
//!   seeded generator, and a virtual clock that only moves when nothing is ready (see its
//!   documentation).
//! * [`SimRuntime`]: the transport's runtime over a `Sim`, for one simulated machine
//!   ([`NodeId`]); its const parameters are the stream's write promises.
//! * [`net`]: the in-memory network, its latencies and its faults (partitions, clogging,
//!   resets mid-frame, half-open connections, crashes, partial writes, ...).
//! * [`cluster`]: a model cluster that speaks the real wire protocol (coordinators, GRV and
//!   commit proxies with a resolver, storage servers over a versioned key-value map) and
//!   keeps a log of every commit it applied, so that what the client reports can be checked
//!   against what happened.
//! * [`workload`] and [`harness`]: workloads (at-most-once commits, a bank, reads checked
//!   against the log), invariants, a chaos schedule, and [`harness::run_seed`].
//!
//! # Determinism
//!
//! A seed fixes the scheduler's choices, the network's latencies and faults, the cluster's
//! decisions, the chaos schedule and the client's own seeds (`DatabaseOptions::*.seed`).
//! [`Sim::trace_hash`] folds every decision into one number; the sweep test asserts that a
//! seed run twice gives the same hash. What the hash leaves out on purpose: the bytes of the
//! client's ConnectPacket (its connection id is drawn once per process,
//! `process_transport_id`), which only reach the servers' handshake code.
//!
//! # Replaying a seed
//!
//! ```text
//! FDB_SIM_SEED=1234 FDB_SIM_TRACE=1 cargo nextest run -p fdb-native-sim --no-capture -E 'test(replay_seed)'
//! ```
//!
//! `FDB_SIM_TRACE=2` also prints every poll, timer and segment.

pub mod cluster;
mod executor;
pub mod harness;
pub mod net;
mod rng;
mod runtime;
mod trace;
pub mod workload;

pub use executor::{NodeId, START, Sim, SimError, SimSleep, SimTask, TaskInfo};
pub use net::{ConnectionInfo, KillMode, NetConfig, NetStats, SimListener, SimStream};
pub use rng::SimRng;
pub use runtime::SimRuntime;
pub use trace::{TraceLevel, hash_bytes};
