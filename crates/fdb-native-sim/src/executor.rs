//! The deterministic executor (see [`Sim`]).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::net::IpAddr;
use std::panic::{AssertUnwindSafe, Location, catch_unwind};
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use fdb_native_transport::runtime::{Instant, JoinError, TaskHandle};

use crate::net::NetState;
use crate::rng::SimRng;
use crate::trace::{Trace, TraceLevel};

/// Where the virtual clock starts: away from [`Instant::ZERO`], which code may use as "never".
pub const START: Instant = Instant::from_origin(Duration::from_secs(1000));

/// The id of the future [`Sim::block_on`] drives (spawned tasks start at 1).
const MAIN: u64 = 0;

/// How many polls may run without the clock moving before the loop calls it a livelock.
const LIVELOCK_STEPS: u64 = 20_000_000;

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A simulated machine. Tasks, connections and listening addresses belong to one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u32);

#[derive(Debug)]
pub(crate) struct NodeInfo {
    pub(crate) name: String,
    pub(crate) ip: IpAddr,
    pub(crate) next_port: u16,
}

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

struct TaskEntry {
    node: NodeId,
    future: Option<BoxFuture>,
    waker: Waker,
    aborted: bool,
    location: &'static Location<'static>,
}

#[derive(Default)]
struct Tasks {
    map: BTreeMap<u64, TaskEntry>,
    next_id: u64,
}

#[derive(Default)]
struct Timers {
    map: BTreeMap<(Instant, u64), Waker>,
    next_id: u64,
}

/// What a timer registration is keyed by.
pub(crate) type TimerKey = (Instant, u64);

pub(crate) struct Shared {
    clock: AtomicU64,
    tasks: Mutex<Tasks>,
    ready: Mutex<BTreeSet<u64>>,
    timers: Mutex<Timers>,
    sched: Mutex<SimRng>,
    master: Mutex<SimRng>,
    pub(crate) trace: Mutex<Trace>,
    pub(crate) net: Mutex<NetState>,
    pub(crate) nodes: Mutex<Vec<NodeInfo>>,
    panics: Mutex<Vec<String>>,
    running: AtomicBool,
    seed: u64,
}

impl Shared {
    pub(crate) fn now(&self) -> Instant {
        Instant::from_origin(Duration::from_nanos(self.clock.load(Ordering::Acquire)))
    }

    /// Records a trace event (see [`Trace::record`]).
    pub(crate) fn trace(
        &self,
        level: TraceLevel,
        kind: &str,
        args: &[u64],
        detail: impl FnOnce() -> String,
    ) {
        let now = self.now();
        lock(&self.trace).record(now, level, kind, args, detail);
    }

    pub(crate) fn trace_level(&self) -> TraceLevel {
        lock(&self.trace).level()
    }

    /// Wakes `waker` once the clock reaches `at`.
    pub(crate) fn add_timer(&self, at: Instant, waker: Waker) -> TimerKey {
        let mut timers = lock(&self.timers);
        timers.next_id += 1;
        let key = (at, timers.next_id);
        timers.map.insert(key, waker);
        key
    }

    pub(crate) fn cancel_timer(&self, key: TimerKey) {
        lock(&self.timers).map.remove(&key);
    }

    /// Registers (or refreshes) the timer `key` for `at` with `waker`.
    fn refresh_timer(&self, key: &mut Option<TimerKey>, at: Instant, waker: &Waker) {
        let mut timers = lock(&self.timers);
        if let Some(k) = *key {
            if let Some(w) = timers.map.get_mut(&k) {
                if !w.will_wake(waker) {
                    *w = waker.clone();
                }
                return;
            }
        }
        timers.next_id += 1;
        let k = (at, timers.next_id);
        timers.map.insert(k, waker.clone());
        *key = Some(k);
    }

    fn wake_id(&self, id: u64) {
        lock(&self.ready).insert(id);
    }

    pub(crate) fn node_name(&self, node: NodeId) -> String {
        lock(&self.nodes)
            .get(node.0 as usize)
            .map_or_else(|| format!("node{}", node.0), |n| n.name.clone())
    }

    pub(crate) fn node_ip(&self, node: NodeId) -> IpAddr {
        lock(&self.nodes)[node.0 as usize].ip
    }

    pub(crate) fn ephemeral_port(&self, node: NodeId) -> u16 {
        let mut nodes = lock(&self.nodes);
        let info = &mut nodes[node.0 as usize];
        info.next_port = info.next_port.checked_add(1).unwrap_or(10_000);
        info.next_port
    }

    fn spawn_boxed(
        self: &Arc<Self>,
        node: NodeId,
        future: BoxFuture,
        location: &'static Location<'static>,
    ) -> u64 {
        let id = {
            let mut tasks = lock(&self.tasks);
            tasks.next_id += 1;
            let id = tasks.next_id;
            let waker = Waker::from(Arc::new(TaskWaker {
                id,
                shared: Arc::downgrade(self),
            }));
            tasks.map.insert(
                id,
                TaskEntry {
                    node,
                    future: Some(future),
                    waker,
                    aborted: false,
                    location,
                },
            );
            id
        };
        self.wake_id(id);
        self.trace(
            TraceLevel::Verbose,
            "spawn",
            &[id, u64::from(node.0)],
            || format!("task {id} on {} at {location}", self.node_name(node)),
        );
        id
    }

    fn abort_task(&self, id: u64) {
        if let Some(entry) = lock(&self.tasks).map.get_mut(&id) {
            entry.aborted = true;
        } else {
            return;
        }
        self.wake_id(id);
    }
}

struct TaskWaker {
    id: u64,
    shared: Weak<Shared>,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(shared) = self.shared.upgrade() {
            shared.wake_id(self.id);
        }
    }
}

/// Why [`Sim::block_on`] gave up.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SimError {
    /// Nothing can run and no timer is set, but the future is not done.
    Deadlock {
        /// The virtual time it happened at.
        now: Duration,
        /// The tasks still alive.
        live: Vec<String>,
    },
    /// The virtual time limit passed before the future was done.
    TimeLimit {
        /// The limit.
        limit: Duration,
    },
    /// Tasks kept running without the clock ever moving.
    Livelock,
}

impl fmt::Display for SimError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SimError::Deadlock { now, live } => write!(
                f,
                "deadlock at {now:?}: nothing can run and no timer is set ({} live tasks: {})",
                live.len(),
                live.join(", ")
            ),
            SimError::TimeLimit { limit } => write!(f, "virtual time limit {limit:?} reached"),
            SimError::Livelock => write!(
                f,
                "livelock: {LIVELOCK_STEPS} polls without the clock moving"
            ),
        }
    }
}

impl std::error::Error for SimError {}

/// A task that is still alive, as [`Sim::live_tasks`] lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskInfo {
    /// The task's id (spawn order).
    pub id: u64,
    /// Its node.
    pub node: NodeId,
    /// Where it was spawned (the caller of `Runtime::spawn`).
    pub location: String,
}

impl fmt::Display for TaskInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "task {} (spawned at {})", self.id, self.location)
    }
}

/// A deterministic simulation: the executor, the clock, the network and the trace. Cheap to
/// clone (a shared handle).
///
/// The deterministic executor: one thread, a seeded choice of which ready task runs next,
/// and a virtual clock that only moves when nothing is ready.
///
/// * **Tasks** are kept in a map by id (ids are handed out in spawn order). A waker puts its
///   task's id in the *ready set*; the run loop takes a ready id chosen by the scheduler's
///   seeded generator (not the order the wakers fired in, which can depend on a hash map's
///   iteration order), polls it, and puts it back if it is pending. So the order in which
///   concurrent tasks interleave is a function of the seed alone. The future
///   [`block_on`](Sim::block_on) drives (the test's own code) runs first whenever it is
///   ready, without a draw: it is often woken by stale wakers (a join handle it awaited
///   earlier, a waker an earlier `block_on` left behind), and such wakes must not shift the
///   schedule of everything else.
/// * **Time** is virtual: [`Sim::now`] reads a counter that the loop moves to the earliest
///   timer deadline when no task is ready (as tokio's paused clock auto-advances), and the
///   timers due then fire together. A sleep never completes early, and no time passes while
///   any task can run.
/// * **Nodes.** Every task belongs to a node (a simulated machine: the client, a coordinator,
///   a storage server, ...). Killing a node aborts its tasks; [`Sim::live_tasks`] lists a
///   node's tasks, which is how a test proves that nothing leaked after a shutdown.
/// * **Aborts and panics** follow the [`Runtime`](fdb_native_transport::runtime::Runtime)
///   contract: an aborted task is dropped at its next suspension point (by the loop, never
///   inside [`TaskHandle::abort`](fdb_native_transport::runtime::TaskHandle::abort)), and a
///   panicking task is caught, reported as `JoinError::Panicked`, and recorded
///   ([`Sim::panics`]).
#[derive(Clone)]
pub struct Sim {
    pub(crate) shared: Arc<Shared>,
}

impl fmt::Debug for Sim {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sim")
            .field("seed", &self.shared.seed)
            .field("now", &self.now())
            .finish_non_exhaustive()
    }
}

impl Sim {
    /// Whether `other` is a handle to this very simulation.
    pub fn same_sim(&self, other: &Sim) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }

    /// A simulation of `seed`, printing its trace at `level`, with the network's default
    /// fault settings ([`NetConfig::default`](crate::NetConfig)).
    pub fn new(seed: u64, level: TraceLevel) -> Sim {
        Sim::with_net(seed, level, crate::NetConfig::default())
    }

    /// A simulation of `seed` whose network behaves as `net` says.
    pub fn with_net(seed: u64, level: TraceLevel, net: crate::NetConfig) -> Sim {
        let mut master = SimRng::new(seed);
        let sched = master.fork();
        let net_rng = master.fork();
        Sim {
            shared: Arc::new(Shared {
                clock: AtomicU64::new(START.since_origin().as_nanos() as u64),
                tasks: Mutex::new(Tasks::default()),
                ready: Mutex::new(BTreeSet::new()),
                timers: Mutex::new(Timers::default()),
                sched: Mutex::new(sched),
                master: Mutex::new(master),
                trace: Mutex::new(Trace::new(seed, level)),
                net: Mutex::new(NetState::new(net, net_rng)),
                nodes: Mutex::new(Vec::new()),
                panics: Mutex::new(Vec::new()),
                running: AtomicBool::new(false),
                seed,
            }),
        }
    }

    /// The seed.
    pub fn seed(&self) -> u64 {
        self.shared.seed
    }

    /// The virtual time.
    pub fn now(&self) -> Instant {
        self.shared.now()
    }

    /// Virtual time since the simulation started.
    pub fn elapsed(&self) -> Duration {
        self.now() - START
    }

    /// A new generator, forked from the simulation's seed (for a workload's own decisions).
    pub fn rng(&self) -> SimRng {
        lock(&self.shared.master).fork()
    }

    /// Adds a machine called `name` at `ip`.
    pub fn add_node(&self, name: impl Into<String>, ip: IpAddr) -> NodeId {
        let mut nodes = lock(&self.shared.nodes);
        nodes.push(NodeInfo {
            name: name.into(),
            ip,
            next_port: 40_000,
        });
        NodeId(nodes.len() as u32 - 1)
    }

    /// The name `node` was added with.
    pub fn node_name(&self, node: NodeId) -> String {
        self.shared.node_name(node)
    }

    /// The address of `node`.
    pub fn node_ip(&self, node: NodeId) -> IpAddr {
        self.shared.node_ip(node)
    }

    /// Records an event in the trace (and prints it at `level` or above; see
    /// [`TraceLevel`]).
    pub fn trace(
        &self,
        level: TraceLevel,
        kind: &str,
        args: &[u64],
        detail: impl FnOnce() -> String,
    ) {
        self.shared.trace(level, kind, args, detail);
    }

    /// The level the trace is printed at.
    pub fn trace_level(&self) -> TraceLevel {
        self.shared.trace_level()
    }

    /// The hash of every event so far: equal for two runs of a seed if and only if (up to
    /// hash collisions) they made the same decisions in the same order.
    pub fn trace_hash(&self) -> u64 {
        lock(&self.shared.trace).hash()
    }

    /// How many events were traced.
    pub fn trace_events(&self) -> u64 {
        lock(&self.shared.trace).events()
    }

    /// Spawns `future` on `node`.
    #[track_caller]
    pub fn spawn_on<T, F>(&self, node: NodeId, future: F) -> SimTask<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let location = Location::caller();
        let join = Arc::new(JoinInner {
            state: Mutex::new(JoinState {
                result: None,
                waker: None,
                finished: false,
            }),
            id: AtomicU64::new(0),
            shared: Arc::downgrade(&self.shared),
        });
        let wrapped = TaskFuture {
            future: Box::pin(future),
            join: join.clone(),
            shared: Arc::downgrade(&self.shared),
            done: false,
        };
        let id = self.shared.spawn_boxed(node, Box::pin(wrapped), location);
        join.id.store(id, Ordering::Release);
        SimTask { join }
    }

    /// Aborts every task of `node` (each is dropped at its next suspension point).
    pub fn abort_node_tasks(&self, node: NodeId) {
        let ids: Vec<u64> = lock(&self.shared.tasks)
            .map
            .iter()
            .filter(|(_, t)| t.node == node)
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            self.shared.abort_task(id);
        }
    }

    /// The tasks of `node` that have not finished.
    pub fn live_tasks(&self, node: NodeId) -> Vec<TaskInfo> {
        lock(&self.shared.tasks)
            .map
            .iter()
            .filter(|(_, t)| t.node == node)
            .map(|(id, t)| TaskInfo {
                id: *id,
                node: t.node,
                location: t.location.to_string(),
            })
            .collect()
    }

    /// The messages of the tasks that panicked so far.
    pub fn panics(&self) -> Vec<String> {
        lock(&self.shared.panics).clone()
    }

    /// A future that completes `duration` from now (on the virtual clock).
    pub fn sleep(&self, duration: Duration) -> SimSleep {
        self.sleep_until(self.now() + duration)
    }

    /// A future that completes once the virtual clock reaches `deadline`.
    pub fn sleep_until(&self, deadline: Instant) -> SimSleep {
        SimSleep {
            shared: self.shared.clone(),
            deadline,
            key: None,
        }
    }

    /// Runs the simulation until `future` completes, with no time limit.
    pub fn block_on<F: Future>(&self, future: F) -> Result<F::Output, SimError> {
        self.run(future, None)
    }

    /// Runs the simulation until `future` completes, or fails once `limit` of virtual time
    /// has passed since the call.
    pub fn block_on_within<F: Future>(
        &self,
        limit: Duration,
        future: F,
    ) -> Result<F::Output, SimError> {
        self.run(future, Some((self.now() + limit, limit)))
    }

    /// Lets the simulation run for `duration` of virtual time.
    pub fn run_for(&self, duration: Duration) -> Result<(), SimError> {
        let sleep = self.sleep(duration);
        self.run(sleep, None)
    }

    fn run<F: Future>(
        &self,
        future: F,
        deadline: Option<(Instant, Duration)>,
    ) -> Result<F::Output, SimError> {
        assert!(
            !self.shared.running.swap(true, Ordering::AcqRel),
            "Sim::block_on is not reentrant"
        );
        struct Running<'a>(&'a AtomicBool);
        impl Drop for Running<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let _running = Running(&self.shared.running);

        let mut future = pin!(future);
        let main_waker = Waker::from(Arc::new(TaskWaker {
            id: MAIN,
            shared: Arc::downgrade(&self.shared),
        }));
        self.shared.wake_id(MAIN);
        let mut idle_steps = 0u64;
        loop {
            match self.pick_ready() {
                Some(MAIN) => {
                    let mut cx = Context::from_waker(&main_waker);
                    if let Poll::Ready(out) = future.as_mut().poll(&mut cx) {
                        return Ok(out);
                    }
                }
                Some(id) => self.run_task(id),
                None => {
                    idle_steps = 0;
                    let next = lock(&self.shared.timers)
                        .map
                        .first_key_value()
                        .map(|(k, _)| k.0);
                    let Some(at) = next else {
                        return Err(SimError::Deadlock {
                            now: self.elapsed(),
                            live: lock(&self.shared.tasks)
                                .map
                                .iter()
                                .map(|(id, t)| {
                                    format!(
                                        "{id}@{} ({})",
                                        self.shared.node_name(t.node),
                                        t.location
                                    )
                                })
                                .collect(),
                        });
                    };
                    if let Some((limit_at, limit)) = deadline {
                        if at > limit_at {
                            return Err(SimError::TimeLimit { limit });
                        }
                    }
                    self.advance_to(at);
                }
            }
            idle_steps += 1;
            if idle_steps > LIVELOCK_STEPS {
                return Err(SimError::Livelock);
            }
        }
    }

    /// Moves the clock to `at` (if it is later) and fires the timers due.
    fn advance_to(&self, at: Instant) {
        let now = self.now();
        if at > now {
            self.shared
                .clock
                .store(at.since_origin().as_nanos() as u64, Ordering::Release);
            self.shared.trace(
                TraceLevel::Verbose,
                "time",
                &[at.since_origin().as_nanos() as u64],
                String::new,
            );
        }
        let now = self.now();
        let due: Vec<Waker> = {
            let mut timers = lock(&self.shared.timers);
            let mut due = Vec::new();
            while let Some(entry) = timers.map.first_entry() {
                if entry.key().0 > now {
                    break;
                }
                due.push(entry.remove());
            }
            due
        };
        for waker in due {
            waker.wake();
        }
    }

    /// The next task to poll: the driven future first whenever it is ready (it is the test's
    /// own code, and is often woken by stale wakers: a join handle it awaited earlier, a
    /// waker an earlier `block_on` left behind; giving it no draw from the scheduler keeps
    /// such wakes from shifting the schedule of everything else), else a ready task drawn
    /// with the scheduler's generator.
    fn pick_ready(&self) -> Option<u64> {
        let mut ready = lock(&self.shared.ready);
        if ready.remove(&MAIN) {
            return Some(MAIN);
        }
        let n = ready.len();
        if n == 0 {
            return None;
        }
        let i = lock(&self.shared.sched).index(n);
        let id = *ready.iter().nth(i).expect("index below len");
        ready.remove(&id);
        Some(id)
    }

    fn run_task(&self, id: u64) {
        let taken = {
            let mut tasks = lock(&self.shared.tasks);
            let Some(entry) = tasks.map.get_mut(&id) else {
                return;
            };
            if entry.aborted {
                let entry = tasks.map.remove(&id).expect("present");
                drop(tasks);
                self.shared
                    .trace(TraceLevel::Verbose, "abort", &[id], String::new);
                // Dropped outside the lock: destructors may wake or spawn.
                drop(entry);
                return;
            }
            entry
                .future
                .take()
                .map(|future| (future, entry.waker.clone()))
        };
        let Some((mut future, waker)) = taken else {
            return;
        };
        self.shared
            .trace(TraceLevel::Verbose, "poll", &[id], || format!("task {id}"));
        let mut cx = Context::from_waker(&waker);
        let poll = future.as_mut().poll(&mut cx);
        let mut tasks = lock(&self.shared.tasks);
        match poll {
            Poll::Ready(()) => {
                tasks.map.remove(&id);
                drop(tasks);
                drop(future);
            }
            Poll::Pending => {
                let entry = tasks.map.get_mut(&id).expect("only the loop removes tasks");
                if entry.aborted {
                    tasks.map.remove(&id);
                    drop(tasks);
                    drop(future);
                } else {
                    entry.future = Some(future);
                }
            }
        }
    }
}

struct JoinState<T> {
    result: Option<Result<T, JoinError>>,
    waker: Option<Waker>,
    finished: bool,
}

struct JoinInner<T> {
    state: Mutex<JoinState<T>>,
    id: AtomicU64,
    shared: Weak<Shared>,
}

impl<T> JoinInner<T> {
    fn complete(&self, result: Result<T, JoinError>) {
        let waker = {
            let mut state = lock(&self.state);
            if state.finished {
                return;
            }
            state.finished = true;
            state.result = Some(result);
            state.waker.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
    }
}

/// The future the executor polls for a spawned task: the task's own, with its panics caught
/// and its output (or abort) handed to its [`SimTask`].
struct TaskFuture<F: Future> {
    future: Pin<Box<F>>,
    join: Arc<JoinInner<F::Output>>,
    shared: Weak<Shared>,
    done: bool,
}

impl<F: Future> Future for TaskFuture<F> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = &mut *self;
        match catch_unwind(AssertUnwindSafe(|| this.future.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(out)) => {
                this.done = true;
                this.join.complete(Ok(out));
                Poll::Ready(())
            }
            Err(payload) => {
                this.done = true;
                let message = payload
                    .downcast_ref::<&str>()
                    .map(|s| (*s).to_owned())
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "a non-string panic".to_owned());
                if let Some(shared) = this.shared.upgrade() {
                    let id = this.join.id.load(Ordering::Acquire);
                    shared.trace(TraceLevel::Events, "panic", &[id], || {
                        format!("task {id}: {message}")
                    });
                    lock(&shared.panics).push(message);
                }
                this.join.complete(Err(JoinError::Panicked));
                Poll::Ready(())
            }
        }
    }
}

impl<F: Future> Drop for TaskFuture<F> {
    fn drop(&mut self) {
        if !self.done {
            self.join.complete(Err(JoinError::Aborted));
        }
    }
}

/// Handle to a task of a [`Sim`]. Dropping it detaches the task.
pub struct SimTask<T> {
    join: Arc<JoinInner<T>>,
}

impl<T> fmt::Debug for SimTask<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimTask")
            .field("id", &self.join.id.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl<T> SimTask<T> {
    /// The task's id.
    pub fn id(&self) -> u64 {
        self.join.id.load(Ordering::Acquire)
    }
}

impl<T> Future for SimTask<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = lock(&self.join.state);
        if let Some(result) = state.result.take() {
            return Poll::Ready(result);
        }
        if state.finished {
            // Polled again after it was ready: the value is gone.
            return Poll::Ready(Err(JoinError::Aborted));
        }
        match &mut state.waker {
            Some(w) if w.will_wake(cx.waker()) => {}
            slot => *slot = Some(cx.waker().clone()),
        }
        Poll::Pending
    }
}

impl<T: Send + 'static> TaskHandle<T> for SimTask<T> {
    fn abort(&self) {
        if lock(&self.join.state).finished {
            return;
        }
        if let Some(shared) = self.join.shared.upgrade() {
            shared.abort_task(self.join.id.load(Ordering::Acquire));
        }
    }

    fn is_finished(&self) -> bool {
        lock(&self.join.state).finished
    }
}

/// A sleep on the virtual clock.
pub struct SimSleep {
    shared: Arc<Shared>,
    deadline: Instant,
    key: Option<TimerKey>,
}

impl fmt::Debug for SimSleep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimSleep")
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

impl Future for SimSleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.shared.now() >= self.deadline {
            if let Some(key) = self.key.take() {
                self.shared.cancel_timer(key);
            }
            return Poll::Ready(());
        }
        let this = &mut *self;
        this.shared
            .refresh_timer(&mut this.key, this.deadline, cx.waker());
        Poll::Pending
    }
}

impl Drop for SimSleep {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.shared.cancel_timer(key);
        }
    }
}
