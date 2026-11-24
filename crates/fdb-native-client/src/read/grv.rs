//! Read versions: `getConsistentReadVersion` and `readVersionBatcher`.

use std::collections::{BTreeMap, VecDeque};
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use fdb_native_schema::Request;
use fdb_native_schema::domain::{GetReadVersion, INVALID_VERSION, ReadVersion};
use fdb_native_transport::peer::StreamWrapper;
use fdb_native_transport::runtime::{Instant, Runtime};
use fdb_native_wire::Endpoint;
use futures_channel::{mpsc, oneshot};
use futures_core::Stream;

use super::balance::ProxyBalance;
use super::util::{Either, never, now_secs, race, secs};
use super::{Shared, lock};
use crate::error::Error;
use crate::error::codes::{INTERNAL_ERROR, INVALID_OPTION_VALUE, OPERATION_CANCELLED};

/// A read version as the batcher hands it to every transaction of a batch.
pub(crate) type GrvResult = Result<Arc<ReadVersion>, Error>;

/// A transaction waiting for a read version.
pub(crate) type Waiter = oneshot::Sender<GrvResult>;

/// `TransactionPriority`: the priority a read version is requested with.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Priority {
    /// `BATCH`: throttled first.
    Batch,
    /// `DEFAULT`.
    #[default]
    Default,
    /// `IMMEDIATE` (system immediate): never throttled.
    Immediate,
}

/// How to get a read version: the transaction options that change the request (C++'s
/// `TransactionState::getReadVersion` flags).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct ReadVersionOptions {
    /// The priority (`PRIORITY_BATCH`, `PRIORITY_SYSTEM_IMMEDIATE`; default
    /// `PRIORITY_DEFAULT`).
    pub priority: Priority,
    /// `CAUSAL_READ_RISKY`: `FLAG_CAUSAL_READ_RISKY`.
    pub causal_read_risky: bool,
    /// `USE_PROVISIONAL_PROXIES`: `FLAG_USE_PROVISIONAL_PROXIES`, and provisional GRV
    /// proxies are used.
    pub use_provisional_proxies: bool,
    /// `LOCK_AWARE` / `READ_LOCK_AWARE`: a read version from a locked database is accepted
    /// instead of failing with `database_locked`.
    pub lock_aware: bool,
    /// `MAX_GRV_QUEUE_DELAY` (milliseconds, 8.0 only; ignored on a 7.4 cluster, which has no
    /// such option): how long the GRV proxy may queue the request. It is part of the batcher's
    /// key (NativeAPI 8.0:5741), so requests with different delays are sent separately.
    pub max_grv_queue_delay_ms: Option<i64>,
}

impl ReadVersionOptions {
    /// The request's `flags`: the priority in the high byte and the flag bits.
    pub fn flags(&self) -> u32 {
        let priority = match self.priority {
            Priority::Batch => GetReadVersion::PRIORITY_BATCH,
            Priority::Default => GetReadVersion::PRIORITY_DEFAULT,
            Priority::Immediate => GetReadVersion::PRIORITY_SYSTEM_IMMEDIATE,
        };
        let mut flags = priority;
        if self.causal_read_risky {
            flags |= GetReadVersion::FLAG_CAUSAL_READ_RISKY;
        }
        if self.use_provisional_proxies {
            flags |= GetReadVersion::FLAG_USE_PROVISIONAL_PROXIES;
        }
        flags
    }

    /// `invalid_option_value` (2006) if `max_grv_queue_delay_ms` is outside `[0, i32::MAX]`
    /// (`extractIntOption(value, 0, std::numeric_limits<int32_t>::max())`, NativeAPI
    /// 8.0:5096-5098), whichever release the cluster runs.
    pub fn validate(&self) -> Result<(), Error> {
        match self.max_grv_queue_delay_ms {
            Some(ms) if !(0..=i64::from(i32::MAX)).contains(&ms) => Err(Error::with_message(
                INVALID_OPTION_VALUE,
                "max_grv_queue_delay_ms must be in [0, 2147483647]",
            )),
            _ => Ok(()),
        }
    }

    /// The batcher these options go through: one per distinct request.
    pub(crate) fn batcher_key(&self) -> BatcherKey {
        (self.flags(), self.max_grv_queue_delay_ms)
    }
}

/// `metadataVersionKey` (`\xff/metadataVersion`): the system key whose value changes
/// whenever a transaction tells the cluster that cached metadata must be refreshed. The GRV
/// proxies report it with every read version.
pub const METADATA_VERSION_KEY: &[u8] = b"\xff/metadataVersion";

/// `DatabaseContext::metadataVersionCache` (`METADATA_VERSION_CACHE_SIZE` entries): the
/// `\xff/metadataVersion` at the latest read versions the database saw, from GRV replies
/// (`extractReadVersion`, NativeAPI 7.4:7830-7837, 8.0:5557-5562) and commit replies
/// (`tryCommit`, 7.4:6990-6996). A version is added only if it is newer than the newest
/// (C++ keeps a ring buffer sorted by version and looks versions up by binary search; this
/// is the same set, oldest dropped first).
#[derive(Debug)]
pub(crate) struct MetadataVersionCache {
    entries: VecDeque<(i64, Option<Vec<u8>>)>,
    capacity: usize,
}

impl MetadataVersionCache {
    pub(crate) fn new(capacity: usize) -> Self {
        MetadataVersionCache {
            entries: VecDeque::new(),
            capacity: capacity.max(1),
        }
    }

    /// Remembers the value at `version`, if it is newer than all remembered.
    pub(crate) fn insert(&mut self, version: i64, value: Option<Vec<u8>>) {
        if self
            .entries
            .back()
            .is_some_and(|(newest, _)| version <= *newest)
        {
            return;
        }
        if self.entries.len() == self.capacity {
            self.entries.pop_front();
        }
        self.entries.push_back((version, value));
    }

    /// The value at exactly `version`, if remembered.
    pub(crate) fn get(&self, version: i64) -> Option<Option<Vec<u8>>> {
        let (a, b) = self.entries.as_slices();
        // Sorted by version across both slices: search whichever may hold it.
        let found = match b.first() {
            Some((first_of_b, _)) if version >= *first_of_b => b
                .binary_search_by_key(&version, |e| e.0)
                .ok()
                .map(|i| &b[i]),
            _ => a
                .binary_search_by_key(&version, |e| e.0)
                .ok()
                .map(|i| &a[i]),
        };
        found.map(|(_, value)| value.clone())
    }
}

impl<R: Runtime, W: StreamWrapper<R::Stream>> Shared<R, W> {
    /// `getConsistentReadVersion` (NativeAPI 7.4:7595-7669, 8.0:5288-5381): a read version
    /// for `count` transactions from one of the GRV proxies, load balanced
    /// (`basicLoadBalance`), asked again of the new proxies whenever the proxies change
    /// before a reply (`onProxiesChanged`).
    pub(crate) async fn get_consistent_read_version(
        &self,
        count: u32,
        flags: u32,
        max_grv_queue_delay_ms: Option<i64>,
    ) -> Result<ReadVersion, Error> {
        let use_provisional = flags & GetReadVersion::FLAG_USE_PROVISIONAL_PROXIES != 0;
        loop {
            self.check_open()?;
            let info = self
                .monitor
                .grv_proxies(use_provisional)
                .await
                .map_err(|_| Error::new(OPERATION_CANCELLED))?;
            self.hold_proxy_references(&info);
            let alternatives: Vec<Endpoint> = info
                .available_grv_proxies(use_provisional)
                .unwrap_or_default()
                .iter()
                .map(|p| p.streams.get_consistent_read_version)
                .collect();
            if alternatives.is_empty() {
                continue;
            }
            let request = GetReadVersion {
                transaction_count: count,
                flags,
                tags: BTreeMap::new(),
                debug_id: None,
                span_context: Default::default(),
                // `ssVersionVectorCache.getMaxVersion()`: version vectors are not used.
                max_version: INVALID_VERSION,
                max_grv_queue_delay_ms,
            };
            // `GrvProxyInfo`: weighted by the proxies' `processBusyTime`, for the proxies of
            // this info (a new info starts again from uniform probabilities).
            let model = {
                let key = (info.id(), info.grv_proxy_ids());
                let now = now_secs(self.rt.now());
                lock(&self.grv_model).get(key, now)
            };
            let reply = self.basic_load_balance(
                &alternatives,
                move |schema, reply, out| request.encode_request_into(schema, reply, out),
                GetReadVersion::decode_reply,
                Some(ProxyBalance {
                    model: &model,
                    busy_time: |reply: &ReadVersion| reply.process_busy_time,
                }),
            );
            let changed = async {
                match self.monitor.proxies_changed(&info).await {
                    Ok(_) => (),
                    // No more changes: keep waiting for the reply.
                    Err(_) => never().await,
                }
            };
            match race(reply, changed).await {
                Either::Left(Ok(v)) if v.version > 0 => return Ok(v),
                // C++ asserts.
                Either::Left(Ok(_)) => return Err(Error::new(INTERNAL_ERROR)),
                Either::Left(Err(e)) => return Err(e),
                Either::Right(()) => continue,
            }
        }
    }
}

/// What a `readVersionBatcher` is for (`cx->versionBatcher[...]`: the flags on 7.4, the flags
/// and `maxGrvQueueDelayMS` on 8.0 as `VersionBatcherKey`). The flags include the priority, so
/// batch, default and immediate requests are never sent together.
pub(crate) type BatcherKey = (u32, Option<i64>);

/// One `readVersionBatcher` (NativeAPI 7.4:7671-7761, 8.0:5383-5482): collects the read
/// version requests of one [`BatcherKey`] and sends them as batches.
pub(crate) struct Batcher<R: Runtime> {
    pub(crate) sender: mpsc::UnboundedSender<Waiter>,
    pub(crate) task: R::Task<()>,
}

impl<R: Runtime> Batcher<R> {
    pub(crate) fn start<W: StreamWrapper<R::Stream>>(
        shared: Arc<Shared<R, W>>,
        key: BatcherKey,
    ) -> Batcher<R> {
        let (sender, receiver) = mpsc::unbounded();
        let rt = shared.rt.clone();
        let task = rt.spawn(run_batcher(shared, key, receiver));
        Batcher { sender, task }
    }
}

type InFlight = Pin<Box<dyn Future<Output = f64> + Send>>;

/// How many batches in a row of a single request make the batcher stop waiting for company
/// while no read version request is in flight. A transaction that asks for its read version
/// only after the previous one's reply (a serial client, concurrency 1) leaves nothing to join
/// the batch, and waiting out `batchTime` below the timer resolution means polling the clock
/// (about 25 us of CPU per transaction on tokio, measured). The first request of the next
/// batch is then sent after the usual yield, which still lets the other tasks of that moment
/// add theirs. Any other state waits as C++ does: a batch that gathered more than one request,
/// or a request that arrives while another batch is in flight (another transaction is running
/// concurrently), restores the wait, and with it the batch sizes the wait produces (about 6 at
/// 16 concurrent transactions, 60 at 256). C++ always waits `batchTime`, but sleeps in its
/// reactor instead of polling. Bead `foundationdb-rs-native-yw5`.
const LONE_BATCHES_BEFORE_NO_WAIT: u32 = 2;

/// How many polls in a row may read the same [`Runtime::now`] before a [`BatchDelay`] stops
/// yielding and sleeps instead: a clock that stands still while tasks run is virtual (tokio's
/// paused clock moves only when every task waits on a timer), and yielding would never reach
/// the deadline.
const FROZEN_CLOCK_POLLS: u32 = 64;

/// `delay(batchTime, TaskPriority::GetConsistentReadVersion)`: the wait from a batch's first
/// request to its sending.
///
/// Flow's `delay` is precise: `Net2` checks its timer heap on every run-loop iteration and
/// sleeps in the reactor to the microsecond, and `delay(0)` queues the task as ready (a yield).
/// `batchTime` is half the GRV latency, tens of microseconds on a LAN, which is below what some
/// runtimes' timers can wait: tokio's `sleep(20us)` takes 1.05 ms, so every transaction would
/// pay a millisecond for its read version. A wait shorter than the runtime's
/// [`TIMER_RESOLUTION`](Runtime::TIMER_RESOLUTION) therefore re-polls, yielding to the other
/// tasks each time, until the clock passes the deadline: the requests that arrive meanwhile
/// join the batch, as in C++ (sending after a single yield instead made batches 3 to 5 times
/// smaller under load; see `docs/benchmarks.md`). The cost is the polling, for less than one
/// resolution per batch. A longer wait, or any wait on a precise timer, sleeps.
///
/// The polling is pure cost when nothing can join: see [`LONE_BATCHES_BEFORE_NO_WAIT`].
enum BatchDelay<'a, R: Runtime> {
    /// On the runtime's timer.
    Sleep(Pin<Box<dyn Future<Output = ()> + Send + 'a>>),
    /// Yielding until `rt.now()` reaches `deadline`. `last` is the previous reading and
    /// `frozen` how many polls in a row saw it unchanged.
    Yield {
        rt: &'a R,
        deadline: Instant,
        last: Instant,
        frozen: u32,
    },
}

impl<'a, R: Runtime> BatchDelay<'a, R> {
    fn new(rt: &'a R, batch_time: f64) -> Self {
        let wait = secs(batch_time);
        let now = rt.now();
        let deadline = now + wait;
        if !wait.is_zero() && wait >= R::TIMER_RESOLUTION {
            BatchDelay::Sleep(Box::pin(rt.sleep_until(deadline)))
        } else {
            BatchDelay::Yield {
                rt,
                deadline,
                last: now,
                frozen: 0,
            }
        }
    }

    fn poll(&mut self, cx: &mut std::task::Context<'_>) -> Poll<()> {
        match self {
            BatchDelay::Sleep(sleep) => sleep.as_mut().poll(cx),
            BatchDelay::Yield {
                rt,
                deadline,
                last,
                frozen,
            } => {
                let now = rt.now();
                if now >= *deadline {
                    return Poll::Ready(());
                }
                if now == *last {
                    *frozen += 1;
                    if *frozen >= FROZEN_CLOCK_POLLS {
                        let sleep = Box::pin(rt.sleep_until(*deadline));
                        *self = BatchDelay::Sleep(sleep);
                        return self.poll(cx);
                    }
                } else {
                    *last = now;
                    *frozen = 0;
                }
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

/// The batcher's loop. A batch is sent when it reaches `max_batch_size`, or `batch_time`
/// after its first request ([`BatchDelay`]); `batch_time` follows half the reply latency,
/// smoothed (`0.1 * target + 0.9 * batch_time`) and capped at `GRV_BATCH_TIMEOUT`. Batches in
/// flight are driven by this task, so they end with it.
async fn run_batcher<R: Runtime, W: StreamWrapper<R::Stream>>(
    shared: Arc<Shared<R, W>>,
    key: BatcherKey,
    mut receiver: mpsc::UnboundedReceiver<Waiter>,
) {
    let max_batch = shared.config.max_batch_size.max(1);
    let max_batch_time = shared.config.grv_batch_timeout.as_secs_f64();
    let mut batch_time = 0.0_f64;
    let mut pending: Vec<Waiter> = Vec::new();
    let mut timer: Option<BatchDelay<'_, R>> = None;
    let mut in_flight: Vec<InFlight> = Vec::new();
    // Batches in a row that held one request.
    let mut lone_batches = 0_u32;
    let mut closed = false;

    poll_fn(|cx| {
        loop {
            let mut progress = false;
            let mut send = false;
            let mut new_timer = false;
            // Everything that has arrived, up to a full batch.
            while !closed && !send {
                match Pin::new(&mut receiver).poll_next(cx) {
                    Poll::Ready(Some(waiter)) => {
                        progress = true;
                        pending.push(waiter);
                        if pending.len() >= max_batch {
                            send = true;
                        } else if timer.is_none() {
                            let alone =
                                in_flight.is_empty() && lone_batches >= LONE_BATCHES_BEFORE_NO_WAIT;
                            timer = Some(BatchDelay::new(
                                &shared.rt,
                                if alone { 0.0 } else { batch_time },
                            ));
                            new_timer = true;
                        }
                    }
                    Poll::Ready(None) => {
                        closed = true;
                        progress = true;
                        send = !pending.is_empty();
                    }
                    Poll::Pending => break,
                }
            }
            if let Some(t) = &mut timer {
                if new_timer {
                    // `delay(batchTime)` yields at least once, even for 0: let the other
                    // tasks of the moment add their requests first.
                    cx.waker().wake_by_ref();
                } else if t.poll(cx).is_ready() {
                    send = true;
                }
            }
            if send && !pending.is_empty() {
                progress = true;
                timer = None;
                let waiters = std::mem::take(&mut pending);
                lone_batches = if waiters.len() == 1 {
                    lone_batches.saturating_add(1)
                } else {
                    0
                };
                in_flight.push(Box::pin(send_batch(shared.clone(), key, waiters)));
            } else if send {
                timer = None;
            }
            let mut i = 0;
            while i < in_flight.len() {
                if let Poll::Ready(latency) = in_flight[i].as_mut().poll(cx) {
                    drop(in_flight.swap_remove(i));
                    progress = true;
                    // Dynamic batching.
                    batch_time = (0.1 * (latency * 0.5) + 0.9 * batch_time).min(max_batch_time);
                } else {
                    i += 1;
                }
            }
            if closed && pending.is_empty() && in_flight.is_empty() {
                return Poll::Ready(());
            }
            if new_timer || !progress {
                return Poll::Pending;
            }
        }
    })
    .await
}

/// Gets one read version for `waiters` and gives it to each (`incrementalBroadcast`).
/// Resolves with the time it took (`timeReply`), whatever the outcome.
async fn send_batch<R: Runtime, W: StreamWrapper<R::Stream>>(
    shared: Arc<Shared<R, W>>,
    (flags, max_grv_queue_delay_ms): BatcherKey,
    waiters: Vec<Waiter>,
) -> f64 {
    let start = now_secs(shared.rt.now());
    let count = u32::try_from(waiters.len()).unwrap_or(u32::MAX);
    let result = shared
        .get_consistent_read_version(count, flags, max_grv_queue_delay_ms)
        .await
        .map(Arc::new);
    for waiter in waiters {
        // A transaction that stopped waiting has dropped its receiver.
        let _ = waiter.send(result.clone());
    }
    now_secs(shared.rt.now()) - start
}

#[cfg(test)]
mod tests {
    use std::task::{Context, Waker};
    use std::time::Duration;

    use fdb_native_transport::runtime::TokioRuntime;

    use super::*;

    async fn wait<R: Runtime>(mut delay: BatchDelay<'_, R>) {
        poll_fn(|cx| delay.poll(cx)).await
    }

    #[tokio::test]
    async fn batch_delay_sleeps_only_from_the_timer_resolution() {
        let rt = TokioRuntime::new();
        assert_eq!(TokioRuntime::TIMER_RESOLUTION, Duration::from_millis(1));
        assert!(matches!(
            BatchDelay::new(&rt, 0.0),
            BatchDelay::Yield { .. }
        ));
        assert!(matches!(
            BatchDelay::new(&rt, 0.000_999),
            BatchDelay::Yield { .. }
        ));
        assert!(matches!(BatchDelay::new(&rt, 0.001), BatchDelay::Sleep(_)));
        assert!(matches!(BatchDelay::new(&rt, 0.005), BatchDelay::Sleep(_)));
        // `delay(0)`: ready at its first poll (the batcher has yielded once already).
        let mut zero = BatchDelay::new(&rt, 0.0);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(zero.poll(&mut cx).is_ready());
    }

    /// Below tokio's resolution the wait is timed by the clock: at least the batch time, and
    /// well under the 1.05 ms that `sleep(300us)` takes.
    #[tokio::test]
    async fn batch_delay_below_the_resolution_waits_on_the_clock() {
        let rt = TokioRuntime::new();
        let mut fastest = Duration::MAX;
        for _ in 0..5 {
            let start = std::time::Instant::now();
            wait(BatchDelay::new(&rt, 0.000_3)).await;
            let took = start.elapsed();
            assert!(took >= Duration::from_micros(300), "{took:?}");
            fastest = fastest.min(took);
        }
        assert!(fastest < Duration::from_micros(900), "{fastest:?}");
    }

    /// tokio's paused clock does not move while a task is runnable, so yielding would never
    /// reach the deadline: the wait notices the clock standing still and sleeps instead, which
    /// lets tokio advance it.
    #[tokio::test(start_paused = true)]
    async fn batch_delay_on_a_frozen_clock_sleeps() {
        let rt = TokioRuntime::new();
        let start = rt.now();
        tokio::time::timeout(Duration::from_secs(10), wait(BatchDelay::new(&rt, 0.000_3)))
            .await
            .expect("the wait ends");
        assert!(rt.now() - start >= Duration::from_micros(300));
    }

    #[test]
    fn metadata_version_cache_keeps_the_newest() {
        let mut cache = MetadataVersionCache::new(3);
        assert_eq!(cache.get(10), None);
        cache.insert(10, None);
        cache.insert(20, Some(b"a".to_vec()));
        // Not newer than the newest: ignored.
        cache.insert(20, Some(b"b".to_vec()));
        cache.insert(15, Some(b"c".to_vec()));
        assert_eq!(cache.get(10), Some(None));
        assert_eq!(cache.get(20), Some(Some(b"a".to_vec())));
        assert_eq!(cache.get(15), None);
        cache.insert(30, Some(b"d".to_vec()));
        // At capacity: the oldest goes.
        cache.insert(40, None);
        assert_eq!(cache.get(10), None);
        assert_eq!(cache.get(20), Some(Some(b"a".to_vec())));
        assert_eq!(cache.get(30), Some(Some(b"d".to_vec())));
        assert_eq!(cache.get(40), Some(None));
        assert_eq!(cache.get(35), None);
    }

    #[test]
    fn metadata_version_cache_lookup_across_the_ring() {
        let mut cache = MetadataVersionCache::new(5);
        for v in 1..=23i64 {
            cache.insert(v * 10, Some(v.to_be_bytes().to_vec()));
        }
        for v in 1..=23i64 {
            let expected = (v > 18).then(|| Some(v.to_be_bytes().to_vec()));
            assert_eq!(cache.get(v * 10), expected, "{v}");
        }
    }

    #[test]
    fn flags_like_cpp() {
        let default = ReadVersionOptions::default();
        assert_eq!(default.flags(), GetReadVersion::PRIORITY_DEFAULT);
        let options = ReadVersionOptions {
            priority: Priority::Batch,
            causal_read_risky: true,
            use_provisional_proxies: true,
            lock_aware: false,
            max_grv_queue_delay_ms: None,
        };
        assert_eq!(
            options.flags(),
            GetReadVersion::PRIORITY_BATCH
                | GetReadVersion::FLAG_CAUSAL_READ_RISKY
                | GetReadVersion::FLAG_USE_PROVISIONAL_PROXIES
        );
    }
}
