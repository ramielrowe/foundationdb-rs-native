//! The load balancers: `basicLoadBalance` (proxies, with the `ModelInterface` weighting the
//! GRV proxies by their `processBusyTime`) and `loadBalance` with its `QueueModel` (storage
//! servers), including second (hedged) requests and lagging requests. See the [module
//! documentation](super) for what is ported and what is left out.

use std::future::{Future, poll_fn};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use fdb_native_schema::domain::{GetKey, GetKeyReply, GetValue, GetValueReply, KeySelector};
use fdb_native_schema::streams::STORAGE_SERVER_STREAMS;
use fdb_native_schema::{Request, SchemaVersion};
use fdb_native_transport::peer::StreamWrapper;
use fdb_native_transport::rpc::RequestError;
use fdb_native_transport::runtime::{Runtime, TaskHandle};
use fdb_native_wire::{Bytes, DecodeError, EncodeError, Endpoint, ErrorOr, NetworkAddress, Uid};
use rustc_hash::FxHashMap;

use super::Shared;
use super::location::Location;
use super::util::{Either, any_available, now_secs, race, secs, try_get_reply, try_get_reply_code};
use crate::error::Error;
use crate::error::codes::*;

// `flow/Knobs.cpp` (the same on both releases; lines 7.4 / 8.0).

/// `LOAD_BALANCE_MAX_BACKOFF` (5 s; 285 / 284).
const LOAD_BALANCE_MAX_BACKOFF: f64 = 5.0;
/// `LOAD_BALANCE_START_BACKOFF` (10 ms; 286 / 285).
const LOAD_BALANCE_START_BACKOFF: f64 = 0.01;
/// `LOAD_BALANCE_BACKOFF_RATE` (2; 287 / 286).
const LOAD_BALANCE_BACKOFF_RATE: f64 = 2.0;
/// `LOAD_BALANCE_MAX_BAD_OPTIONS` (1; 305 / 304).
const LOAD_BALANCE_MAX_BAD_OPTIONS: usize = 1;
/// `LOAD_BALANCE_PENALTY_IS_BAD` (true; 306 / 305).
const LOAD_BALANCE_PENALTY_IS_BAD: bool = true;
/// `QUEUE_MODEL_SMOOTHING_AMOUNT` (2 s; 69 / 72).
const QUEUE_MODEL_SMOOTHING_AMOUNT: f64 = 2.0;
/// `FUTURE_VERSION_INITIAL_BACKOFF` (1 s; 302 / 301).
const FUTURE_VERSION_INITIAL_BACKOFF: f64 = 1.0;
/// `FUTURE_VERSION_MAX_BACKOFF` (8 s; 303 / 302).
const FUTURE_VERSION_MAX_BACKOFF: f64 = 8.0;
/// `FUTURE_VERSION_BACKOFF_GROWTH` (2; 304 / 303).
const FUTURE_VERSION_BACKOFF_GROWTH: f64 = 2.0;
/// `ALTERNATIVES_FAILURE_RESET_TIME` (5 s; 295 / 294).
const ALTERNATIVES_FAILURE_RESET_TIME: f64 = 5.0;
/// `ALTERNATIVES_FAILURE_MIN_DELAY` (50 ms; 296 / 295).
const ALTERNATIVES_FAILURE_MIN_DELAY: f64 = 0.05;
/// `ALTERNATIVES_FAILURE_DELAY_RATIO` (0.2; 297 / 296).
const ALTERNATIVES_FAILURE_DELAY_RATIO: f64 = 0.2;
/// `ALTERNATIVES_FAILURE_MAX_DELAY` (1 s; 298 / 297).
const ALTERNATIVES_FAILURE_MAX_DELAY: f64 = 1.0;
/// `ALTERNATIVES_FAILURE_SLOW_DELAY_RATIO` (0.04; 299 / 298).
const ALTERNATIVES_FAILURE_SLOW_DELAY_RATIO: f64 = 0.04;
/// `ALTERNATIVES_FAILURE_SLOW_MAX_DELAY` (30 s; 300 / 299).
const ALTERNATIVES_FAILURE_SLOW_MAX_DELAY: f64 = 30.0;
/// `ALTERNATIVES_FAILURE_SKIP_DELAY` (1 s; 301 / 300).
const ALTERNATIVES_FAILURE_SKIP_DELAY: f64 = 1.0;
/// `MAX_LAGGING_REQUESTS_OUTSTANDING` (100 000; 288 / 287).
const MAX_LAGGING_REQUESTS_OUTSTANDING: usize = 100_000;
/// `INSTANT_SECOND_REQUEST_MULTIPLIER` (2; 289 / 288).
const INSTANT_SECOND_REQUEST_MULTIPLIER: f64 = 2.0;
/// `BASE_SECOND_REQUEST_TIME` (0.5 ms; 290 / 289).
const BASE_SECOND_REQUEST_TIME: f64 = 0.0005;
/// `SECOND_REQUEST_MULTIPLIER_GROWTH` (0.01; 291 / 290).
const SECOND_REQUEST_MULTIPLIER_GROWTH: f64 = 0.01;
/// `SECOND_REQUEST_MULTIPLIER_DECAY` (0.00025; 292 / 291).
const SECOND_REQUEST_MULTIPLIER_DECAY: f64 = 0.00025;
/// `SECOND_REQUEST_BUDGET_GROWTH` (0.05; 293 / 292).
const SECOND_REQUEST_BUDGET_GROWTH: f64 = 0.05;
/// `SECOND_REQUEST_MAX_BUDGET` (100; 294 / 293).
const SECOND_REQUEST_MAX_BUDGET: f64 = 100.0;
/// `BASIC_LOAD_BALANCE_UPDATE_RATE` (10 s; 307 / 306).
const BASIC_LOAD_BALANCE_UPDATE_RATE: f64 = 10.0;
/// `BASIC_LOAD_BALANCE_MAX_CHANGE` (0.10; 308 / 307).
const BASIC_LOAD_BALANCE_MAX_CHANGE: f64 = 0.10;
/// `BASIC_LOAD_BALANCE_MAX_PROB` (2; 309 / 308).
const BASIC_LOAD_BALANCE_MAX_PROB: f64 = 2.0;
/// `BASIC_LOAD_BALANCE_MIN_REQUESTS` (20; 310 / 309).
const BASIC_LOAD_BALANCE_MIN_REQUESTS: f64 = 20.0;
/// `BASIC_LOAD_BALANCE_COMPUTE_PRECISION` (10000; 313 / 312).
const BASIC_LOAD_BALANCE_COMPUTE_PRECISION: i32 = 10_000;

/// `Smoother` (`fdbrpc/include/fdbrpc/Smoother.h`): a total, and an estimate that follows it
/// exponentially with time constant `e_folding`.
#[derive(Clone, Debug)]
pub(crate) struct Smoother {
    e_folding: f64,
    time: f64,
    total: f64,
    estimate: f64,
}

impl Smoother {
    pub(crate) fn new(e_folding: f64) -> Smoother {
        Smoother {
            e_folding,
            time: 0.0,
            total: 0.0,
            estimate: 0.0,
        }
    }

    pub(crate) fn add_delta(&mut self, delta: f64, now: f64) {
        self.update(now);
        self.total += delta;
    }

    pub(crate) fn smooth_total(&mut self, now: f64) -> f64 {
        self.update(now);
        self.estimate
    }

    fn update(&mut self, now: f64) {
        let elapsed = now - self.time;
        if elapsed != 0.0 {
            self.time = now;
            self.estimate +=
                (self.total - self.estimate) * (1.0 - (-elapsed / self.e_folding).exp());
        }
    }
}

/// `QueueData`: what the client knows about one storage server endpoint.
#[derive(Clone, Debug)]
pub(crate) struct QueueData {
    /// Outstanding requests, each counted as the penalty it was sent with, smoothed.
    pub(crate) smooth_outstanding: Smoother,
    /// How many requests are outstanding (counted by [`QueueModel::add_request`], released
    /// by [`QueueModel::end_request`]). C++ has no such count: it keeps a `QueueData` as long
    /// as the `QueueModel`. Here the data of endpoints nothing uses any more is pruned, and
    /// an entry with outstanding requests must survive that, or their release would find no
    /// entry (or a new one, which it would drive below zero).
    outstanding: usize,
    pub(crate) latency: f64,
    /// The server's last reported penalty (1.0 by default).
    pub(crate) penalty: f64,
    /// Not chosen by the model before this time.
    pub(crate) failed_until: f64,
    future_version_backoff: f64,
    increase_backoff_time: f64,
}

impl Default for QueueData {
    fn default() -> Self {
        QueueData {
            smooth_outstanding: Smoother::new(QUEUE_MODEL_SMOOTHING_AMOUNT),
            outstanding: 0,
            latency: 0.001,
            penalty: 1.0,
            failed_until: 0.0,
            future_version_backoff: FUTURE_VERSION_INITIAL_BACKOFF,
            increase_backoff_time: 0.0,
        }
    }
}

/// `QueueModel` (`fdbrpc/QueueModel.cpp`), keyed by the endpoint token's `first` (of the
/// stream's own, adjusted endpoint: one server's `getValue`, `getKey` and `getKeyValues` are
/// three entries, as `getAdjustedEndpoint` makes them), with the second-request state (`secondMultiplier`, `secondBudget`).
#[derive(Debug)]
pub(crate) struct QueueModel {
    /// FxHash: the first half of a storage server's id, looked up for every request.
    data: FxHashMap<u64, QueueData>,
    /// `secondMultiplier`: scales how long the first request is given before a second one
    /// is sent (at least 1; grows with each second request, decays with each reply).
    pub(crate) second_multiplier: f64,
    /// `secondBudget`: how many second requests may be sent; earned by replies to first
    /// requests, spent one per second request.
    pub(crate) second_budget: f64,
}

impl Default for QueueModel {
    fn default() -> Self {
        QueueModel {
            data: FxHashMap::default(),
            second_multiplier: 1.0,
            second_budget: 0.0,
        }
    }
}

impl QueueModel {
    /// `addRequest`: counts a request as the server's penalty; returns it (the `delta`).
    pub(crate) fn add_request(&mut self, id: u64, now: f64) -> f64 {
        let d = self.data.entry(id).or_default();
        let penalty = d.penalty;
        d.smooth_outstanding.add_delta(penalty, now);
        d.outstanding += 1;
        penalty
    }

    /// `endRequest`. An endpoint with no data (never counted, or pruned: [`retain`] keeps
    /// the ones with outstanding requests, so that is not expected) is left alone, not
    /// created with a negative outstanding count.
    ///
    /// [`retain`]: Self::retain
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn end_request(
        &mut self,
        id: u64,
        latency: f64,
        penalty: f64,
        delta: f64,
        clean: bool,
        future_version: bool,
        now: f64,
    ) {
        let Some(d) = self.data.get_mut(&id) else {
            return;
        };
        d.smooth_outstanding.add_delta(-delta, now);
        d.outstanding = d.outstanding.saturating_sub(1);
        d.latency = if clean {
            latency
        } else {
            d.latency.max(latency)
        };
        if future_version {
            if now > d.increase_backoff_time {
                d.future_version_backoff = (d.future_version_backoff
                    * FUTURE_VERSION_BACKOFF_GROWTH)
                    .min(FUTURE_VERSION_MAX_BACKOFF);
                d.increase_backoff_time = now + d.future_version_backoff;
            }
            d.failed_until = now + d.future_version_backoff;
        } else if clean {
            d.future_version_backoff = FUTURE_VERSION_INITIAL_BACKOFF;
            d.increase_backoff_time = 0.0;
        }
        if penalty > 0.0 {
            d.penalty = penalty;
        }
    }

    /// How many endpoints the model has data for.
    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    /// Keeps the data of the endpoints `keep` accepts, and of those with outstanding
    /// requests (a lagging request can outlive the shard that used its endpoint).
    pub(crate) fn retain(&mut self, mut keep: impl FnMut(u64) -> bool) {
        self.data.retain(|id, d| d.outstanding > 0 || keep(*id));
    }

    /// What the model knows of each endpoint, by token.
    pub(crate) fn stats(&self) -> Vec<EndpointStats> {
        let mut stats: Vec<EndpointStats> = self
            .data
            .iter()
            .map(|(id, d)| EndpointStats {
                token_first: *id,
                latency: d.latency,
                penalty: d.penalty,
                outstanding: d.outstanding,
                outstanding_total: d.smooth_outstanding.total,
            })
            .collect();
        stats.sort_by_key(|e| e.token_first);
        stats
    }

    /// `getMeasurement`.
    pub(crate) fn measurement(&mut self, id: u64) -> &mut QueueData {
        self.data.entry(id).or_default()
    }

    /// A first request got its reply (`loadBalance`, in the branch that sends one request):
    /// the multiplier decays toward 1 and the budget grows.
    pub(crate) fn first_request_replied(&mut self) {
        self.second_multiplier =
            (self.second_multiplier - SECOND_REQUEST_MULTIPLIER_DECAY).max(1.0);
        self.second_budget =
            (self.second_budget + SECOND_REQUEST_BUDGET_GROWTH).min(SECOND_REQUEST_MAX_BUDGET);
    }

    /// The second-request delay timer fired: spends one unit of budget on a second request
    /// if there is one (and makes the next one wait longer), else it is not sent.
    pub(crate) fn try_second_request(&mut self) -> bool {
        if self.second_budget >= 1.0 {
            self.second_multiplier += SECOND_REQUEST_MULTIPLIER_GROWTH;
            self.second_budget -= 1.0;
            true
        } else {
            false
        }
    }
}

/// How long `loadBalance` gives the first request before sending a second one to the
/// next-best replica, from the latencies of the best and the next replica: `None` if the
/// next one's latency is unknown (`nextTime >= 1e9`: there is no second choice), `Some(0)`
/// to send it at once (the best replica is more than `INSTANT_SECOND_REQUEST_MULTIPLIER`
/// times slower than the delay would be), else `secondMultiplier * nextTime +
/// BASE_SECOND_REQUEST_TIME`.
fn second_delay_secs(best_time: f64, next_time: f64, multiplier: f64) -> Option<f64> {
    if next_time >= 1e9 {
        return None;
    }
    let delay = multiplier * next_time + BASE_SECOND_REQUEST_TIME;
    if best_time > INSTANT_SECOND_REQUEST_MULTIPLIER * delay {
        Some(0.0)
    } else {
        Some(delay)
    }
}

/// What the queue model knows of one endpoint (`QueueData`), for tests.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq)]
pub struct EndpointStats {
    /// The endpoint token's `first` (the model's key).
    pub token_first: u64,
    /// The latency of the last request that was measured.
    pub latency: f64,
    /// The last penalty the server reported.
    pub penalty: f64,
    /// How many requests are outstanding.
    pub outstanding: usize,
    /// The penalties of the outstanding requests, summed (not smoothed).
    pub outstanding_total: f64,
}

/// What the load balancer is holding on to, for tests.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq)]
pub struct LoadBalanceStats {
    /// The queue model's endpoints, by token.
    pub endpoints: Vec<EndpointStats>,
    /// Lagging requests that have not ended.
    pub lagging_requests: usize,
}

/// `ModelHolder`: a request counted in the model, released once with its outcome, or as
/// unclean (with no latency and no penalty) if the request is abandoned. Owns what it needs
/// to outlive the call that made it: a request that lost to its second request goes on as a
/// lagging request (see [`RequestData`]).
struct ModelHolder<R: Runtime, W> {
    shared: Arc<Shared<R, W>>,
    id: u64,
    delta: f64,
    start: f64,
    released: bool,
}

impl<R: Runtime, W> ModelHolder<R, W> {
    fn new(shared: Arc<Shared<R, W>>, id: u64) -> Self {
        let start = now_secs(shared.rt.now());
        let delta = lock(&shared.model).add_request(id, start);
        ModelHolder {
            shared,
            id,
            delta,
            start,
            released: false,
        }
    }

    fn release(&mut self, clean: bool, future_version: bool, penalty: f64, measure: bool) {
        if self.released {
            return;
        }
        self.released = true;
        let now = now_secs(self.shared.rt.now());
        let latency = if clean || measure {
            now - self.start
        } else {
            0.0
        };
        lock(&self.shared.model).end_request(
            self.id,
            latency,
            penalty,
            self.delta,
            clean,
            future_version,
            now,
        );
    }
}

impl<R: Runtime, W> Drop for ModelHolder<R, W> {
    fn drop(&mut self) {
        // `~ModelHolder` is `release(false, false, -1.0, false)`: unclean, no latency, no
        // penalty, with the smoother brought up to the current time.
        self.release(false, false, -1.0, false);
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A request failure as a [`Error`]: a server's error reply is just its code.
pub(crate) fn client_error(error: RequestError) -> Error {
    match error {
        RequestError::Fdb(e) => Error::new(e.code),
        other => Error::from_request(other),
    }
}

/// The alternative to try (`useAlt`), as `loadBalance` and `basicLoadBalance` walk them:
/// `best` first, then from `start` on, skipping it (C++'s `startAlt` / `startDistance`
/// arithmetic).
fn use_alt(next: usize, start: usize, best: usize, start_distance: usize, n: usize) -> usize {
    if next == start {
        best
    } else if (next + n - start) % n <= start_distance {
        (next + n - 1) % n
    } else {
        next
    }
}

fn grow_backoff(backoff: f64) -> f64 {
    LOAD_BALANCE_MAX_BACKOFF
        .min(LOAD_BALANCE_START_BACKOFF.max(backoff * LOAD_BALANCE_BACKOFF_RATE))
}

/// `ModelInterface` (`fdbrpc/MultiInterface.h`): the probabilities with which
/// `basicLoadBalance` first tries each of a set of proxies. They start uniform, and follow
/// what the proxies report as their `processBusyTime`: every
/// `BASIC_LOAD_BALANCE_UPDATE_RATE` (10 s, as C++'s `recurring` updater) each probability
/// moves by up to `BASIC_LOAD_BALANCE_MAX_CHANGE` toward the share of the proxy's load, when
/// every proxy has reported within half that period and there is enough load to tell.
///
/// Ported for GRV proxies, which C++ builds with `BalanceOnRequests::True` (7.4,
/// `NativeAPI` 2366, 2382, 3019) / `ProxyGrvMetric` (8.0, `ProxyLoadBalanceMetrics.h`): the
/// load is the number of recent requests, `processBusyTime / COMPUTE_PRECISION`
/// (`GrvProxyServer` packs the proxy's recent request count in the quotient and its CPU
/// busyness in the remainder). Commit proxies are modelled on CPU
/// (`processBusyTime % COMPUTE_PRECISION`), but nothing feeds them (a
/// `GetKeyServerLocationsReply` is not a `BasicLoadBalancedReply`), so their probabilities
/// stay uniform and need no model.
#[derive(Debug)]
pub(crate) struct ProxyModel {
    alternatives: Vec<ProxyAlternative>,
    /// When the next periodic update is due (the updater is evaluated lazily, in order,
    /// before each use).
    next_update: f64,
}

#[derive(Clone, Debug)]
struct ProxyAlternative {
    probability: f64,
    cumulative_probability: f64,
    /// `processBusyTime` of the last reply (-1: none yet).
    process_busy_time: i32,
    last_update: f64,
}

impl ProxyModel {
    /// A model of `n` proxies, uniform, created at `now` (seconds).
    pub(crate) fn new(n: usize, now: f64) -> ProxyModel {
        ProxyModel {
            alternatives: (0..n)
                .map(|i| ProxyAlternative {
                    probability: 1.0 / n as f64,
                    cumulative_probability: (i as f64 + 1.0) / n as f64,
                    process_busy_time: -1,
                    last_update: f64::NEG_INFINITY,
                })
                .collect(),
            next_update: now + BASIC_LOAD_BALANCE_UPDATE_RATE,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.alternatives.len()
    }

    /// The probability of the `i`-th proxy being tried first.
    #[cfg(test)]
    fn probability(&self, i: usize) -> f64 {
        self.alternatives[i].probability
    }

    /// `getBest`: the first proxy whose cumulative probability reaches `r` (a number in
    /// `[0, 1)`).
    pub(crate) fn get_best(&mut self, r: f64, now: f64) -> usize {
        self.run_updates(now);
        self.alternatives
            .partition_point(|a| a.cumulative_probability < r)
            .min(self.alternatives.len().saturating_sub(1))
    }

    /// `updateRecent`: the proxy answered, reporting `process_busy_time`.
    pub(crate) fn update_recent(&mut self, index: usize, process_busy_time: i32, now: f64) {
        self.run_updates(now);
        if let Some(a) = self.alternatives.get_mut(index) {
            a.process_busy_time = process_busy_time;
            a.last_update = now;
        }
    }

    /// The periodic updates due by `now`, each as of its own time (nothing changes the
    /// model between calls, so what the updater would have seen is what is here).
    fn run_updates(&mut self, now: f64) {
        while self.next_update <= now {
            let at = self.next_update;
            self.next_update += BASIC_LOAD_BALANCE_UPDATE_RATE;
            if !self.update_probabilities(at) {
                // Nothing changed, so no later update before the next report does either.
                let missed = ((now - self.next_update) / BASIC_LOAD_BALANCE_UPDATE_RATE).floor();
                if missed > 0.0 {
                    self.next_update += missed * BASIC_LOAD_BALANCE_UPDATE_RATE;
                }
            }
        }
    }

    /// `updateProbabilities` for `balanceOnRequests`. Returns whether it changed anything.
    fn update_probabilities(&mut self, now: f64) -> bool {
        let n = self.alternatives.len() as f64;
        let metric = |a: &ProxyAlternative| {
            f64::from(a.process_busy_time / BASIC_LOAD_BALANCE_COMPUTE_PRECISION)
        };
        let mut total_busy = 0.0;
        for a in &self.alternatives {
            total_busy += metric(a);
            if now - a.last_update > BASIC_LOAD_BALANCE_UPDATE_RATE / 2.0 {
                return false;
            }
        }
        if total_busy < BASIC_LOAD_BALANCE_MIN_REQUESTS * n {
            return false;
        }
        let mut total_probability = 0.0;
        for a in &mut self.alternatives {
            let busy = metric(a);
            a.probability += (1.0 / n - (busy / total_busy)) * BASIC_LOAD_BALANCE_MAX_CHANGE;
            a.probability = a.probability.max(1.0 / (BASIC_LOAD_BALANCE_MAX_PROB * n));
            a.probability = a.probability.min(BASIC_LOAD_BALANCE_MAX_PROB / n);
            total_probability += a.probability;
        }
        for a in &mut self.alternatives {
            a.probability /= total_probability;
        }
        let mut cumulative = 0.0;
        for a in &mut self.alternatives {
            cumulative += a.probability;
            a.cumulative_probability = cumulative;
        }
        if let Some(last) = self.alternatives.last_mut() {
            last.cumulative_probability = 1.0;
        }
        true
    }
}

/// Which proxies a [`ProxyModel`] weights, so that the model is replaced when they change
/// (C++ builds a new `ModelInterface` for each new `ClientDBInfo`).
pub(crate) type ProxyModelKey = (Uid, Vec<Uid>);

/// The GRV proxies' model, for the info it was built for.
#[derive(Debug, Default)]
pub(crate) struct GrvModel {
    current: Option<(ProxyModelKey, Arc<Mutex<ProxyModel>>)>,
}

impl GrvModel {
    /// The model for `key`'s proxies: the current one, or a new uniform one.
    pub(crate) fn get(&mut self, key: ProxyModelKey, now: f64) -> Arc<Mutex<ProxyModel>> {
        match &self.current {
            Some((k, model)) if *k == key => model.clone(),
            _ => {
                let model = Arc::new(Mutex::new(ProxyModel::new(key.1.len(), now)));
                self.current = Some((key, model.clone()));
                model
            }
        }
    }
}

/// How `basicLoadBalance` weights its choice of proxy by the replies' `processBusyTime`.
pub(crate) struct ProxyBalance<'a, T> {
    pub(crate) model: &'a Mutex<ProxyModel>,
    /// The reply's `processBusyTime` (`BasicLoadBalancedReply`).
    pub(crate) busy_time: fn(&T) -> i32,
}

impl<R: Runtime, W: StreamWrapper<R::Stream>> Shared<R, W> {
    fn is_failed(&self, (address, token): &(NetworkAddress, Uid)) -> bool {
        self.peers
            .failure_monitor()
            .endpoint_status(address, *token)
            .is_failed()
    }

    /// `basicLoadBalance` (`LoadBalance.actor.h` 7.4:1031-1131, `LoadBalance.h` 8.0:645-760)
    /// for an idempotent request (not `AtMostOnce`) to always-fresh alternatives (proxies):
    /// try them in turn, skipping failed ones, until one answers; a
    /// `broken_promise`/`request_maybe_delivered` moves on (with backoff once all were
    /// tried), any other error is returned. If all are failed, wait for one to come back.
    ///
    /// With `balance`, the first choice is weighted by what the proxies reported as their
    /// `processBusyTime` (`ModelInterface::getBest`, `updateRecent`); without, it is uniform
    /// (what a `ModelInterface` that is never told anything does).
    pub(crate) async fn basic_load_balance<T, E, D>(
        &self,
        alternatives: &[Endpoint],
        encode: E,
        decode: D,
        balance: Option<ProxyBalance<'_, T>>,
    ) -> Result<T, Error>
    where
        E: Fn(SchemaVersion, Uid, &mut Vec<u8>) -> Result<(), EncodeError> + Clone + Send + 'static,
        D: Fn(SchemaVersion, &[u8]) -> Result<ErrorOr<T>, DecodeError>,
    {
        let n = alternatives.len();
        assert!(n > 0, "no alternatives");
        let targets: Vec<(NetworkAddress, Uid)> = alternatives
            .iter()
            .map(|e| (self.peers.primary_address(&e.addresses), e.token))
            .collect();
        let balance = balance.filter(|b| lock(b.model).len() == n);
        let (best_alt, mut next_alt) = {
            let mut rng = lock(&self.rng);
            // `ModelInterface::getBest` picks by probabilities that start uniform and are
            // adjusted from the proxies' `processBusyTime`.
            let best = match &balance {
                Some(b) => {
                    let r = rng.unit();
                    lock(b.model).get_best(r, now_secs(self.rt.now()))
                }
                None => rng.below(n),
            };
            let mut next = rng.below((n - 1).max(1));
            if next >= best {
                next += 1;
            }
            (best, next)
        };
        let start_alt = next_alt;
        let start_distance = (best_alt + n - start_alt) % n;
        let mut num_attempts = 0;
        let mut backoff = 0.0;
        loop {
            self.check_open()?;
            let mut chosen = None;
            for _ in 0..n {
                let alt = use_alt(next_alt, start_alt, best_alt, start_distance, n);
                if !self.is_failed(&targets[alt]) {
                    chosen = Some(alt);
                    break;
                }
                next_alt = (next_alt + 1) % n;
            }
            match chosen {
                None => {
                    // Everything is down: wait for someone to be up.
                    let up = any_available(self.peers.failure_monitor(), &targets);
                    if let Either::Right(()) = race(up, self.closed()).await {
                        return Err(Error::new(OPERATION_CANCELLED));
                    }
                    num_attempts = 0;
                }
                Some(alt) => {
                    if backoff > 0.0 {
                        self.rt.sleep(secs(backoff)).await;
                    }
                    let (address, token) = targets[alt];
                    let error = match try_get_reply(&self.peers, &address, token, encode.clone())
                        .await
                    {
                        Ok((schema, payload)) => match decode(schema, &payload) {
                            Ok(Ok(reply)) => {
                                if let Some(b) = &balance {
                                    let busy = (b.busy_time)(&reply);
                                    lock(b.model).update_recent(alt, busy, now_secs(self.rt.now()));
                                }
                                return Ok(reply);
                            }
                            Ok(Err(e)) => RequestError::from_reply_error(e),
                            Err(e) => RequestError::Decode(e),
                        },
                        Err(e) => e,
                    };
                    let code = try_get_reply_code(&error);
                    if code != BROKEN_PROMISE && code != REQUEST_MAYBE_DELIVERED {
                        return Err(client_error(error));
                    }
                    num_attempts += 1;
                    if num_attempts >= n {
                        backoff = grow_backoff(backoff);
                    }
                }
            }
            next_alt = (next_alt + 1) % n;
        }
    }

    /// `allAlternativesFailedDelay` (`fdbrpc/LoadBalance.actor.cpp` 7.4, `LoadBalance.cpp`
    /// 8.0:29-53): wait for one of `targets` to come back, for a delay that grows while
    /// alternatives keep failing; `all_alternatives_failed` if none does.
    pub(super) async fn all_alternatives_failed_delay(
        &self,
        targets: &[(NetworkAddress, Uid)],
    ) -> Result<(), Error> {
        let delay = {
            let now = now_secs(self.rt.now());
            let mut state = lock(&self.state);
            let f = &mut state.alternatives;
            if now - f.newest > ALTERNATIVES_FAILURE_RESET_TIME {
                f.oldest = now;
            }
            let mut delay = ALTERNATIVES_FAILURE_MIN_DELAY;
            if now - f.last_skip > ALTERNATIVES_FAILURE_SKIP_DELAY {
                f.last_skip = now;
            } else {
                let elapsed = now - f.oldest;
                delay = delay.max(
                    (elapsed * ALTERNATIVES_FAILURE_DELAY_RATIO)
                        .min(ALTERNATIVES_FAILURE_MAX_DELAY),
                );
                delay = delay.max(
                    (elapsed * ALTERNATIVES_FAILURE_SLOW_DELAY_RATIO)
                        .min(ALTERNATIVES_FAILURE_SLOW_MAX_DELAY),
                );
            }
            f.newest = now;
            lock(&self.rng).jitter(delay)
        };
        let up = any_available(self.peers.failure_monitor(), targets);
        match race(race(up, self.closed()), self.rt.sleep(delay)).await {
            Either::Left(Either::Left(())) => Ok(()),
            Either::Left(Either::Right(())) => Err(Error::new(OPERATION_CANCELLED)),
            Either::Right(()) => Err(Error::new(ALL_ALTERNATIVES_FAILED)),
        }
    }

    /// [`load_balance`](Self::load_balance) of a `GetValueRequest`: the value.
    pub(crate) async fn load_balance_get_value(
        &self,
        location: &Location,
        request: GetValue,
    ) -> Result<Option<Vec<u8>>, Error> {
        self.load_balance(location, request).await
    }

    /// `loadBalance` (`LoadBalance.actor.h` 7.4:686-1022, `LoadBalance.h` 8.0:307-620) of a
    /// request to one stream of a shard's replicas ([`LoadBalanced`]), with the queue model,
    /// second (speculative) requests and lagging requests. Returns the reply's result, or the
    /// error to surface (`wrong_shard_server`, `all_alternatives_failed`, `future_version`,
    /// `transaction_too_old`, ...).
    ///
    /// **Second requests.** When the model has a second choice, the first request is given
    /// `secondMultiplier * latency(second choice) + BASE_SECOND_REQUEST_TIME` (nothing, if
    /// the first choice is more than `INSTANT_SECOND_REQUEST_MULTIPLIER` times slower than
    /// that) before a second request goes to the next-best replica, if the model's
    /// `secondBudget` has a whole unit to spend (replies to first requests earn it back at
    /// `SECOND_REQUEST_BUDGET_GROWTH` each). The first good reply of the two is the result.
    /// The other request is not cancelled but becomes a *lagging request*: it is awaited in
    /// a task of its own so that its reply still updates the queue model (latency, penalty,
    /// `future_version` backoff) and its outstanding count is released when it ends, as C++'s
    /// `~RequestData` / `makeLaggingRequest` does. Only idempotent reads are sent this way;
    /// there is no `AtMostOnce` mode here (commits do not use this balancer).
    ///
    /// The failure monitor and the queue model are asked about the stream's own endpoint, as
    /// in C++ (`getAdjustedEndpoint`): what is learned of `getKeyValues` does not drive the
    /// hedging, penalties or `failed_until` of `getValue` and `getKey`.
    pub(crate) async fn load_balance<Q: LoadBalanced>(
        &self,
        location: &Location,
        request: Q,
    ) -> Result<Q::Output, Error> {
        let Some(this) = self.this.upgrade() else {
            return Err(Error::new(OPERATION_CANCELLED));
        };
        let alternatives = &location.servers;
        let n = alternatives.len();
        if n == 0 {
            // An empty team (C++ asserts): nothing to ask until the location is refreshed.
            return Err(Error::new(ALL_ALTERNATIVES_FAILED));
        }
        let targets: Vec<(NetworkAddress, Uid)> = alternatives
            .iter()
            .map(|s| s.target_for(Q::STREAM))
            .collect();
        // The queue model's ids: the token's `first` of the stream's own endpoint (as C++'s
        // `getAdjustedEndpoint` makes it), so that each stream is modelled on its own.
        let ids: Vec<u64> = targets.iter().map(|(_, token)| token.first).collect();
        let count_best = location.best_count.max(1);
        let (mut best_alt, mut next_alt) = {
            let mut rng = lock(&self.rng);
            let best = rng.below(count_best);
            let mut next = rng.below((n - 1).max(1));
            if next >= best {
                next += 1;
            }
            (best, next)
        };

        // When to send the second request, if the first is still out (`secondDelay`).
        let mut second_delay: Option<Pin<Box<dyn Future<Output = ()> + Send + '_>>> = None;

        // The model's choice: the replica with the fewest (smoothed, penalty-weighted)
        // outstanding requests, among the nearest unless too many of those are bad.
        {
            let now = now_secs(self.rt.now());
            let started = self.rt.now();
            let mut model = lock(&self.model);
            let mut best_metric = 1e9;
            let mut next_metric = 1e9;
            // The latency of the replica with the fewest outstanding requests, and of the
            // one chosen to be second.
            let mut best_time = 1e9;
            let mut next_time = 1e9;
            let mut bad_servers = 0;
            for (i, target) in targets.iter().enumerate() {
                if bad_servers < i.min(LOAD_BALANCE_MAX_BAD_OPTIONS + 1) && i == count_best {
                    break;
                }
                if self.is_failed(target) {
                    bad_servers += 1;
                    continue;
                }
                let qd = model.measurement(ids[i]);
                if now > qd.failed_until {
                    let metric = qd.smooth_outstanding.smooth_total(now);
                    let time = qd.latency;
                    if LOAD_BALANCE_PENALTY_IS_BAD && qd.penalty > 1.001 {
                        bad_servers += 1;
                    }
                    if metric < best_metric {
                        if i != best_alt {
                            next_alt = best_alt;
                            next_metric = best_metric;
                            next_time = best_time;
                        }
                        best_alt = i;
                        best_metric = metric;
                        best_time = time;
                    } else if metric < next_metric {
                        next_alt = i;
                        next_metric = metric;
                        next_time = time;
                    }
                } else {
                    bad_servers += 1;
                }
            }
            if next_metric > 1e8 {
                for (i, target) in targets.iter().enumerate().skip(count_best) {
                    if self.is_failed(target) {
                        continue;
                    }
                    let qd = model.measurement(ids[i]);
                    if now > qd.failed_until {
                        let metric = qd.smooth_outstanding.smooth_total(now);
                        let time = qd.latency;
                        if metric < next_metric {
                            next_alt = i;
                            next_metric = metric;
                            next_time = time;
                        }
                    }
                }
            }
            // Decide when to send the request to the second best choice.
            if let Some(delay) = second_delay_secs(best_time, next_time, model.second_multiplier) {
                second_delay = Some(if delay > 0.0 {
                    // At least `BASE_SECOND_REQUEST_TIME` (0.5 ms), so on a coarse timer it
                    // fires up to a tick late: a hedge, not a wait on the reply path.
                    Box::pin(self.rt.sleep_until(started + secs(delay)))
                } else {
                    // `delay(0)`: at once, not at the timer's next tick (tokio fires a
                    // deadline just past about 1 ms later).
                    Box::pin(std::future::ready(()))
                });
            }
        }

        let start_alt = next_alt;
        let start_distance = (best_alt + n - start_alt) % n;
        let mut num_attempts = 0;
        let mut backoff = 0.0;
        let mut tried_all = false;
        // `firstRequestData` (`isValid()` is `is_some()`), and the endpoint of the first
        // request, which no other request goes to.
        let mut first: Option<RequestData<R, W, Q>> = None;
        let mut first_endpoint: Option<u64> = None;
        loop {
            self.check_open()?;
            // Find an alternative that is not failed, and not the first request's.
            let mut chosen = None;
            for _ in 0..n {
                let alt = use_alt(next_alt, start_alt, best_alt, start_distance, n);
                if !self.is_failed(&targets[alt]) && first_endpoint != Some(ids[alt]) {
                    chosen = Some(alt);
                    break;
                }
                next_alt = (next_alt + 1) % n;
                if next_alt == start_alt {
                    tried_all = true;
                }
            }
            match (chosen, first.is_some()) {
                (None, false) => {
                    // Everything is down: wait for someone to be up.
                    self.all_alternatives_failed_delay(&targets).await?;
                    num_attempts = 0;
                }
                (None, true) => {
                    // Only the first location is available.
                    let sent = poll_fn(|cx| first.as_mut().expect("first").poll_response(cx)).await;
                    let tried = first.take().map_or(tried_all, |f| f.tried_all);
                    match process::<R, W, Q>(sent, tried) {
                        Outcome::Done(value) => return Ok(value),
                        Outcome::Fail(error) => return Err(error),
                        Outcome::Retry => {}
                    }
                    first_endpoint = None;
                }
                (Some(alt), true) => {
                    // Issue a second request, the first one is taking a long time.
                    // `secondRequestData`.
                    let mut second = Some(RequestData::start(
                        &this,
                        backoff,
                        tried_all,
                        targets[alt],
                        ids[alt],
                        request.clone(),
                    ));
                    loop {
                        let ready = poll_fn(|cx| {
                            if let Some(f) = first.as_mut() {
                                if let Poll::Ready(sent) = f.poll_response(cx) {
                                    return Poll::Ready(Either::Left(sent));
                                }
                            }
                            match second.as_mut().expect("second").poll_response(cx) {
                                Poll::Ready(sent) => Poll::Ready(Either::Right(sent)),
                                Poll::Pending => Poll::Pending,
                            }
                        })
                        .await;
                        match ready {
                            Either::Left(sent) => {
                                let tried = first.take().map_or(tried_all, |f| f.tried_all);
                                match process::<R, W, Q>(sent, tried) {
                                    Outcome::Done(value) => return Ok(value),
                                    Outcome::Fail(error) => return Err(error),
                                    Outcome::Retry => {}
                                }
                                first_endpoint = None;
                            }
                            Either::Right(sent) => {
                                let tried = second.take().map_or(tried_all, |f| f.tried_all);
                                match process::<R, W, Q>(sent, tried) {
                                    Outcome::Done(value) => return Ok(value),
                                    Outcome::Fail(error) => return Err(error),
                                    Outcome::Retry => break,
                                }
                            }
                        }
                    }
                    num_attempts += 1;
                    if num_attempts >= n {
                        backoff = grow_backoff(backoff);
                    }
                }
                (Some(alt), false) => {
                    // Issue a request; if it takes too long to get a reply, send a second
                    // one (budget allowing) or go around the loop.
                    first = Some(RequestData::start(
                        &this,
                        backoff,
                        tried_all,
                        targets[alt],
                        ids[alt],
                        request.clone(),
                    ));
                    first_endpoint = Some(ids[alt]);
                    loop {
                        let ready = poll_fn(|cx| {
                            if let Poll::Ready(sent) =
                                first.as_mut().expect("first").poll_response(cx)
                            {
                                return Poll::Ready(Either::Left(sent));
                            }
                            if let Some(delay) = second_delay.as_mut() {
                                if delay.as_mut().poll(cx).is_ready() {
                                    return Poll::Ready(Either::Right(()));
                                }
                            }
                            Poll::Pending
                        })
                        .await;
                        match ready {
                            Either::Left(sent) => {
                                lock(&self.model).first_request_replied();
                                let tried = first.take().map_or(tried_all, |f| f.tried_all);
                                match process::<R, W, Q>(sent, tried) {
                                    Outcome::Done(value) => return Ok(value),
                                    Outcome::Fail(error) => return Err(error),
                                    Outcome::Retry => {}
                                }
                                first_endpoint = None;
                                break;
                            }
                            Either::Right(()) => {
                                second_delay = None;
                                if lock(&self.model).try_second_request() {
                                    break;
                                }
                            }
                        }
                    }
                    num_attempts += 1;
                    if num_attempts >= n {
                        backoff = grow_backoff(backoff);
                    }
                }
            }
            next_alt = (next_alt + 1) % n;
            // With one alternative `next_alt` is 0 from here on, never `start_alt` (1): as in
            // C++, `tried_all` stays false, so `process_behind` is retried (with backoff)
            // rather than returned.
            if next_alt == start_alt {
                tried_all = true;
            }
            second_delay = None;
        }
    }
}

/// A reply, or why there is none, with the model entry its request was counted in.
type Sent<R, W> = (
    ModelHolder<R, W>,
    Result<(SchemaVersion, Vec<u8>), RequestError>,
);

/// A request in flight: resolves with its reply once it is done.
type Response<R, W> = Pin<Box<dyn Future<Output = Sent<R, W>> + Send>>;

/// `RequestData`: a request `load_balance` has started (after its backoff, if any), and what
/// its reply has to update. If it is dropped before its reply was processed (the other
/// request won, or `load_balance` returned or was cancelled), it goes on as a lagging
/// request (`makeLaggingRequest`): a task awaits the reply and updates the queue model, so
/// that a slow server's latency and penalty are learned, and its outstanding request counts
/// until it ends. Dropping one that has not been sent yet (still in its backoff) is
/// harmless.
struct RequestData<R: Runtime, W: StreamWrapper<R::Stream>, Q: LoadBalanced> {
    shared: Arc<Shared<R, W>>,
    /// The reply, once the request is done. `None` once it was taken.
    response: Option<Response<R, W>>,
    /// Whether the request was sent (`requestStarted`).
    started: Arc<AtomicBool>,
    /// `triedAllOptions` when the request was started.
    tried_all: bool,
    request: PhantomData<fn() -> Q>,
}

impl<R: Runtime, W: StreamWrapper<R::Stream>, Q: LoadBalanced> RequestData<R, W, Q> {
    /// `startRequest`: sends `request` to `target` after `backoff` seconds, counting it in
    /// the queue model as `id`.
    fn start(
        shared: &Arc<Shared<R, W>>,
        backoff: f64,
        tried_all: bool,
        target: (NetworkAddress, Uid),
        id: u64,
        request: Q,
    ) -> Self {
        let started = Arc::new(AtomicBool::new(false));
        let response: Response<R, W> = {
            let shared = shared.clone();
            let started = started.clone();
            Box::pin(async move {
                if backoff > 0.0 {
                    shared.rt.sleep(secs(backoff)).await;
                }
                started.store(true, Ordering::Release);
                let holder = ModelHolder::new(shared.clone(), id);
                let (address, token) = target;
                let result = try_get_reply(&shared.peers, &address, token, move |s, r, out| {
                    request.encode_request_into(s, r, out)
                })
                .await;
                (holder, result)
            })
        };
        RequestData {
            shared: shared.clone(),
            response: Some(response),
            started,
            tried_all,
            request: PhantomData,
        }
    }

    /// Polls for the reply; pending for good once it was taken.
    fn poll_response(&mut self, cx: &mut Context<'_>) -> Poll<Sent<R, W>> {
        let Some(response) = self.response.as_mut() else {
            return Poll::Pending;
        };
        match response.as_mut().poll(cx) {
            Poll::Ready(sent) => {
                self.response = None;
                Poll::Ready(sent)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<R: Runtime, W: StreamWrapper<R::Stream>, Q: LoadBalanced> Drop for RequestData<R, W, Q> {
    fn drop(&mut self) {
        let Some(response) = self.response.take() else {
            return;
        };
        if !self.started.load(Ordering::Acquire) {
            return;
        }
        let tried_all = self.tried_all;
        let task = self.shared.rt.spawn(async move {
            let (mut holder, result) = response.await;
            // Only what the model learns from it matters: the reply is not used.
            let _ = check_and_process::<R, W, Q>(result, &mut holder, tried_all, false);
        });
        self.shared.add_lagging(task);
    }
}

impl<R: Runtime, W> Shared<R, W> {
    /// Keeps the task of a lagging request (`laggingRequests`, an `actorCollection`) so that it
    /// can be cancelled with the reader. As C++ does, past
    /// `MAX_LAGGING_REQUESTS_OUTSTANDING` it cancels them all and starts over.
    fn add_lagging(&self, task: R::Task<()>) {
        lock(&self.lagging).add(task, MAX_LAGGING_REQUESTS_OUTSTANDING);
    }

    /// Cancels the lagging requests (the reader ended).
    pub(super) fn abort_lagging(&self) {
        lock(&self.lagging).abort_all();
    }

    /// What the queue model and the lagging requests hold, for tests.
    pub(super) fn load_balance_stats(&self) -> LoadBalanceStats {
        LoadBalanceStats {
            endpoints: lock(&self.model).stats(),
            lagging_requests: lock(&self.lagging).live(),
        }
    }
}

/// The lagging requests' tasks.
pub(crate) struct Lagging<R: Runtime> {
    tasks: Vec<R::Task<()>>,
    /// The size at which finished tasks are dropped from `tasks`.
    prune_at: usize,
}

/// The size of the task list at which finished tasks are first dropped from it.
const LAGGING_PRUNE_AT: usize = 64;

impl<R: Runtime> Default for Lagging<R> {
    fn default() -> Self {
        Lagging {
            tasks: Vec::new(),
            prune_at: LAGGING_PRUNE_AT,
        }
    }
}

impl<R: Runtime> Lagging<R> {
    /// Keeps `task`. Past `limit` requests that have not ended, cancels them all first (as
    /// C++ does past `MAX_LAGGING_REQUESTS_OUTSTANDING`): the bound counts the live
    /// requests, not the finished ones the list has not dropped yet.
    pub(crate) fn add(&mut self, task: R::Task<()>, limit: usize) {
        if self.tasks.len() >= self.prune_at || self.tasks.len() > limit {
            self.tasks.retain(|t| !t.is_finished());
            self.prune_at = (2 * self.tasks.len()).max(LAGGING_PRUNE_AT);
        }
        if self.tasks.len() > limit {
            self.abort_all();
        }
        self.tasks.push(task);
    }

    /// Cancels every lagging request and starts over.
    pub(crate) fn abort_all(&mut self) {
        for task in self.tasks.drain(..) {
            task.abort();
        }
        self.prune_at = LAGGING_PRUNE_AT;
    }

    /// How many requests have not ended.
    pub(crate) fn live(&self) -> usize {
        self.tasks.iter().filter(|t| !t.is_finished()).count()
    }
}

impl<R: Runtime> std::fmt::Debug for Lagging<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lagging")
            .field("tasks", &self.tasks.len())
            .finish()
    }
}

/// [`check_and_process`] of a reply that arrived in `load_balance`.
fn process<R: Runtime, W, Q: LoadBalanced>(
    sent: Sent<R, W>,
    tried_all: bool,
) -> Outcome<Q::Output> {
    let (mut holder, result) = sent;
    check_and_process::<R, W, Q>(result, &mut holder, tried_all, true)
}

/// A request [`Shared::load_balance`] sends to one stream of a shard's storage servers, whose
/// reply is a `LoadBalancedReply`: a penalty and an error besides its result.
pub(crate) trait LoadBalanced: Request + Clone + Send + 'static {
    /// What a successful reply gives.
    type Output;
    /// The stream (`StorageServerInterface`'s adjusted-endpoint index).
    const STREAM: u32;
    /// The reply's penalty, error code and result; the result (out of the reply, which
    /// borrows from `payload`) only if `keep` (a lagging request's reply is only learned
    /// from).
    fn split(
        reply: Self::Reply<'_>,
        payload: &Bytes,
        keep: bool,
    ) -> LoadBalancedReply<Self::Output>;
}

impl LoadBalanced for GetValue {
    type Output = Option<Vec<u8>>;
    const STREAM: u32 = STORAGE_SERVER_STREAMS.get_value;

    fn split(
        reply: GetValueReply<'_>,
        _: &Bytes,
        keep: bool,
    ) -> LoadBalancedReply<Option<Vec<u8>>> {
        (
            reply.penalty,
            reply.error.map(|e| e.code),
            keep.then(|| reply.value.map(<[u8]>::to_vec)),
        )
    }
}

impl LoadBalanced for GetKey {
    type Output = KeySelector;
    const STREAM: u32 = STORAGE_SERVER_STREAMS.get_key;

    fn split(reply: GetKeyReply<'_>, _: &Bytes, keep: bool) -> LoadBalancedReply<KeySelector> {
        (
            reply.penalty,
            reply.error.map(|e| e.code),
            keep.then(|| reply.sel.into_owned()),
        )
    }
}

/// A decoded `LoadBalancedReply`: its penalty, its error code, and its result (`None` if it
/// was not asked for).
pub(crate) type LoadBalancedReply<T> = (f64, Option<u16>, Option<T>);

enum Outcome<T> {
    Done(T),
    Retry,
    Fail(Error),
}

/// `RequestData::checkAndProcessResultImpl` (`LoadBalance.actor.h` 7.4:572-627,
/// `LoadBalance.h` 8.0:175-229) for a `LoadBalancedReply`, not `AtMostOnce`. `keep` is
/// whether the reply's result is wanted: a lagging request's is not (its outcome is `Retry`,
/// which nothing reads), so it is not copied out of the reply.
fn check_and_process<R: Runtime, W, Q: LoadBalanced>(
    result: Result<(SchemaVersion, Vec<u8>), RequestError>,
    holder: &mut ModelHolder<R, W>,
    tried_all: bool,
    keep: bool,
) -> Outcome<Q::Output> {
    // The reply decoded: `Ok` is a `LoadBalancedReply` (penalty, error, result).
    let decoded: Result<LoadBalancedReply<Q::Output>, RequestError> = match result {
        Ok((schema, payload)) => {
            // A reply's result may keep slices of its buffer (a range read's rows).
            let payload = Bytes::from(payload);
            match Q::decode_reply(schema, &payload) {
                Ok(Ok(reply)) => Ok(Q::split(reply, &payload, keep)),
                Ok(Err(e)) => Err(RequestError::from_reply_error(e)),
                Err(e) => Err(RequestError::Decode(e)),
            }
        }
        Err(e) => Err(e),
    };
    let err_code = match &decoded {
        Ok((_, error, _)) => error.unwrap_or(SUCCESS),
        Err(e) => try_get_reply_code(e),
    };
    let maybe_delivered = err_code == BROKEN_PROMISE || err_code == REQUEST_MAYBE_DELIVERED;
    let received_response =
        matches!(&decoded, Ok((_, None, _))) || (!maybe_delivered && err_code != PROCESS_BEHIND);
    let future_version = err_code == FUTURE_VERSION || err_code == PROCESS_BEHIND;
    let penalty = match &decoded {
        Ok((penalty, _, _)) => *penalty,
        Err(_) => -1.0,
    };
    holder.release(received_response, future_version, penalty, true);

    if err_code == SERVER_OVERLOADED {
        return Outcome::Retry;
    }
    match decoded {
        Ok((_, None, Some(value))) => Outcome::Done(value),
        // Only when the result was not asked for.
        Ok((_, None, None)) => Outcome::Retry,
        Ok((_, Some(code), _)) if received_response => Outcome::Fail(Error::new(code)),
        Err(e) if received_response => Outcome::Fail(client_error(e)),
        _ if tried_all && err_code == PROCESS_BEHIND => Outcome::Fail(Error::new(PROCESS_BEHIND)),
        _ => Outcome::Retry,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fdb_native_transport::runtime::TokioRuntime;

    #[test]
    fn smoother_follows_the_total() {
        let mut s = Smoother::new(2.0);
        s.add_delta(5.0, 0.0);
        assert_eq!(s.smooth_total(0.0), 0.0);
        let after = s.smooth_total(2.0);
        assert!((after - 5.0 * (1.0 - (-1.0f64).exp())).abs() < 1e-9);
        assert!(s.smooth_total(100.0) > 4.99);
    }

    #[test]
    fn queue_model_penalty_and_future_version() {
        let mut m = QueueModel::default();
        let d = m.add_request(1, 0.0);
        assert_eq!(d, 1.0);
        m.end_request(1, 0.01, 4.0, d, true, false, 0.01);
        assert_eq!(m.measurement(1).penalty, 4.0);
        assert_eq!(m.add_request(1, 1.0), 4.0);
        m.end_request(1, 0.01, -1.0, 4.0, true, true, 1.0);
        let qd = m.measurement(1);
        assert_eq!(qd.penalty, 4.0, "no penalty in the reply keeps the old one");
        assert_eq!(qd.failed_until, 1.0 + 2.0);
        // A clean reply resets the future-version backoff.
        m.end_request(1, 0.01, 1.0, 0.0, true, false, 5.0);
        assert_eq!(m.measurement(1).future_version_backoff, 1.0);
    }

    #[test]
    fn second_delay_follows_the_latencies() {
        // No second choice: never.
        assert_eq!(second_delay_secs(0.001, 1e9, 1.0), None);
        // Comparable latencies: the second one's latency plus the base time.
        let d = second_delay_secs(0.001, 0.001, 1.0).unwrap();
        assert!((d - 0.0015).abs() < 1e-12, "{d}");
        // The multiplier stretches it.
        let d = second_delay_secs(0.001, 0.001, 2.0).unwrap();
        assert!((d - 0.0025).abs() < 1e-12, "{d}");
        // A first choice more than twice as slow as that is not waited for.
        assert_eq!(second_delay_secs(0.01, 0.001, 1.0), Some(0.0));
        // Not quite twice as slow is still waited for (`>`).
        assert!(second_delay_secs(0.003, 0.001, 1.0).unwrap() > 0.0);
    }

    #[test]
    fn second_requests_spend_the_budget() {
        let mut m = QueueModel::default();
        assert_eq!(m.second_multiplier, 1.0);
        // No budget at first.
        assert!(!m.try_second_request());
        assert_eq!(m.second_multiplier, 1.0);
        // 20 replies earn one request.
        for _ in 0..19 {
            m.first_request_replied();
        }
        assert!(!m.try_second_request(), "{}", m.second_budget);
        m.first_request_replied();
        assert!(m.try_second_request());
        assert!((m.second_multiplier - 1.01).abs() < 1e-12);
        assert!(m.second_budget < 1e-9);
        // Replies decay the multiplier toward 1, never below.
        for _ in 0..20 {
            m.first_request_replied();
        }
        assert!((m.second_multiplier - (1.01 - 20.0 * 0.00025)).abs() < 1e-9);
        for _ in 0..1000 {
            m.first_request_replied();
        }
        assert_eq!(m.second_multiplier, 1.0);
        // The budget is capped.
        for _ in 0..10_000 {
            m.first_request_replied();
        }
        assert_eq!(m.second_budget, 100.0);
    }

    #[test]
    fn proxy_model_follows_busy_time_like_cpp() {
        // The numbers of `/fdbclient/NativeAPI/proxyLoadBalanceMetrics` (8.0), GRV model.
        let precision = BASIC_LOAD_BALANCE_COMPUTE_PRECISION;
        let mut m = ProxyModel::new(2, 0.0);
        assert_eq!((m.probability(0), m.probability(1)), (0.5, 0.5));
        let update = |m: &mut ProxyModel, first: i32, second: i32| {
            m.update_recent(0, first, 1.0);
            m.update_recent(1, second, 1.0);
            m.update_probabilities(1.0)
        };
        assert!(update(&mut m, 80 * precision + 1000, 20 * precision + 9000));
        assert!((m.probability(0) - 0.47).abs() < 1e-12);
        assert!((m.probability(1) - 0.53).abs() < 1e-12);
        // Too little load (39 < 20 per proxy): unchanged.
        assert!(!update(
            &mut m,
            19 * precision + 9000,
            20 * precision + 1000
        ));
        assert!((m.probability(0) - 0.47).abs() < 1e-12);
        // Only the quotient counts: the CPU part is ignored.
        assert!(update(&mut m, 40 * precision, 0));
        assert!((m.probability(0) - 0.42).abs() < 1e-12);
        // Selection by cumulative probability.
        assert_eq!(m.get_best(0.0, 1.0), 0);
        assert_eq!(m.get_best(0.41, 1.0), 0);
        assert_eq!(m.get_best(0.43, 1.0), 1);
        assert_eq!(m.get_best(0.999, 1.0), 1);
    }

    #[test]
    fn proxy_model_updates_every_period_and_only_from_fresh_reports() {
        let precision = BASIC_LOAD_BALANCE_COMPUTE_PRECISION;
        let mut m = ProxyModel::new(2, 0.0);
        m.update_recent(0, 80 * precision, 8.0);
        m.update_recent(1, 20 * precision, 8.0);
        // Before the first period nothing moved.
        m.get_best(0.5, 9.0);
        assert_eq!(m.probability(0), 0.5);
        // The tick at 10 s saw reports 2 s old.
        m.get_best(0.5, 12.0);
        assert!((m.probability(0) - 0.47).abs() < 1e-12);
        // The tick at 20 s saw reports 12 s old, and so did every later one.
        m.get_best(0.5, 1000.0);
        assert!((m.probability(0) - 0.47).abs() < 1e-12);
        // A proxy that has not reported yet holds the update back.
        let mut m = ProxyModel::new(3, 0.0);
        m.update_recent(0, 80 * precision, 9.0);
        m.update_recent(1, 20 * precision, 9.0);
        m.get_best(0.5, 10.5);
        assert_eq!(m.probability(0), 1.0 / 3.0);
    }

    #[test]
    fn grv_model_is_replaced_with_the_proxies() {
        let mut g = GrvModel::default();
        let key = (Uid::new(1, 1), vec![Uid::new(2, 2), Uid::new(3, 3)]);
        let a = g.get(key.clone(), 0.0);
        assert!(Arc::ptr_eq(&a, &g.get(key.clone(), 5.0)));
        let other = (Uid::new(1, 2), key.1.clone());
        let b = g.get(other, 6.0);
        assert!(!Arc::ptr_eq(&a, &b));
        assert_eq!(b.lock().unwrap().len(), 2);
    }

    #[test]
    fn an_endpoint_with_outstanding_requests_survives_pruning() {
        let mut m = QueueModel::default();
        let a = m.add_request(1, 0.0);
        let b = m.add_request(1, 0.0);
        m.add_request(2, 0.0);
        let d2 = m.measurement(2).penalty;
        m.end_request(2, 0.01, 1.0, d2, true, false, 0.01);
        // Nothing is live: the endpoint without requests goes, the other stays.
        m.retain(|_| false);
        assert_eq!(m.len(), 1);
        // Its requests end, after the prune, with nothing driving the count below zero.
        m.end_request(1, 0.5, 1.0, a, true, false, 1.0);
        assert_eq!(m.stats()[0].outstanding, 1);
        m.retain(|_| false);
        assert_eq!(m.len(), 1, "one request is still out");
        m.end_request(1, 0.5, 1.0, b, true, false, 2.0);
        let stats = m.stats();
        assert_eq!((stats[0].outstanding, stats[0].outstanding_total), (0, 0.0));
        assert_eq!(stats[0].latency, 0.5);
        m.retain(|_| false);
        assert_eq!(m.len(), 0);
    }

    #[test]
    fn ending_a_request_of_a_forgotten_endpoint_creates_nothing() {
        let mut m = QueueModel::default();
        m.end_request(7, 0.5, 4.0, 1.0, true, true, 1.0);
        assert_eq!(m.len(), 0);
        // A new entry of the same endpoint is not driven negative by the old request.
        m.add_request(7, 2.0);
        assert_eq!(m.stats()[0].outstanding_total, 1.0);
    }

    #[test]
    fn an_abandoned_request_is_released_without_latency_or_penalty() {
        // What `~ModelHolder` does: `release(false, false, -1.0, false)`.
        let mut m = QueueModel::default();
        let delta = m.add_request(1, 0.0);
        m.end_request(1, 0.0, -1.0, delta, false, false, 4.0);
        let stats = m.stats();
        assert_eq!(
            stats[0].latency, 0.001,
            "an unclean end keeps the larger latency"
        );
        assert_eq!(stats[0].penalty, 1.0);
        assert_eq!((stats[0].outstanding, stats[0].outstanding_total), (0, 0.0));
        // The smoother was brought up to the end's time: 4 s with 1 outstanding.
        let smooth = m.measurement(1).smooth_outstanding.smooth_total(4.0);
        assert!((smooth - (1.0 - (-2.0f64).exp())).abs() < 1e-9, "{smooth}");
    }

    /// A task that never ends on its own, and says when it was dropped.
    struct DropFlag(Arc<AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn pending_task(rt: &TokioRuntime) -> (<TokioRuntime as Runtime>::Task<()>, Arc<AtomicBool>) {
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = DropFlag(dropped.clone());
        let task = rt.spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });
        (task, dropped)
    }

    async fn settle() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn the_lagging_bound_counts_live_tasks() {
        let rt = TokioRuntime::new();
        let mut lagging = Lagging::<TokioRuntime>::default();
        // Four that never end, then five that do: nine listed, past the bound of eight, but
        // only once the five have finished are they not live.
        let mut flags = Vec::new();
        for _ in 0..4 {
            let (task, dropped) = pending_task(&rt);
            flags.push(dropped);
            lagging.add(task, 8);
        }
        for _ in 0..5 {
            lagging.add(rt.spawn(async {}), 8);
        }
        settle().await;
        assert_eq!(lagging.tasks.len(), 9);
        assert_eq!(lagging.live(), 4);
        // The next one finds the list past the bound: it drops the finished ones, and the
        // live ones are within the bound, so none is cancelled.
        let (task, dropped) = pending_task(&rt);
        flags.push(dropped);
        lagging.add(task, 8);
        settle().await;
        assert_eq!(lagging.tasks.len(), 5);
        assert_eq!(lagging.live(), 5);
        assert!(flags.iter().all(|d| !d.load(Ordering::SeqCst)));

        // Past the bound of live tasks, all are cancelled and it starts over.
        for _ in 0..5 {
            let (task, dropped) = pending_task(&rt);
            flags.push(dropped);
            lagging.add(task, 8);
        }
        settle().await;
        let cancelled = flags.iter().filter(|d| d.load(Ordering::SeqCst)).count();
        assert_eq!(cancelled, 9, "the nine that were live");
        assert_eq!(lagging.live(), 1);
    }

    #[tokio::test]
    async fn aborting_the_lagging_requests_ends_them() {
        let rt = TokioRuntime::new();
        let mut lagging = Lagging::<TokioRuntime>::default();
        let flags: Vec<_> = (0..3)
            .map(|_| {
                let (task, dropped) = pending_task(&rt);
                lagging.add(task, 100);
                dropped
            })
            .collect();
        settle().await;
        assert_eq!(lagging.live(), 3);
        assert!(flags.iter().all(|d| !d.load(Ordering::SeqCst)));
        lagging.abort_all();
        settle().await;
        assert_eq!(lagging.live(), 0);
        assert!(flags.iter().all(|d| d.load(Ordering::SeqCst)));
    }

    #[test]
    fn alternatives_order_starts_with_best() {
        // n = 3, best 1, next (start) 0: 1, then 0's successors skipping 1, then 0.
        let (best, start, n) = (1, 0, 3);
        let dist = (best + n - start) % n;
        let order: Vec<usize> = (0..n)
            .map(|k| use_alt((start + k) % n, start, best, dist, n))
            .collect();
        assert_eq!(order, vec![1, 0, 2]);
        // A single alternative: `next` starts out of range at 1.
        assert_eq!(use_alt(1, 1, 0, 0, 1), 0);
    }
}
