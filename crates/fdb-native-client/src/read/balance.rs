//! The load balancers: `basicLoadBalance` (proxies) and a simplified `loadBalance` with its
//! `QueueModel` (storage servers). See the [module documentation](super) for what is ported
//! and what is left out.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use fdb_native_schema::domain::GetValue;
use fdb_native_schema::{Request, SchemaVersion};
use fdb_native_transport::peer::StreamWrapper;
use fdb_native_transport::rpc::RequestError;
use fdb_native_transport::runtime::Runtime;
use fdb_native_wire::{DecodeError, EncodeError, Endpoint, ErrorOr, NetworkAddress, Uid};

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
            latency: 0.001,
            penalty: 1.0,
            failed_until: 0.0,
            future_version_backoff: FUTURE_VERSION_INITIAL_BACKOFF,
            increase_backoff_time: 0.0,
        }
    }
}

/// `QueueModel` (`fdbrpc/QueueModel.cpp`), keyed by the endpoint token's `first`, without
/// the second-request budget.
#[derive(Debug, Default)]
pub(crate) struct QueueModel {
    data: HashMap<u64, QueueData>,
}

impl QueueModel {
    /// `addRequest`: counts a request as the server's penalty; returns it (the `delta`).
    pub(crate) fn add_request(&mut self, id: u64, now: f64) -> f64 {
        let d = self.data.entry(id).or_default();
        let penalty = d.penalty;
        d.smooth_outstanding.add_delta(penalty, now);
        penalty
    }

    /// `endRequest`.
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
        let d = self.data.entry(id).or_default();
        d.smooth_outstanding.add_delta(-delta, now);
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

    /// Keeps the data of the endpoints `keep` accepts.
    pub(crate) fn retain(&mut self, mut keep: impl FnMut(u64) -> bool) {
        self.data.retain(|id, _| keep(*id));
    }

    /// `getMeasurement`.
    pub(crate) fn measurement(&mut self, id: u64) -> &mut QueueData {
        self.data.entry(id).or_default()
    }
}

/// `ModelHolder`: a request counted in the model, released once with its outcome, or as
/// unclean (with no latency and no penalty) if the request is abandoned.
struct ModelHolder<'a, R> {
    model: &'a Mutex<QueueModel>,
    rt: &'a R,
    id: u64,
    delta: f64,
    start: f64,
    released: bool,
}

impl<'a, R: Runtime> ModelHolder<'a, R> {
    fn new(model: &'a Mutex<QueueModel>, rt: &'a R, id: u64) -> Self {
        let start = now_secs(rt.now());
        let delta = lock(model).add_request(id, start);
        ModelHolder {
            model,
            rt,
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
        let now = now_secs(self.rt.now());
        let latency = if clean || measure {
            now - self.start
        } else {
            0.0
        };
        lock(self.model).end_request(
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

impl<R> Drop for ModelHolder<'_, R> {
    fn drop(&mut self) {
        if !self.released {
            // `~ModelHolder`: unclean, no latency, no penalty. Time is not needed for that,
            // except to update the smoother, which the next use does anyway.
            let mut model = lock(self.model);
            let d = model.data.entry(self.id).or_default();
            let time = d.smooth_outstanding.time;
            d.smooth_outstanding.add_delta(-self.delta, time);
        }
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
    pub(crate) async fn basic_load_balance<T, E, D>(
        &self,
        alternatives: &[Endpoint],
        encode: E,
        decode: D,
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
        let (best_alt, mut next_alt) = {
            let mut rng = lock(&self.rng);
            // `ModelInterface::getBest` picks by probabilities that start uniform and are
            // adjusted from the proxies' `processBusyTime`; here they stay uniform.
            let best = rng.below(n);
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
                    let error =
                        match try_get_reply(&self.peers, &address, token, encode.clone()).await {
                            Ok((schema, payload)) => match decode(schema, &payload) {
                                Ok(Ok(reply)) => return Ok(reply),
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
    async fn all_alternatives_failed_delay(
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

    /// `loadBalance` (`LoadBalance.actor.h` 7.4:686-1022, `LoadBalance.h` 8.0:307-620) of a
    /// `GetValueRequest` over a shard's replicas, with the queue model and without second
    /// (speculative) requests. Returns the value, or the error to surface (`wrong_shard_server`,
    /// `all_alternatives_failed`, `future_version`, `transaction_too_old`, ...).
    pub(crate) async fn load_balance_get_value(
        &self,
        location: &Location,
        request: GetValue,
    ) -> Result<Option<Vec<u8>>, Error> {
        let alternatives = &location.servers;
        let n = alternatives.len();
        if n == 0 {
            // An empty team (C++ asserts): nothing to ask until the location is refreshed.
            return Err(Error::new(ALL_ALTERNATIVES_FAILED));
        }
        let targets: Vec<(NetworkAddress, Uid)> = alternatives.iter().map(|s| s.target()).collect();
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

        // The model's choice: the replica with the fewest (smoothed, penalty-weighted)
        // outstanding requests, among the nearest unless too many of those are bad.
        {
            let now = now_secs(self.rt.now());
            let mut model = lock(&self.model);
            let mut best_metric = 1e9;
            let mut next_metric = 1e9;
            let mut bad_servers = 0;
            for (i, target) in targets.iter().enumerate() {
                if bad_servers < i.min(LOAD_BALANCE_MAX_BAD_OPTIONS + 1) && i == count_best {
                    break;
                }
                if self.is_failed(target) {
                    bad_servers += 1;
                    continue;
                }
                let qd = model.measurement(target.1.first);
                if now > qd.failed_until {
                    let metric = qd.smooth_outstanding.smooth_total(now);
                    if LOAD_BALANCE_PENALTY_IS_BAD && qd.penalty > 1.001 {
                        bad_servers += 1;
                    }
                    if metric < best_metric {
                        if i != best_alt {
                            next_alt = best_alt;
                            next_metric = best_metric;
                        }
                        best_alt = i;
                        best_metric = metric;
                    } else if metric < next_metric {
                        next_alt = i;
                        next_metric = metric;
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
                    let qd = model.measurement(target.1.first);
                    if now > qd.failed_until {
                        let metric = qd.smooth_outstanding.smooth_total(now);
                        if metric < next_metric {
                            next_alt = i;
                            next_metric = metric;
                        }
                    }
                }
            }
        }

        let start_alt = next_alt;
        let start_distance = (best_alt + n - start_alt) % n;
        let mut num_attempts = 0;
        let mut backoff = 0.0;
        let mut tried_all = false;
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
                if next_alt == start_alt {
                    tried_all = true;
                }
            }
            match chosen {
                None => {
                    self.all_alternatives_failed_delay(&targets).await?;
                    num_attempts = 0;
                }
                Some(alt) => {
                    if backoff > 0.0 {
                        self.rt.sleep(secs(backoff)).await;
                    }
                    let (address, token) = targets[alt];
                    let mut holder = ModelHolder::new(&self.model, &self.rt, token.first);
                    let request = request.clone();
                    let result = try_get_reply(&self.peers, &address, token, move |s, r, out| {
                        request.encode_request_into(s, r, out)
                    })
                    .await;
                    match check_and_process(result, &mut holder, tried_all) {
                        Outcome::Done(value) => return Ok(value),
                        Outcome::Fail(error) => return Err(error),
                        Outcome::Retry => {}
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
        }
    }
}

/// A decoded `GetValueReply`: its penalty, its error code, and its value.
type LoadBalancedReply = (f64, Option<u16>, Option<Vec<u8>>);

enum Outcome {
    Done(Option<Vec<u8>>),
    Retry,
    Fail(Error),
}

/// `RequestData::checkAndProcessResultImpl` (`LoadBalance.actor.h` 7.4:572-627,
/// `LoadBalance.h` 8.0:175-229) for a `GetValueReply` (a `LoadBalancedReply`), not
/// `AtMostOnce`.
fn check_and_process<R: Runtime>(
    result: Result<(SchemaVersion, Vec<u8>), RequestError>,
    holder: &mut ModelHolder<'_, R>,
    tried_all: bool,
) -> Outcome {
    // The reply decoded: `Ok` is a `LoadBalancedReply` (penalty, error, value).
    let decoded: Result<LoadBalancedReply, RequestError> = match result {
        Ok((schema, payload)) => match GetValue::decode_reply(schema, &payload) {
            Ok(Ok(reply)) => Ok((
                reply.penalty,
                reply.error.map(|e| e.code),
                reply.value.map(<[u8]>::to_vec),
            )),
            Ok(Err(e)) => Err(RequestError::from_reply_error(e)),
            Err(e) => Err(RequestError::Decode(e)),
        },
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
        Ok((_, None, value)) => Outcome::Done(value),
        Ok((_, Some(code), _)) if received_response => Outcome::Fail(Error::new(code)),
        Err(e) if received_response => Outcome::Fail(client_error(e)),
        _ if tried_all && err_code == PROCESS_BEHIND => Outcome::Fail(Error::new(PROCESS_BEHIND)),
        _ => Outcome::Retry,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
