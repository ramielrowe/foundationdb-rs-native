//! Read versions: `getConsistentReadVersion` and `readVersionBatcher`.

use std::collections::BTreeMap;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use fdb_native_schema::Request;
use fdb_native_schema::domain::{GetReadVersion, INVALID_VERSION, ReadVersion};
use fdb_native_transport::peer::StreamWrapper;
use fdb_native_transport::runtime::Runtime;
use fdb_native_wire::Endpoint;
use futures_channel::{mpsc, oneshot};
use futures_core::Stream;

use super::Shared;
use super::util::{Either, never, now_secs, race, secs};
use crate::error::ClientError;
use crate::error::codes::{INTERNAL_ERROR, OPERATION_CANCELLED};

/// A read version as the batcher hands it to every transaction of a batch.
pub(crate) type GrvResult = Result<Arc<ReadVersion>, ClientError>;

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
    ) -> Result<ReadVersion, ClientError> {
        let use_provisional = flags & GetReadVersion::FLAG_USE_PROVISIONAL_PROXIES != 0;
        loop {
            self.check_open()?;
            let info = self
                .monitor
                .grv_proxies(use_provisional)
                .await
                .map_err(|_| ClientError::new(OPERATION_CANCELLED))?;
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
                max_grv_queue_delay_ms: None,
            };
            let reply = self.basic_load_balance(
                &alternatives,
                move |schema, reply, out| request.encode_request_into(schema, reply, out),
                GetReadVersion::decode_reply,
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
                Either::Left(Ok(_)) => return Err(ClientError::new(INTERNAL_ERROR)),
                Either::Left(Err(e)) => return Err(e),
                Either::Right(()) => continue,
            }
        }
    }
}

/// One `readVersionBatcher` (NativeAPI 7.4:7671-7761, 8.0:5383-5482): collects the read
/// version requests of one set of flags and sends them as batches.
pub(crate) struct Batcher<R: Runtime> {
    pub(crate) sender: mpsc::UnboundedSender<Waiter>,
    pub(crate) task: R::Task<()>,
}

impl<R: Runtime> Batcher<R> {
    pub(crate) fn start<W: StreamWrapper<R::Stream>>(
        shared: Arc<Shared<R, W>>,
        flags: u32,
    ) -> Batcher<R> {
        let (sender, receiver) = mpsc::unbounded();
        let rt = shared.rt.clone();
        let task = rt.spawn(run_batcher(shared, flags, receiver));
        Batcher { sender, task }
    }
}

type InFlight = Pin<Box<dyn Future<Output = f64> + Send>>;

/// The batcher's loop. A batch is sent when it reaches `max_batch_size`, or `batch_time`
/// after its first request; `batch_time` follows half the reply latency, smoothed
/// (`0.1 * target + 0.9 * batch_time`) and capped at `GRV_BATCH_TIMEOUT`. Batches in flight
/// are driven by this task, so they end with it.
async fn run_batcher<R: Runtime, W: StreamWrapper<R::Stream>>(
    shared: Arc<Shared<R, W>>,
    flags: u32,
    mut receiver: mpsc::UnboundedReceiver<Waiter>,
) {
    let max_batch = shared.config.max_batch_size.max(1);
    let max_batch_time = shared.config.grv_batch_timeout.as_secs_f64();
    let mut batch_time = 0.0_f64;
    let mut pending: Vec<Waiter> = Vec::new();
    let mut timer: Option<Pin<Box<dyn Future<Output = ()> + Send>>> = None;
    let mut in_flight: Vec<InFlight> = Vec::new();
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
                            timer = Some(Box::pin(shared.rt.sleep(secs(batch_time))));
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
                } else if t.as_mut().poll(cx).is_ready() {
                    send = true;
                }
            }
            if send && !pending.is_empty() {
                progress = true;
                timer = None;
                let waiters = std::mem::take(&mut pending);
                in_flight.push(Box::pin(send_batch(shared.clone(), flags, waiters)));
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
    flags: u32,
    waiters: Vec<Waiter>,
) -> f64 {
    let start = now_secs(shared.rt.now());
    let count = u32::try_from(waiters.len()).unwrap_or(u32::MAX);
    let result = shared
        .get_consistent_read_version(count, flags)
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
    use super::*;

    #[test]
    fn flags_like_cpp() {
        let default = ReadVersionOptions::default();
        assert_eq!(default.flags(), GetReadVersion::PRIORITY_DEFAULT);
        let options = ReadVersionOptions {
            priority: Priority::Batch,
            causal_read_risky: true,
            use_provisional_proxies: true,
            lock_aware: false,
        };
        assert_eq!(
            options.flags(),
            GetReadVersion::PRIORITY_BATCH
                | GetReadVersion::FLAG_CAUSAL_READ_RISKY
                | GetReadVersion::FLAG_USE_PROVISIONAL_PROXIES
        );
    }
}
