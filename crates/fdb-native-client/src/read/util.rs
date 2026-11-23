//! Small executor-neutral helpers: a seeded RNG, racing futures, time in seconds, and the
//! transport's request primitive with the schema set of the connection.

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use fdb_native_schema::SchemaVersion;
use fdb_native_transport::peer::{FailureMonitor, PeerManager, StreamWrapper};
use fdb_native_transport::rpc::RequestError;
use fdb_native_transport::runtime::{Instant, Runtime};
use fdb_native_wire::{EncodeError, NetworkAddress, ProtocolVersion, Uid};

/// SplitMix64, for C++'s `deterministicRandom()` uses (shuffles, picks, jitter).
#[derive(Clone, Debug)]
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) fn new(seed: Option<u64>) -> Rng {
        Rng(seed.unwrap_or_else(|| {
            use std::hash::{BuildHasher, Hasher};
            std::collections::hash_map::RandomState::new()
                .build_hasher()
                .finish()
        }))
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// `randomInt(0, n)`: a number in `0..n` (`n > 0`).
    pub(crate) fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }

    /// `random01()`.
    pub(crate) fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// `randomShuffle` (Fisher-Yates).
    pub(crate) fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = self.below(i + 1);
            items.swap(i, j);
        }
    }

    /// `delayJittered`'s factor: `DELAY_JITTER_OFFSET + DELAY_JITTER_RANGE * random01()`
    /// (0.9 + 0.2 r; `flow/Knobs.cpp` 7.4:58-59, 8.0:63-64).
    pub(crate) fn jitter(&mut self, seconds: f64) -> Duration {
        secs(seconds * (0.9 + 0.2 * self.unit()))
    }
}

/// `now()` in seconds.
pub(crate) fn now_secs(now: Instant) -> f64 {
    now.since_origin().as_secs_f64()
}

/// A non-negative number of seconds as a [`Duration`].
pub(crate) fn secs(seconds: f64) -> Duration {
    Duration::try_from_secs_f64(seconds.max(0.0)).unwrap_or(Duration::MAX)
}

/// Which of two raced futures finished.
pub(crate) enum Either<A, B> {
    Left(A),
    Right(B),
}

/// Polls `a` then `b` until one finishes (C++'s `race`/`choose`, favouring the first).
pub(crate) async fn race<A: Future, B: Future>(a: A, b: B) -> Either<A::Output, B::Output> {
    let mut a = std::pin::pin!(a);
    let mut b = std::pin::pin!(b);
    poll_fn(|cx| {
        if let Poll::Ready(v) = a.as_mut().poll(cx) {
            return Poll::Ready(Either::Left(v));
        }
        if let Poll::Ready(v) = b.as_mut().poll(cx) {
            return Poll::Ready(Either::Right(v));
        }
        Poll::Pending
    })
    .await
}

/// A future that never resolves.
pub(crate) async fn never<T>() -> T {
    std::future::pending().await
}

/// Resolves once any of the endpoints is not failed in the failure monitor (C++'s
/// `quorum(onStateEqual(endpoint, available), 1)`): at once if one already is.
pub(crate) async fn any_available(failures: &FailureMonitor, endpoints: &[(NetworkAddress, Uid)]) {
    let mut waits: Vec<Pin<Box<dyn Future<Output = ()> + Send + '_>>> = endpoints
        .iter()
        .map(|(address, token)| {
            let wait = async move {
                while failures.endpoint_status(address, *token).is_failed() {
                    failures.on_state_changed(address, *token).await;
                }
            };
            Box::pin(wait) as Pin<Box<dyn Future<Output = ()> + Send + '_>>
        })
        .collect();
    if waits.is_empty() {
        return never().await;
    }
    poll_fn(|cx| {
        for wait in &mut waits {
            if wait.as_mut().poll(cx).is_ready() {
                return Poll::Ready(());
            }
        }
        Poll::Pending
    })
    .await
}

/// The schema set for a connection's protocol version.
pub(crate) fn schema_for(version: ProtocolVersion) -> Result<SchemaVersion, EncodeError> {
    SchemaVersion::for_protocol_version(version).ok_or(EncodeError::Invalid(
        "no schema set for the server's protocol version",
    ))
}

/// One `tryGetReply` to the endpoint `token` at `address`: the request is encoded by
/// `encode` for the schema set of the connection it goes out on, and the reply comes back
/// with the schema set to decode it with.
pub(crate) async fn try_get_reply<R, W, F>(
    peers: &PeerManager<R, W>,
    address: &NetworkAddress,
    token: Uid,
    encode: F,
) -> Result<(SchemaVersion, Vec<u8>), RequestError>
where
    R: Runtime,
    W: StreamWrapper<R::Stream>,
    F: FnOnce(SchemaVersion, Uid, &mut Vec<u8>) -> Result<(), EncodeError> + Send + 'static,
{
    let peer = peers.peer(address);
    let reply = peer
        .request(token, move |reply, version, out| {
            encode(schema_for(version)?, reply, out)
        })
        .await?;
    let schema = schema_for(reply.version).map_err(RequestError::Encode)?;
    Ok((schema, reply.payload))
}

/// The code C++'s `tryGetReply` returns for a failed request, which is what the load
/// balancers look at: a server's error reply is its code, a request that may have been
/// delivered or was refused by the connection (`NotSent`, ...) is `request_maybe_delivered`
/// (C++ cannot tell "not sent" apart), `broken_promise` is `broken_promise`, and the rest
/// is [`RequestError::fdb_code`].
pub(crate) fn try_get_reply_code(error: &RequestError) -> u16 {
    use crate::error::codes::{BROKEN_PROMISE, REQUEST_MAYBE_DELIVERED};
    match error {
        RequestError::Fdb(e) => e.code,
        RequestError::BrokenPromise => BROKEN_PROMISE,
        RequestError::MaybeDelivered(_)
        | RequestError::EndpointNotFound { .. }
        | RequestError::NotSent(_)
        | RequestError::Backpressure
        | RequestError::TooManyPending { .. } => REQUEST_MAYBE_DELIVERED,
        other => other.fdb_code(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rng_ranges() {
        let mut rng = Rng::new(Some(3));
        for _ in 0..1000 {
            assert!(rng.below(5) < 5);
            let u = rng.unit();
            assert!((0.0..1.0).contains(&u));
            let j = rng.jitter(1.0).as_secs_f64();
            assert!((0.9..=1.1).contains(&j));
        }
        assert_eq!(secs(-1.0), Duration::ZERO);
    }
}
