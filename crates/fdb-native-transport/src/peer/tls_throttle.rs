//! The client side of FDB's TLS handshake throttle: after repeated failed handshakes with a
//! server address, stop dialing it for a while.
//!
//! `SSLConnection::connect` and `connectHandshakeWrapper` (`flow/Net2.actor.cpp` 7.4:927-946
//! and 1194-1226, `flow/Net2.cpp` 8.0:919-939 and 1160-1197) keep one entry per remote
//! `(ip, port)` in `serverTLSConnectionThrottler`:
//!
//! * a failed client handshake (`recordHandshakeFailure`, 7.4:1342-1353; inline on 8.0)
//!   creates the entry `(0, now + TLS_CLIENT_CONNECTION_THROTTLE_TIMEOUT)` (11 s), or adds
//!   one to the count of an existing one, without moving its end;
//! * a connect to an address whose entry has not ended and whose count has reached
//!   `TLS_CLIENT_CONNECTION_THROTTLE_ATTEMPTS` (1, which the second failure in the window
//!   does) does not dial: it waits `CONNECTION_MONITOR_TIMEOUT` (2 s) and fails with
//!   `connection_failed`; an entry that has ended is dropped when it is next looked at.
//!
//! The knobs (`flow/Knobs.cpp` 7.4:137-139, 8.0:140-142) are
//! [`PeerManagerConfig::tls_throttle_timeout`](super::PeerManagerConfig::tls_throttle_timeout)
//! and [`tls_throttle_attempts`](super::PeerManagerConfig::tls_throttle_attempts).
//!
//! **What counts** is a handshake that fails ([`PeerError::Tls`](super::PeerError::Tls)).
//! One that runs out of time does not, in practice: `connectionKeeper` races the whole
//! connect (TCP, then the handshake) against `CONNECTION_MONITOR_TIMEOUT` from its start
//! (`fdbrpc/FlowTransport.actor.cpp` 7.4:829-869), so that timer ends first and cancels the
//! handshake, whose own timer (the same 2 s, started after the TCP connect) never fires, and
//! a cancelled handshake records nothing. Nor does a server's refusal of the client's
//! certificate, which a TLS 1.3 client only sees after its handshake, as a connection error.
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use crate::runtime::Instant;

/// The failures counted per address: `(count, end of the window)`.
#[derive(Debug, Default)]
pub(crate) struct TlsThrottle {
    entries: Mutex<HashMap<SocketAddr, (u32, Instant)>>,
}

impl TlsThrottle {
    /// Whether a connect to `addr` at `now` must not dial (C++'s check before connecting).
    /// An entry whose window has ended is dropped.
    pub(crate) fn is_throttled(&self, addr: SocketAddr, now: Instant, attempts: u32) -> bool {
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        match entries.get(&addr) {
            Some(&(count, end)) if now < end => count >= attempts,
            Some(_) => {
                entries.remove(&addr);
                false
            }
            None => false,
        }
    }

    /// Records a failed (or timed-out) handshake with `addr` at `now`
    /// (`recordHandshakeFailure`). Entries whose window has ended are dropped first, so the
    /// map holds only addresses that failed within the last `timeout`.
    pub(crate) fn record_failure(&self, addr: SocketAddr, now: Instant, timeout: Duration) {
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        entries.retain(|_, (_, end)| now < *end);
        entries
            .entry(addr)
            .and_modify(|(count, _)| *count = count.saturating_add(1))
            .or_insert((0, now + timeout));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(secs: u64) -> Instant {
        Instant::from_origin(Duration::from_secs(secs))
    }

    #[test]
    fn the_second_failure_in_the_window_throttles_until_it_ends() {
        let t = TlsThrottle::default();
        let a: SocketAddr = "10.0.0.1:4500".parse().unwrap();
        let b: SocketAddr = "10.0.0.1:4501".parse().unwrap();
        let timeout = Duration::from_secs(11);
        assert!(!t.is_throttled(a, at(0), 1));
        t.record_failure(a, at(0), timeout);
        assert!(!t.is_throttled(a, at(1), 1), "one failure: count 0");
        t.record_failure(a, at(5), timeout);
        assert!(t.is_throttled(a, at(5), 1));
        assert!(t.is_throttled(a, at(10), 1), "the window is not moved");
        assert!(!t.is_throttled(b, at(10), 1), "per ip:port");
        assert!(!t.is_throttled(a, at(11), 1), "ended");
        // Dropped: the next failure starts a new window.
        t.record_failure(a, at(12), timeout);
        assert!(!t.is_throttled(a, at(12), 1));
        // More attempts allowed.
        t.record_failure(a, at(13), timeout);
        assert!(!t.is_throttled(a, at(13), 2));
        t.record_failure(a, at(13), timeout);
        assert!(t.is_throttled(a, at(13), 2));
    }

    #[test]
    fn ended_windows_are_pruned() {
        let t = TlsThrottle::default();
        let timeout = Duration::from_secs(11);
        for port in 0..100 {
            t.record_failure(SocketAddr::from(([10, 0, 0, 1], port)), at(0), timeout);
        }
        t.record_failure("10.0.0.2:1".parse().unwrap(), at(20), timeout);
        assert_eq!(t.entries.lock().unwrap().len(), 1);
    }
}
