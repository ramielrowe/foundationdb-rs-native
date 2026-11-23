//! Reconnect backoff: `connectionKeeper`'s `reconnectionDelay`, as a sans-IO calculation.

use std::time::Duration;

use crate::runtime::Instant;

/// The reconnect backoff knobs (`flow/Knobs.cpp`), identical on 7.4 and 8.0:
///
/// | field | knob | default | 7.4 | 8.0 |
/// | --- | --- | --- | --- | --- |
/// | [`initial`](Self::initial) | `INITIAL_RECONNECTION_TIME` | 0.05 s | 119 | 122 |
/// | [`max`](Self::max) | `MAX_RECONNECTION_TIME` | 0.5 s | 120 | 123 |
/// | [`growth`](Self::growth) | `RECONNECTION_TIME_GROWTH_RATE` | 1.2 | 121 | 124 |
/// | [`reset_after`](Self::reset_after) | `RECONNECTION_RESET_TIME` | 5.0 s | 122 | 125 |
/// | [`jitter_offset`](Self::jitter_offset) | `DELAY_JITTER_OFFSET` | 0.9 | 58 | 63 |
/// | [`jitter_range`](Self::jitter_range) | `DELAY_JITTER_RANGE` | 0.2 | 59 | 64 |
///
/// See the [module documentation](super#reconnecting) for how they are used.
#[derive(Clone, Copy, Debug, PartialEq)]
#[non_exhaustive]
pub struct BackoffConfig {
    /// The delay a fresh peer starts with, and returns to after a connection that lasted
    /// longer than [`reset_after`](Self::reset_after).
    pub initial: Duration,
    /// The longest the delay grows to.
    pub max: Duration,
    /// What the delay is multiplied by after each connection that ends early.
    pub growth: f64,
    /// A connection attempt that started more than this long before its connection ended
    /// resets the delay to [`initial`](Self::initial).
    pub reset_after: Duration,
    /// `DELAY_JITTER_OFFSET`: a wait `d` becomes `d * (offset + range * random01())`.
    pub jitter_offset: f64,
    /// `DELAY_JITTER_RANGE`. 0 turns jitter off.
    pub jitter_range: f64,
}

impl BackoffConfig {
    /// FDB's defaults.
    pub const FDB_DEFAULT: BackoffConfig = BackoffConfig {
        initial: Duration::from_millis(50),
        max: Duration::from_millis(500),
        growth: 1.2,
        reset_after: Duration::from_secs(5),
        jitter_offset: 0.9,
        jitter_range: 0.2,
    };

    /// The same delays without jitter (for tests): every wait is exact.
    pub const fn without_jitter(self) -> BackoffConfig {
        BackoffConfig {
            jitter_offset: 1.0,
            jitter_range: 0.0,
            ..self
        }
    }

    /// `delayJittered(d)`'s delay for a random number `unit` in `[0, 1)`. Nonsense knobs fall
    /// back to the plain delay.
    pub(crate) fn jittered(&self, d: Duration, unit: f64) -> Duration {
        let factor = self.jitter_offset + self.jitter_range * unit;
        if factor.is_finite() && factor >= 0.0 {
            d.mul_f64(factor)
        } else {
            d
        }
    }
}

impl Default for BackoffConfig {
    fn default() -> BackoffConfig {
        BackoffConfig::FDB_DEFAULT
    }
}

/// One peer's `lastConnectTime` and `reconnectionDelay`.
///
/// `connectionKeeper` waits `delayJittered(max(0, lastConnectTime + reconnectionDelay - now))`
/// before every connect, sets `lastConnectTime = now` when it starts connecting, and when a
/// connection (or attempt) ends sets `reconnectionDelay` to the initial value if
/// `now - lastConnectTime > RECONNECTION_RESET_TIME`, else to
/// `min(MAX, reconnectionDelay * GROWTH)` (7.4:814-818 and 974-979, 8.0:926-930 and
/// 1067-1072). The delay therefore counts from the *start* of the previous attempt, and grows
/// before it is first used: the first reconnect after a failed attempt waits
/// `INITIAL * GROWTH` = 60 ms, then 72 ms, 86.4 ms, ... up to 500 ms.
#[derive(Clone, Debug)]
pub(crate) struct Backoff {
    config: BackoffConfig,
    delay: Duration,
    last_attempt: Option<Instant>,
}

impl Backoff {
    pub(crate) fn new(config: BackoffConfig) -> Backoff {
        Backoff {
            delay: config.initial,
            config,
            last_attempt: None,
        }
    }

    /// The current `reconnectionDelay`.
    #[cfg(test)]
    pub(crate) fn delay(&self) -> Duration {
        self.delay
    }

    /// When the next attempt may start, if it is wanted at `now`: `now` plus the jittered
    /// remainder of `last_attempt + delay` (`unit` is a random number in `[0, 1)`).
    pub(crate) fn next_attempt(&self, now: Instant, unit: f64) -> Instant {
        let remaining = match self.last_attempt {
            Some(last) => (last + self.delay).saturating_duration_since(now),
            None => Duration::ZERO,
        };
        now + self.config.jittered(remaining, unit)
    }

    /// An attempt starts at `now` (`lastConnectTime = now()`).
    pub(crate) fn attempt_started(&mut self, now: Instant) {
        self.last_attempt = Some(now);
    }

    /// The attempt's connection ended at `now`. A `clean` end (idle, unreferenced or closed
    /// locally) says the peer is healthy and resets the delay; FDB applies its growth rule to
    /// those too, but they only happen on a connection that lived long enough to be idle.
    pub(crate) fn ended(&mut self, now: Instant, clean: bool) {
        let lived = self
            .last_attempt
            .map(|last| now.saturating_duration_since(last))
            .unwrap_or(Duration::ZERO);
        if clean || lived > self.config.reset_after {
            self.delay = self.config.initial;
        } else {
            self.delay = self.config.max.min(self.delay.mul_f64(self.config.growth));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(ms: u64) -> Instant {
        Instant::from_origin(Duration::from_millis(ms))
    }

    #[test]
    fn knob_values() {
        let c = BackoffConfig::FDB_DEFAULT;
        assert_eq!(c.initial, Duration::from_millis(50));
        assert_eq!(c.max, Duration::from_millis(500));
        assert_eq!(c.growth, 1.2);
        assert_eq!(c.reset_after, Duration::from_secs(5));
    }

    #[test]
    fn the_first_attempt_is_immediate() {
        let b = Backoff::new(BackoffConfig::FDB_DEFAULT);
        assert_eq!(b.next_attempt(t(7), 0.5), t(7));
    }

    #[test]
    fn quick_failures_grow_the_delay_from_the_start_of_each_attempt() {
        let mut b = Backoff::new(BackoffConfig::FDB_DEFAULT.without_jitter());
        let mut now = t(1000);
        let mut starts = Vec::new();
        for _ in 0..16 {
            let at = b.next_attempt(now, 0.5);
            starts.push(at.since_origin().as_secs_f64() - 1.0);
            now = at;
            b.attempt_started(now);
            // The attempt fails at once.
            b.ended(now, false);
        }
        let gaps: Vec<f64> = starts.windows(2).map(|w| w[1] - w[0]).collect();
        let expected = [
            0.06, 0.072, 0.0864, 0.10368, 0.124416, 0.1492992, 0.17915904, 0.215_000, 0.258_000,
            0.31, 0.372, 0.446, 0.5, 0.5, 0.5,
        ];
        for (gap, want) in gaps.iter().zip(expected) {
            assert!((gap - want).abs() < 0.0015, "{gaps:?}");
        }
        assert_eq!(b.delay(), Duration::from_millis(500));
    }

    #[test]
    fn a_failure_that_takes_longer_than_the_delay_waits_nothing_more() {
        let mut b = Backoff::new(BackoffConfig::FDB_DEFAULT.without_jitter());
        b.attempt_started(t(0));
        b.ended(t(2000), false); // delay 60 ms, attempt began 2 s ago
        assert_eq!(b.next_attempt(t(2000), 0.0), t(2000));
        assert_eq!(b.next_attempt(t(10), 0.0), t(60));
    }

    #[test]
    fn a_long_connection_resets_the_delay() {
        let mut b = Backoff::new(BackoffConfig::FDB_DEFAULT.without_jitter());
        for i in 0..5 {
            b.attempt_started(t(i));
            b.ended(t(i), false);
        }
        assert!(b.delay() > Duration::from_millis(100));
        b.attempt_started(t(100));
        b.ended(t(100 + 5001), false);
        assert_eq!(b.delay(), Duration::from_millis(50));
        // Exactly the reset time is not longer than it.
        b.attempt_started(t(10_000));
        b.ended(t(15_000), false);
        assert_eq!(b.delay(), Duration::from_millis(60));
    }

    #[test]
    fn a_clean_close_resets_the_delay() {
        let mut b = Backoff::new(BackoffConfig::FDB_DEFAULT.without_jitter());
        b.attempt_started(t(0));
        b.ended(t(0), false);
        b.ended(t(0), false);
        assert!(b.delay() > Duration::from_millis(60));
        b.ended(t(1), true);
        assert_eq!(b.delay(), Duration::from_millis(50));
    }

    #[test]
    fn jitter_scales_the_wait() {
        let mut b = Backoff::new(BackoffConfig::FDB_DEFAULT);
        b.attempt_started(t(0));
        b.ended(t(0), false); // 60 ms
        let lo = b.next_attempt(t(0), 0.0);
        let hi = b.next_attempt(t(0), 0.999_999);
        assert_eq!(lo, t(54));
        assert!(hi >= t(65) && hi <= t(66), "{hi:?}");
        let mut nonsense = BackoffConfig::FDB_DEFAULT;
        nonsense.jitter_offset = f64::NAN;
        assert_eq!(
            nonsense.jittered(Duration::from_secs(1), 0.5),
            Duration::from_secs(1)
        );
    }
}
