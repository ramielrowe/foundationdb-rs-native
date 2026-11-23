//! Monotonic time points that are independent of any concrete clock.

use std::ops::{Add, AddAssign, Sub};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// A point on a [`Runtime`](super::Runtime)'s monotonic clock.
///
/// An `Instant` is a [`Duration`] measured from an arbitrary, runtime-defined
/// origin. Only the *difference* between two instants from the same runtime is
/// meaningful; instants from different runtimes (or different
/// [`ManualClock`]s) must not be compared.
///
/// This is deliberately not [`std::time::Instant`]: that type cannot be
/// constructed from a value, so it cannot represent tokio's paused clock or a
/// [`ManualClock`]. Sans-IO state machines take an `Instant` as an input
/// argument, which keeps them deterministic under test.
///
/// Arithmetic saturates rather than panicking: `a - b` is zero when `b > a`,
/// and `a + d` clamps at the largest representable instant.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Instant(Duration);

impl Instant {
    /// The clock's origin.
    pub const ZERO: Instant = Instant(Duration::ZERO);

    /// Builds an instant `since_origin` after the clock's origin.
    pub const fn from_origin(since_origin: Duration) -> Self {
        Instant(since_origin)
    }

    /// Time elapsed between the clock's origin and this instant.
    pub const fn since_origin(self) -> Duration {
        self.0
    }

    /// Time from `earlier` to `self`, or zero if `earlier` is later.
    pub fn saturating_duration_since(self, earlier: Instant) -> Duration {
        self.0.saturating_sub(earlier.0)
    }

    /// `self + d`, or `None` on overflow.
    pub fn checked_add(self, d: Duration) -> Option<Instant> {
        self.0.checked_add(d).map(Instant)
    }
}

impl Add<Duration> for Instant {
    type Output = Instant;

    fn add(self, d: Duration) -> Instant {
        Instant(self.0.saturating_add(d))
    }
}

impl AddAssign<Duration> for Instant {
    fn add_assign(&mut self, d: Duration) {
        *self = *self + d;
    }
}

impl Sub<Instant> for Instant {
    type Output = Duration;

    fn sub(self, earlier: Instant) -> Duration {
        self.saturating_duration_since(earlier)
    }
}

/// A hand-advanced clock for deterministic tests of sans-IO logic.
///
/// Clones share the same time. The clock only moves when [`advance`] is
/// called. It has no timers: it is a time source to feed into state machines
/// that take `now: Instant`. To test code that awaits real
/// [`Runtime::sleep`](super::Runtime::sleep) futures, use the runtime's own
/// virtual time instead (for tokio, `tokio::time::pause`).
///
/// [`advance`]: ManualClock::advance
#[derive(Clone, Debug, Default)]
pub struct ManualClock {
    nanos: Arc<AtomicU64>,
}

impl ManualClock {
    /// A clock reading [`Instant::ZERO`].
    pub fn new() -> Self {
        Self::default()
    }

    /// The current reading.
    pub fn now(&self) -> Instant {
        Instant(Duration::from_nanos(self.nanos.load(Ordering::Acquire)))
    }

    /// Moves the clock forward by `d` (saturating at `u64::MAX` nanoseconds,
    /// about 584 years) and returns the new reading.
    pub fn advance(&self, d: Duration) -> Instant {
        let add = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        let prev = match self
            .nanos
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                Some(n.saturating_add(add))
            }) {
            Ok(n) | Err(n) => n,
        };
        Instant(Duration::from_nanos(prev.saturating_add(add)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instant_arithmetic_saturates() {
        let a = Instant::ZERO + Duration::from_secs(5);
        let b = a + Duration::from_secs(2);
        assert_eq!(b - a, Duration::from_secs(2));
        assert_eq!(a - b, Duration::ZERO);
        assert!(a < b);
        let max = Instant::from_origin(Duration::MAX);
        assert_eq!(max + Duration::from_secs(1), max);
        assert_eq!(max.checked_add(Duration::from_secs(1)), None);
    }

    #[test]
    fn manual_clock_is_shared_between_clones() {
        let clock = ManualClock::new();
        let other = clock.clone();
        assert_eq!(clock.now(), Instant::ZERO);
        let t = other.advance(Duration::from_millis(50));
        assert_eq!(t, Instant::from_origin(Duration::from_millis(50)));
        assert_eq!(clock.now(), t);
        clock.advance(Duration::MAX);
        assert_eq!(clock.now().since_origin(), Duration::from_nanos(u64::MAX));
    }
}
