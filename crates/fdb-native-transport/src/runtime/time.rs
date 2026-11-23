//! Monotonic time points that are independent of any concrete clock.

use std::ops::{Add, AddAssign, Sub};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// How far before first use the epoch is placed, so that a clock which started
/// shortly before the epoch was created (a paused tokio runtime, say) still
/// reads positive times instead of clamping to zero.
const EPOCH_BACKDATE: Duration = Duration::from_secs(60 * 60);

/// The process-wide origin of every [`Instant`]: shortly before the first time
/// anything in this crate asked for it.
fn epoch() -> std::time::Instant {
    static EPOCH: OnceLock<std::time::Instant> = OnceLock::new();
    *EPOCH.get_or_init(|| {
        let now = std::time::Instant::now();
        now.checked_sub(EPOCH_BACKDATE).unwrap_or(now)
    })
}

/// A point on a [`Runtime`](super::Runtime)'s monotonic clock.
///
/// An `Instant` is a [`Duration`] measured from a **process-wide epoch**: the
/// [`std::time::Instant`] at which this crate first needed one, backdated by
/// an hour. Instants
/// produced by any [`Runtime`](super::Runtime) in the process are therefore
/// comparable with each other, and a deadline computed from one runtime's
/// `now()` is valid on another. Adapters must map their native clock onto that
/// same epoch with [`Instant::from_std`] and [`Instant::to_std`]; they must
/// not invent their own origin.
///
/// The one exception is virtual time. Under tokio's paused clock each tokio
/// runtime has its own mock clock, so instants are only comparable between
/// users of the same paused runtime. Readings from before the epoch (a
/// mock clock started over an hour before first use) clamp to
/// [`Instant::ZERO`]. [`ManualClock`] is a
/// separate clock domain of its own (it starts at `ZERO` and never relates to
/// real time), so never mix its instants with a runtime's.
///
/// This is deliberately not [`std::time::Instant`]: that type cannot be
/// constructed from a value, so it cannot represent a [`ManualClock`].
/// Sans-IO state machines take an `Instant` as an input argument, which keeps
/// them deterministic under test.
///
/// Arithmetic saturates rather than panicking: `a - b` is zero when `b > a`,
/// and `a + d` clamps at the largest representable instant.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Instant(Duration);

impl Instant {
    /// The epoch.
    pub const ZERO: Instant = Instant(Duration::ZERO);

    /// Builds an instant `since_origin` after the clock's origin.
    pub const fn from_origin(since_origin: Duration) -> Self {
        Instant(since_origin)
    }

    /// Time elapsed between the clock's origin and this instant.
    pub const fn since_origin(self) -> Duration {
        self.0
    }

    /// Converts a native monotonic reading to an `Instant`, saturating at
    /// [`Instant::ZERO`] for readings before the epoch.
    pub fn from_std(at: std::time::Instant) -> Self {
        Instant(at.saturating_duration_since(epoch()))
    }

    /// Converts back to a native reading, or `None` if it would overflow
    /// [`std::time::Instant`].
    pub fn to_std(self) -> Option<std::time::Instant> {
        epoch().checked_add(self.0)
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
    fn std_conversion_round_trips() {
        let now = std::time::Instant::now();
        let at = Instant::from_std(now);
        assert_eq!(at.to_std(), Some(now.max(epoch())));
        // Readings before the epoch clamp to zero.
        assert_eq!(
            Instant::from_std(epoch() - Duration::from_nanos(1)),
            Instant::ZERO
        );
        assert_eq!(Instant::from_origin(Duration::MAX).to_std(), None);
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
