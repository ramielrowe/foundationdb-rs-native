//! Wakers of tasks that wait on shared state, one slot per waiter.
//!
//! A waiter takes a slot when it first has to wait and gives it back when it is dropped, so a
//! wait that is abandoned (a timeout, a `select`) leaves nothing behind. The slots are a slab:
//! a vector of optional wakers and a free list, so taking and returning one is O(1).

use std::task::Waker;

/// The wakers of the tasks waiting on one thing.
#[derive(Debug, Default)]
pub(crate) struct Waiters {
    slots: Vec<Option<Waker>>,
    free: Vec<usize>,
    /// How many slots are in use (taken and not yet released), woken or not.
    in_use: usize,
}

impl Waiters {
    /// Records `waker` for the waiter that holds `slot`, taking a slot first if it has none.
    pub(crate) fn register(&mut self, slot: &mut Option<usize>, waker: &Waker) {
        match *slot {
            Some(key) => match &mut self.slots[key] {
                Some(existing) if existing.will_wake(waker) => {}
                other => *other = Some(waker.clone()),
            },
            None => {
                let key = match self.free.pop() {
                    Some(key) => {
                        self.slots[key] = Some(waker.clone());
                        key
                    }
                    None => {
                        self.slots.push(Some(waker.clone()));
                        self.slots.len() - 1
                    }
                };
                self.in_use += 1;
                *slot = Some(key);
            }
        }
    }

    /// Gives `slot` back (a waiter that finished or was dropped).
    pub(crate) fn release(&mut self, slot: &mut Option<usize>) {
        if let Some(key) = slot.take() {
            self.slots[key] = None;
            self.free.push(key);
            self.in_use -= 1;
            if self.in_use == 0 {
                // Nobody waits: let the slab shrink back.
                self.slots = Vec::new();
                self.free = Vec::new();
            }
        }
    }

    /// Takes every recorded waker (to wake outside the lock). The waiters keep their slots and
    /// register again when they next poll.
    pub(crate) fn take_all(&mut self) -> Vec<Waker> {
        self.slots.iter_mut().filter_map(Option::take).collect()
    }

    /// Takes the waker of one waiter, the lowest slot first.
    pub(crate) fn take_one(&mut self) -> Option<Waker> {
        self.slots.iter_mut().find_map(Option::take)
    }

    /// How many waiters hold a slot.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.in_use
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.in_use == 0
    }
}
