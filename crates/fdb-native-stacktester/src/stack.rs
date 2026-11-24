//! The stack: items tagged with the number of the instruction that pushed them, some of them
//! futures that are waited for when they are popped (`Stack.pop` of the Python tester).

use std::future::{Future, poll_fn};
use std::sync::Arc;
use std::task::Poll;

use fdb_native_tuple::Element;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::Fatal;

/// An item of the stack: the 0-based number of the instruction that pushed it, and its value.
#[derive(Clone, Debug)]
pub(crate) struct Item {
    pub(crate) index: usize,
    pub(crate) entry: Entry,
}

/// A value, or a future of one (`COMMIT`, `ON_ERROR`, `GET_VERSIONSTAMP`). A future that
/// fails resolves to the packed `("ERROR", code)` tuple; one with no result to
/// `RESULT_NOT_PRESENT`: the conversion happens in the future itself.
#[derive(Clone, Debug)]
pub(crate) enum Entry {
    Ready(Element),
    /// Shared by the copies `DUP` makes, and resolved once.
    Pending(Arc<Pending>),
}

/// A future running as a task of its own (so that it makes progress whether or not it is
/// waited for, as an `fdb_c` future does), and its value once it was waited for.
#[derive(Debug)]
pub(crate) struct Pending(Mutex<PendingState>);

#[derive(Debug)]
enum PendingState {
    Running(JoinHandle<Element>),
    Done(Element),
}

impl Entry {
    /// The value, waiting for a future.
    pub(crate) async fn resolve(&self) -> Result<Element, Fatal> {
        match self {
            Entry::Ready(value) => Ok(value.clone()),
            Entry::Pending(pending) => {
                let mut state = pending.0.lock().await;
                let value = match &mut *state {
                    PendingState::Done(value) => return Ok(value.clone()),
                    PendingState::Running(task) => task
                        .await
                        .map_err(|e| Fatal::new(format!("a future's task failed: {e}")))?,
                };
                *state = PendingState::Done(value.clone());
                Ok(value)
            }
        }
    }
}

/// Starts `future` now: polls it once in place, so that what it does synchronously (a
/// commit's start, `on_error`'s reset) happens before the next instruction runs, as it does
/// when `fdb_c` creates the future; the rest runs as a task.
pub(crate) async fn start<F>(future: F) -> Entry
where
    F: Future<Output = Element> + Send + 'static,
{
    let mut future = Box::pin(future);
    match poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await {
        Poll::Ready(value) => Entry::Ready(value),
        Poll::Pending => Entry::Pending(Arc::new(Pending(Mutex::new(PendingState::Running(
            tokio::spawn(future),
        ))))),
    }
}

/// The stack. Depth 0 is the top (the last item pushed).
#[derive(Debug, Default)]
pub(crate) struct Stack {
    /// Bottom first.
    items: Vec<Item>,
}

impl Stack {
    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub(crate) fn push(&mut self, index: usize, value: Element) {
        self.push_entry(index, Entry::Ready(value));
    }

    pub(crate) fn push_entry(&mut self, index: usize, entry: Entry) {
        self.items.push(Item { index, entry });
    }

    pub(crate) fn push_item(&mut self, item: Item) {
        self.items.push(item);
    }

    /// Pops the top item, waiting for it if it is a future: its instruction number and value.
    pub(crate) async fn pop(&mut self) -> Result<(usize, Element), Fatal> {
        let item = self
            .items
            .pop()
            .ok_or_else(|| Fatal::new("pop from an empty stack"))?;
        Ok((item.index, item.entry.resolve().await?))
    }

    /// Pops up to `count` items (fewer if the stack is shorter, as the Python tester's slice
    /// does), the top first, waiting for the futures.
    pub(crate) async fn pop_many(&mut self, count: usize) -> Result<Vec<Element>, Fatal> {
        let count = count.min(self.items.len());
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(self.pop().await?.1);
        }
        Ok(values)
    }

    /// The top item (not waited for).
    pub(crate) fn top(&self) -> Result<&Item, Fatal> {
        self.items
            .last()
            .ok_or_else(|| Fatal::new("the stack is empty"))
    }

    /// Swaps the items at depth 0 and `depth`, with their instruction numbers.
    pub(crate) fn swap(&mut self, depth: usize) -> Result<(), Fatal> {
        let len = self.items.len();
        if depth >= len {
            return Err(Fatal::new(format!(
                "SWAP with depth {depth} on a stack of {len} items"
            )));
        }
        self.items.swap(len - 1, len - 1 - depth);
        Ok(())
    }

    pub(crate) fn clear(&mut self) {
        self.items.clear();
    }

    /// The items, bottom first (for tests).
    #[cfg(test)]
    pub(crate) fn items(&self) -> &[Item] {
        &self.items
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(b: &[u8]) -> Element {
        Element::Bytes(b.to_vec())
    }

    #[tokio::test]
    async fn pop_returns_items_last_in_first_out_with_their_index() {
        let mut stack = Stack::default();
        stack.push(0, bytes(b"a"));
        stack.push(1, bytes(b"b"));
        assert_eq!(stack.pop().await.unwrap(), (1, bytes(b"b")));
        assert_eq!(stack.pop().await.unwrap(), (0, bytes(b"a")));
        assert!(stack.pop().await.is_err());
    }

    #[tokio::test]
    async fn pop_many_takes_what_there_is() {
        let mut stack = Stack::default();
        stack.push(0, Element::Int(1));
        stack.push(1, Element::Int(2));
        assert_eq!(
            stack.pop_many(5).await.unwrap(),
            vec![Element::Int(2), Element::Int(1)]
        );
        assert!(stack.is_empty());
    }

    #[tokio::test]
    async fn swap_keeps_instruction_numbers_with_items() {
        let mut stack = Stack::default();
        for i in 0..3 {
            stack.push(i, Element::Int(i as i64));
        }
        stack.swap(2).unwrap();
        assert_eq!(stack.pop().await.unwrap(), (0, Element::Int(0)));
        assert_eq!(stack.pop().await.unwrap(), (1, Element::Int(1)));
        assert_eq!(stack.pop().await.unwrap(), (2, Element::Int(2)));
        assert!(stack.swap(0).is_err());
    }

    #[tokio::test]
    async fn a_ready_future_is_a_value() {
        let entry = start(async { bytes(b"now") }).await;
        assert!(matches!(entry, Entry::Ready(_)));
    }

    #[tokio::test]
    async fn a_pending_future_is_resolved_once_and_shared_by_its_copies() {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let entry = start(async move {
            rx.await.unwrap();
            bytes(b"later")
        })
        .await;
        assert!(matches!(entry, Entry::Pending(_)));
        let mut stack = Stack::default();
        stack.push_entry(7, entry);
        let copy = stack.top().unwrap().clone();
        stack.push_item(copy);
        tx.send(()).unwrap();
        assert_eq!(stack.pop().await.unwrap(), (7, bytes(b"later")));
        assert_eq!(stack.pop().await.unwrap(), (7, bytes(b"later")));
    }

    #[tokio::test]
    async fn start_runs_the_synchronous_part_before_returning() {
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seen = flag.clone();
        let (_keep, rx) = tokio::sync::oneshot::channel::<()>();
        let _entry = start(async move {
            seen.store(true, std::sync::atomic::Ordering::SeqCst);
            let _ = rx.await;
            Element::Null
        })
        .await;
        assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
    }
}
