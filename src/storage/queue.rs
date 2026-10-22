//! The queue between connection tasks and the log writer, bounded by message
//! count and by payload bytes, so that a burst of large messages cannot hold
//! more than a fixed amount of memory while the writer is busy.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use tokio::sync::{Semaphore, SemaphorePermit, mpsc};

use super::Message;

/// The writer is gone.
#[derive(Debug)]
pub struct Closed;

/// A queue of at most `depth` messages and `bytes` of payload. A message
/// counts against the byte budget from the moment room is reserved for it
/// until the writer has written it, so the budget covers the batch being
/// written as well.
pub fn queue(depth: usize, bytes: u32) -> (QueueSender, QueueReceiver) {
    let (tx, rx) = mpsc::channel(depth);
    let budget = Arc::new(Semaphore::new(bytes as usize));
    let queued = Arc::new(Depth::default());
    (
        QueueSender {
            tx,
            budget: budget.clone(),
            bytes,
            queued: queued.clone(),
        },
        QueueReceiver {
            rx,
            budget,
            bytes,
            queued,
        },
    )
}

/// Messages queued and not yet written, and their payload bytes.
#[derive(Default)]
pub struct Depth {
    pub messages: AtomicU64,
    pub bytes: AtomicU64,
}

#[derive(Clone)]
pub struct QueueSender {
    tx: mpsc::Sender<Message>,
    budget: Arc<Semaphore>,
    bytes: u32,
    queued: Arc<Depth>,
}

impl QueueSender {
    /// Waits until a message of `len` payload bytes fits. The room is held
    /// until the permit is used or dropped, so a caller can wait for room
    /// before it copies the message out of its own buffer. Fails only once
    /// the writer is gone; cancelling the future never leaks room.
    pub async fn reserve(&self, len: usize) -> Result<QueuePermit<'_>, Closed> {
        let bytes = self
            .budget
            .acquire_many(cost(len, self.bytes))
            .await
            .map_err(|_| Closed)?;
        let slot = self.tx.reserve().await.map_err(|_| Closed)?;
        Ok(QueuePermit {
            slot,
            bytes,
            len,
            queued: &self.queued,
        })
    }

    /// Waits until the message fits, then queues it.
    pub async fn send(&self, message: Message) -> Result<(), Closed> {
        self.reserve(message.payload.len()).await?.send(message);
        Ok(())
    }

    /// What waits for the writer; it does not keep the queue open.
    pub fn depth(&self) -> Arc<Depth> {
        self.queued.clone()
    }
}

/// Room for one message of a known length.
pub struct QueuePermit<'a> {
    slot: mpsc::Permit<'a, Message>,
    bytes: SemaphorePermit<'a>,
    len: usize,
    queued: &'a Depth,
}

impl QueuePermit<'_> {
    /// Queues a message with exactly the reserved payload length.
    pub fn send(self, message: Message) {
        assert_eq!(
            message.payload.len(),
            self.len,
            "message length differs from the reservation"
        );
        self.queued.messages.fetch_add(1, Relaxed);
        self.queued.bytes.fetch_add(self.len as u64, Relaxed);
        self.slot.send(message);
        // The writer returns the budget once the message is written.
        self.bytes.forget();
    }
}

pub struct QueueReceiver {
    rx: mpsc::Receiver<Message>,
    budget: Arc<Semaphore>,
    bytes: u32,
    queued: Arc<Depth>,
}

impl QueueReceiver {
    pub async fn recv(&mut self) -> Option<Message> {
        self.rx.recv().await
    }

    pub fn blocking_recv(&mut self) -> Option<Message> {
        self.rx.blocking_recv()
    }

    /// The next message if one is queued already.
    pub fn try_recv(&mut self) -> Option<Message> {
        self.rx.try_recv().ok()
    }

    /// Returns the budget of messages the writer is done with, whether they
    /// were written or dropped.
    pub fn release(&self, messages: &[Message]) {
        let total: usize = messages
            .iter()
            .map(|m| cost(m.payload.len(), self.bytes) as usize)
            .sum();
        self.budget.add_permits(total);
        let bytes: usize = messages.iter().map(|m| m.payload.len()).sum();
        self.queued
            .messages
            .fetch_sub(messages.len() as u64, Relaxed);
        self.queued.bytes.fetch_sub(bytes as u64, Relaxed);
    }
}

impl Drop for QueueReceiver {
    /// Senders waiting for budget fail instead of waiting forever once the
    /// writer is gone, for example after it panicked.
    fn drop(&mut self) {
        self.budget.close();
    }
}

/// A message larger than the whole budget takes all of it rather than
/// waiting forever.
fn cost(len: usize, bytes: u32) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX).min(bytes)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn msg(len: usize) -> Message {
        Message {
            ts: 0,
            payload: vec![b'x'; len],
        }
    }

    /// Whether `send` is still waiting after the scheduler had a chance to run it.
    async fn blocked(task: &tokio::task::JoinHandle<Result<(), Closed>>) -> bool {
        tokio::time::sleep(Duration::from_millis(50)).await;
        !task.is_finished()
    }

    #[tokio::test]
    async fn room_is_held_from_reservation_and_returned_if_unused() {
        let (tx, mut rx) = queue(16, 10);
        let permit = tx.reserve(8).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), tx.reserve(3))
                .await
                .is_err(),
            "8 reserved bytes leave room for 2"
        );
        drop(permit);
        tx.reserve(10).await.unwrap().send(msg(10));
        assert_eq!(rx.recv().await.unwrap().payload.len(), 10);
    }

    #[tokio::test]
    async fn byte_budget_holds_senders_until_the_writer_releases() {
        let (tx, mut rx) = queue(16, 10);
        tx.send(msg(6)).await.unwrap();
        let waiting = tokio::spawn({
            let tx = tx.clone();
            async move { tx.send(msg(5)).await }
        });
        assert!(blocked(&waiting).await, "6 + 5 bytes exceed a budget of 10");

        // Receiving alone frees nothing: the budget covers the write as well.
        let first = rx.recv().await.unwrap();
        assert!(blocked(&waiting).await);
        rx.release(std::slice::from_ref(&first));
        waiting.await.unwrap().unwrap();
        assert_eq!(rx.recv().await.unwrap().payload.len(), 5);
    }

    #[tokio::test]
    async fn depth_counts_messages_until_the_writer_releases_them() {
        let (tx, mut rx) = queue(16, 100);
        let depth = tx.depth();
        let reserved = tx.reserve(7).await.unwrap();
        let level = || (depth.messages.load(Relaxed), depth.bytes.load(Relaxed));
        assert_eq!(level(), (0, 0), "a reservation is not a message yet");
        reserved.send(msg(7));
        tx.send(msg(3)).await.unwrap();
        assert_eq!(level(), (2, 10));
        let first = rx.recv().await.unwrap();
        assert_eq!(level(), (2, 10), "a message being written still waits");
        let second = rx.recv().await.unwrap();
        rx.release(&[first, second]);
        assert_eq!(level(), (0, 0));
    }

    #[tokio::test]
    async fn a_message_larger_than_the_budget_takes_all_of_it() {
        let (tx, mut rx) = queue(16, 10);
        tx.send(msg(100)).await.unwrap();
        let waiting = tokio::spawn({
            let tx = tx.clone();
            async move { tx.send(msg(1)).await }
        });
        assert!(blocked(&waiting).await);
        let big = rx.recv().await.unwrap();
        rx.release(std::slice::from_ref(&big));
        waiting.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn senders_fail_once_the_receiver_is_gone() {
        let (tx, rx) = queue(16, 10);
        tx.send(msg(10)).await.unwrap();
        let waiting = tokio::spawn({
            let tx = tx.clone();
            async move { tx.send(msg(1)).await }
        });
        assert!(blocked(&waiting).await);
        drop(rx);
        assert!(waiting.await.unwrap().is_err());
        assert!(tx.send(msg(1)).await.is_err());
    }

    #[tokio::test]
    async fn a_cancelled_send_returns_its_budget() {
        let (tx, mut rx) = queue(1, 10);
        tx.send(msg(1)).await.unwrap();
        // The channel is full, so this send holds budget while it waits; dropping it must return that.
        assert!(
            tokio::time::timeout(Duration::from_millis(50), tx.send(msg(9)))
                .await
                .is_err()
        );
        let first = rx.recv().await.unwrap();
        rx.release(std::slice::from_ref(&first));
        tokio::time::timeout(Duration::from_secs(1), tx.send(msg(10)))
            .await
            .expect("the whole budget is available again")
            .unwrap();
    }
}
