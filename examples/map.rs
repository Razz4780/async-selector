//! This example shows how [`Id`]s can be used to build
//! a simple [`Stream`] map on top of a [`StreamSelector`].
//!
//! The map holds streams under user-chosen keys (like a [`HashMap`]),
//! and it is a [`Stream`] itself, which yields `(key, item)` pairs.
//!
//! Run it with `cargo run --example map`.

use std::{
    collections::HashMap,
    hash::Hash,
    pin::Pin,
    task::{Context, Poll},
};

use async_selector::{
    StreamSelector,
    selector::{BorrowedMut, Id, Removed},
};
use futures::{FutureExt, Stream, StreamExt, channel::mpsc};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut map = StreamMap::default();

    // Insert 16 channel receivers, keyed by numbers 0..16.
    let mut senders = vec![];
    for key in 0..16 {
        let (tx, rx) = mpsc::unbounded::<usize>();
        senders.push(tx);
        map.insert(key, rx);
    }

    // Each item comes with the key of the stream that produced it.
    senders[0].unbounded_send(111).unwrap();
    assert_eq!(map.next().await.unwrap(), (0, 111));
    senders[1].unbounded_send(222).unwrap();
    assert_eq!(map.next().await.unwrap(), (1, 222));

    // Removing a stream discards its pending items.
    senders[2].unbounded_send(333).unwrap();
    senders[3].unbounded_send(444).unwrap();
    map.remove(&2).unwrap();
    assert_eq!(map.next().await.unwrap(), (3, 444));

    // No other stream has items ready.
    assert!(map.next().now_or_never().is_none());

    // We can access the streams by key.
    map.get_mut(&4).unwrap().stream.close();
    assert!(senders[4].is_closed());

    println!("All good!");
}

/// [`StreamSelector`] combined with a [`HashMap`],
/// which allows for accessing the streams by an arbitrary key.
///
/// # Note
///
/// To keep this example simple, the keys are cloned a lot
/// (each yielded item comes with a clone of the key).
/// Use cheap keys, or wrap them in an [`Arc`](std::sync::Arc).
struct StreamMap<K, S> {
    /// Holds and polls the streams.
    selector: StreamSelector<KeyedStream<K, S>>,
    /// Maps each key to the [`Id`] of its stream in the selector.
    keys: HashMap<K, Id<KeyedStream<K, S>>>,
}

impl<K, S> StreamMap<K, S>
where
    K: Clone + Eq + Hash + Unpin,
    S: Stream + Unpin,
{
    /// Inserts a stream under the given key.
    ///
    /// If the map already held a stream under this key, the old stream is removed and returned.
    pub fn insert(&mut self, key: K, stream: S) -> Option<Removed<KeyedStream<K, S>>> {
        // Push the stream into the selector, and remember its ID.
        let id = self
            .selector
            .push(KeyedStream {
                stream,
                key: Some(key.clone()),
            })
            .id()
            .clone();

        // Remove the previous stream stored under this key (if any).
        let prev = self.keys.insert(key, id);
        prev.and_then(|id| self.selector.remove(&id))
    }

    /// Returns a mutable reference to the stream stored under the given key.
    pub fn get_mut(&mut self, key: &K) -> Option<BorrowedMut<'_, KeyedStream<K, S>>> {
        let id = self.keys.get(key)?;
        self.selector.get_mut(id)
    }

    /// Removes the stream stored under the given key.
    pub fn remove(&mut self, key: &K) -> Option<Removed<KeyedStream<K, S>>> {
        let id = self.keys.remove(key)?;
        self.selector.remove(&id)
    }
}

impl<K, S> Stream for StreamMap<K, S>
where
    K: Clone + Eq + Hash + Unpin,
    S: Stream + Unpin,
{
    type Item = (K, S::Item);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match std::task::ready!(this.selector.poll_next_unpin(cx)) {
                // One of the streams produced an item.
                Some((key, Some(item))) => break Poll::Ready(Some((key, item))),

                // One of the streams was exhausted. The selector has already dropped it,
                // so we only need to forget its key. Then, we look for the next item.
                Some((key, None)) => {
                    this.keys.remove(&key);
                },

                // The selector is empty.
                None => break Poll::Ready(None),
            }
        }
    }
}

impl<K, S> Default for StreamMap<K, S> {
    fn default() -> Self {
        Self {
            selector: Default::default(),
            keys: Default::default(),
        }
    }
}

/// Wrapper over a stream, which attaches the key to every item,
/// and notifies the [`StreamMap`] when the stream is exhausted.
///
/// We need this to remove the key from the [`StreamMap`] when the stream ends.
/// A plain [`StreamSelector`] silently drops exhausted streams, and does not tell us about it.
struct KeyedStream<K, S> {
    stream: S,
    /// Set to [`None`] after the stream is exhausted.
    key: Option<K>,
}

impl<K, S> Stream for KeyedStream<K, S>
where
    K: Clone + Unpin,
    S: Stream + Unpin,
{
    /// * `(key, Some(item))` for every item produced by the inner stream.
    /// * `(key, None)` once, when the inner stream is exhausted.
    type Item = (K, Option<S::Item>);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Both fields are `Unpin`, so we can access them without any `unsafe` code.
        let this = self.get_mut();

        // We have already reported the end of the stream.
        let Some(key) = &this.key else {
            return Poll::Ready(None);
        };

        match std::task::ready!(this.stream.poll_next_unpin(cx)) {
            Some(item) => Poll::Ready(Some((key.clone(), Some(item)))),
            // Report the end of the stream, giving away the key.
            None => Poll::Ready(Some((this.key.take().unwrap(), None))),
        }
    }
}
