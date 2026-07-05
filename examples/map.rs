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

/// This example shows how [`Id`]s can be used to build
/// a simple [`Stream`] hashmap on top of a [`StreamSelector`].
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut map = StreamMap::default();

    let mut senders = vec![];
    for i in 0..16 {
        let (tx, rx) = mpsc::unbounded::<usize>();
        senders.push(tx);
        map.insert(i, rx);
    }

    senders[0].unbounded_send(111).unwrap();
    assert_eq!(map.next().await.unwrap(), (0, 111));
    senders[1].unbounded_send(222).unwrap();
    assert_eq!(map.next().await.unwrap(), (1, 222));

    senders[2].unbounded_send(333).unwrap();
    senders[3].unbounded_send(444).unwrap();
    let _ = map.remove(&2).unwrap();
    assert_eq!(map.next().await.unwrap(), (3, 444));
    assert!(map.next().now_or_never().is_none());

    map.get_mut(&4).unwrap().get_mut().get_mut().stream.close();
    assert!(senders[4].is_closed());
}

/// Combined [`StreamSelector`] and a [`HashMap`] that allows for accessing the streams by generic key.
///
/// # Note
///
/// This data structure might not be optimal, as the keys are cloned a lot.
struct StreamMap<K: Clone + Unpin, S: Stream> {
    selector: StreamSelector<NotifyEnd<S, K>>,
    keys: HashMap<K, Id<NotifyEnd<S, K>>>,
}

impl<K, S> StreamMap<K, S>
where
    K: Clone + Eq + Hash + Unpin,
    S: Stream,
{
    pub fn insert(&mut self, key: K, stream: S) -> Option<Removed<NotifyEnd<S, K>>> {
        let id = self.selector.push_with_id(NotifyEnd {
            stream,
            key: Some(key.clone()),
        });
        let prev = self.keys.insert(key, id);
        prev.and_then(|id| self.selector.remove(&id))
    }

    pub fn get_mut(&mut self, key: &K) -> Option<BorrowedMut<'_, NotifyEnd<S, K>>> {
        let id = self.keys.get(key)?;
        self.selector.get_mut(id)
    }

    pub fn remove(&mut self, key: &K) -> Option<Removed<NotifyEnd<S, K>>> {
        let id = self.keys.remove(key)?;
        self.selector.remove(&id)
    }
}

impl<K, S> Stream for StreamMap<K, S>
where
    K: Clone + Eq + Hash + Unpin,
    S: Stream,
{
    type Item = (K, S::Item);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match std::task::ready!(this.selector.poll_next_unpin(cx)) {
                Some((key, Some(item))) => {
                    break Poll::Ready(Some((key, item)));
                }
                Some((key, None)) => {
                    this.keys.remove(&key);
                }
                None => {
                    break Poll::Ready(None);
                }
            }
        }
    }
}

impl<K: Clone + Unpin, S: Stream> Default for StreamMap<K, S> {
    fn default() -> Self {
        Self {
            selector: Default::default(),
            keys: Default::default(),
        }
    }
}

struct NotifyEnd<S, K> {
    stream: S,
    key: Option<K>,
}

impl<S: Stream, K: Clone + Unpin> Stream for NotifyEnd<S, K> {
    type Item = (K, Option<S::Item>);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe {
            // SAFETY: stream is never moved in memory
            self.get_unchecked_mut()
        };
        let Some(key) = &this.key else {
            return Poll::Ready(None);
        };
        let stream = unsafe {
            // SAFETY: stream is never moved in memory
            Pin::new_unchecked(&mut this.stream)
        };
        match std::task::ready!(stream.poll_next(cx)) {
            Some(item) => Poll::Ready(Some((key.clone(), Some(item)))),
            None => Poll::Ready(Some((this.key.take().unwrap(), None))),
        }
    }
}
