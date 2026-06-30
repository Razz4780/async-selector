use std::{
    ops::Not,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use futures::{
    Stream, StreamExt,
    stream::{FuturesUnordered, SelectAll},
};

use async_selector::{FutureSelector, StreamSelector};
use tokio::{sync::Barrier, task::JoinSet};

#[tokio::main]
async fn main() {
    let barrier = Arc::new(Barrier::new(4));

    let mut set = JoinSet::new();
    let b = barrier.clone();
    set.spawn(async move {
        b.wait().await;
        speed_test_stream_selector().await;
    });
    let b = barrier.clone();
    set.spawn(async move {
        b.wait().await;
        speed_test_select_all().await;
    });
    let b = barrier.clone();
    set.spawn(async move {
        b.wait().await;
        speed_test_futures_unordered().await;
    });
    let b = barrier.clone();
    set.spawn(async move {
        b.wait().await;
        speed_test_future_selector().await;
    });
    set.join_all().await;
}

async fn speed_test_stream_selector() {
    let mut selector = StreamSelector::default();
    for _ in 0_usize..1024 {
        selector.push(MyStream::default().take(16 * 1024));
    }
    test_speed(selector, "async_selector::StreamSelector").await;
}

async fn speed_test_select_all() {
    let mut selector = SelectAll::default();
    for _ in 0_usize..1024 {
        selector.push(MyStream::default().take(16 * 1024));
    }
    test_speed(selector, "futures::SelectAll").await;
}

async fn speed_test_futures_unordered() {
    let selector = FuturesUnordered::default();
    for _ in 0_usize..1024 {
        selector.push(
            MyStream::default()
                .take(16 * 1024)
                .collect::<CountCollector>(),
        );
    }
    test_speed(selector, "futures::FuturesUnordered").await;
}

async fn speed_test_future_selector() {
    let mut selector = FutureSelector::default();
    for _ in 0_usize..1024 {
        selector.push(
            MyStream::default()
                .take(16 * 1024)
                .collect::<CountCollector>(),
        );
    }
    test_speed(selector, "async_selector::FutureSelector").await;
}

#[derive(Default, Debug)]
struct MyStream(bool);

impl Stream for MyStream {
    type Item = ();

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.0 = this.0.not();
        if this.0 {
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(Some(()))
        }
    }
}

async fn test_speed<I, S: Stream<Item = I>>(stream: S, msg: &str)
where
    CountCollector: Extend<I>,
{
    let started = Instant::now();
    let CountCollector(count) = stream.collect().await;
    let elapsed = started.elapsed();
    println!("{msg} processed {count} items in {elapsed:?}");
}

#[derive(Default)]
struct CountCollector(usize);

impl Extend<()> for CountCollector {
    fn extend<T: IntoIterator<Item = ()>>(&mut self, iter: T) {
        for _ in iter {
            self.0 += 1;
        }
    }
}

impl Extend<CountCollector> for CountCollector {
    fn extend<T: IntoIterator<Item = CountCollector>>(&mut self, iter: T) {
        for i in iter {
            self.0 += i.0;
        }
    }
}
