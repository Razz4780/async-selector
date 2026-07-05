use std::{
    ops::Not,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use futures::{
    Stream, StreamExt,
    stream::{FuturesUnordered, SelectAll},
};

use async_selector::{FutureSelector, StreamSelector};
use tokio::{sync::Barrier, task::JoinSet};

const ITEMS_IN_TASK: usize = 16 * 1024;
const TASKS: usize = 1024;

/// This example compares speed of [`Selector`](async_selector::selector::Selector)
/// with [`FuturesUnordered`] and [`SelectAll`].
///
/// Each combinator is run in parallel on all threads to showcase
/// the performance gain coming from no global allocator contestation.
fn main() {
    let (workers, elapsed) = run_scenario(|| run_in_multi_thread(stream_selector));
    println!(
        "Ran {workers} concurrent instance(s) of async_selector StreamSelector. \n\
        Each instance processed {} streams (each stream producing {} values). \n\
        Avg instance time: {elapsed:?}\n",
        TASKS, ITEMS_IN_TASK,
    );
    let (workers, elapsed) = run_scenario(|| run_in_multi_thread(select_all));
    println!(
        "Ran {workers} concurrent instance(s) of futures SelectAll. \n\
        Each instance processed {} streams (each stream producing {} values). \n\
        Avg instance time: {elapsed:?}\n",
        TASKS, ITEMS_IN_TASK,
    );
    let (workers, elapsed) = run_scenario(|| run_in_multi_thread(future_selector));
    println!(
        "Ran {workers} concurrent instance(s) of async_selector FutureSelector. \n\
        Each instance processed {} futures (each future yielding {} times). \n\
        Avg instance time: {elapsed:?}\n",
        TASKS, ITEMS_IN_TASK,
    );
    let (workers, elapsed) = run_scenario(|| run_in_multi_thread(futures_unordered));
    println!(
        "Ran {workers} concurrent instance(s) of futures FuturesUnordered. \n\
        Each instance processed {} futures (each future yielding {} times). \n\
        Avg instance time: {elapsed:?}\n",
        TASKS, ITEMS_IN_TASK,
    );
}

fn run_in_multi_thread<Fut, F>(fun: F) -> (usize, Duration)
where
    Fut: Future<Output = Duration> + Send,
    F: 'static + Fn() -> Fut + Clone + Send,
{
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let workers = runtime.metrics().num_workers();

    let elapsed = runtime.block_on(async move {
        let b = Arc::new(Barrier::new(workers));
        let mut set = JoinSet::new();
        for _ in 0..workers {
            let fun = fun.clone();
            let b = b.clone();
            set.spawn(async move {
                b.wait().await;
                fun().await
            });
        }
        let mut sum = Duration::ZERO;
        while let Some(result) = set.join_next().await.transpose().unwrap() {
            sum += result;
        }
        sum
    });

    (workers, elapsed / u32::try_from(workers).unwrap())
}

async fn stream_selector() -> Duration {
    let mut selector = StreamSelector::default();
    for _ in 0_usize..TASKS {
        selector.push(MyStream::default().take(ITEMS_IN_TASK));
    }
    let start = Instant::now();
    let count = selector.collect::<Count>().await;
    let elapsed = start.elapsed();
    assert_eq!(count.0, TASKS * ITEMS_IN_TASK);
    elapsed
}

async fn select_all() -> Duration {
    let mut selector = SelectAll::default();
    for _ in 0_usize..TASKS {
        selector.push(MyStream::default().take(ITEMS_IN_TASK));
    }
    let start = Instant::now();
    let count = selector.collect::<Count>().await;
    let elapsed = start.elapsed();
    assert_eq!(count.0, TASKS * ITEMS_IN_TASK);
    elapsed
}

async fn future_selector() -> Duration {
    let mut selector = FutureSelector::default();
    for _ in 0_usize..TASKS {
        selector.push(MyStream::default().take(ITEMS_IN_TASK).collect::<Count>());
    }
    let start = Instant::now();
    let count = selector.collect::<Count>().await;
    let elapsed = start.elapsed();
    assert_eq!(count.0, TASKS * ITEMS_IN_TASK);
    elapsed
}

async fn futures_unordered() -> Duration {
    let selector = FuturesUnordered::default();
    for _ in 0_usize..TASKS {
        selector.push(MyStream::default().take(ITEMS_IN_TASK).collect::<Count>());
    }
    let start = Instant::now();
    let count = selector.collect::<Count>().await;
    let elapsed = start.elapsed();
    assert_eq!(count.0, TASKS * ITEMS_IN_TASK);
    elapsed
}

fn run_scenario<T, F: Fn() -> T>(scenario: F) -> T {
    const WARMUP: Duration = Duration::from_secs(3);
    println!("Warming up...");
    let warmup_start = Instant::now();
    while warmup_start.elapsed() < WARMUP {
        scenario();
    }
    scenario()
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

#[derive(Default)]
struct Count(usize);

impl Extend<()> for Count {
    fn extend<T: IntoIterator<Item = ()>>(&mut self, iter: T) {
        for _ in iter {
            self.0 += 1;
        }
    }
}

impl Extend<Count> for Count {
    fn extend<T: IntoIterator<Item = Count>>(&mut self, iter: T) {
        for i in iter {
            self.0 += i.0;
        }
    }
}
