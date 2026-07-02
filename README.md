# async-selector

Fast and flexible `Future`/`Stream`/task selector.

Designed for optimal performance when polling a large number of tasks
(see [example](https://github.com/Razz4780/async-selector/blob/main/examples/speed.rs)).

Allows for:
1. Polling multiple tasks concurrently on the same thread
2. Safely injecting shared state into polling logic
3. Accessing and removing the tasks by automatically assigned unique ids

## Examples

Simply flatten a set of streams:

```rust
let mut selector = StreamSelector::default();
(0..5).for_each(|i| {
    let (tx, rx) = mpsc::unbounded();
    selector.push(rx);
    tx.unbounded_send(i).unwrap();
});
let collected = selector.collect::<Vec<_>>().await;
assert_eq!(
    collected,
    vec![0, 1, 2, 3],
);
```

Use as a map of streams:

```rust
let mut selector = StreamSelector::default();
let txs = (0..10)
    .map(|_| {
        let (tx, rx) = mpsc::channel::<()>(8);
        let id = selector.push_with_id_cyclic(|id| {
            rx.map(move |item| (id.clone(), item))
        });
        (tx, id)
    })
    .collect::<Vec<_>>();
for (mut tx, saved_id) in txs {
   tx.send(()).await.unwrap();
   let (received_id, ()) = selector.next().await.unwrap();
   assert_eq!(received_id, saved_id);
}
```

More examples live [here](https://github.com/Razz4780/async-selector/tree/main/examples).
