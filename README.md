# async-selector

Fast and flexible async task selector for Rust.

Supports:
1. Polling a large number of tasks (e.g. futures/streams) concurrently
2. Safely injecting shared state into the tasks
3. Accessing and removing the tasks by unique ids
4. Static polymorphism in the types of values yielded by the tasks
