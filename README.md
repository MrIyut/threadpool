# Threadpool

## Statement

A generic thread pool that's used for testing to traverse a graph and compute the sum of the elements contained by the nodes.
## Implementation

### Thread Pool Description

- `enqueue_task()`: Enqueue task to the shared task queue.
  Use synchronization.
- `dequeue_task()`: Dequeue task from the shared task queue.
  Use synchronization.
- `wait_for_completion()`: Wait for all worker threads.
  Use synchronization.
- `create_threadpool()`: Create a new thread pool.
- `destroy_threadpool()`: Destroy a thread pool.
  Assume all threads have been joined.

The thread pool is completely independent of any given application.
Any function can be registered in the task queue.

Since the threads are polling the task queue indefinitely, there needs to be defined a condition for them to stop once the graph has been traversed completely.
That is, the condition used by the `wait_for_completion()` function.