# Phase 1: Multithreaded Task Queue

## Objective

Build a simple multithreaded task queue using Python threads and a thread-safe queue.

The goal of this phase is to demonstrate concurrent task execution, where multiple worker threads process tasks simultaneously from a shared queue.

---

## Features

* Creates 50 tasks and loads them into a `queue.Queue`
* Launches 4 worker threads that pull tasks concurrently
* Simulates task execution using random delays (0.1–0.5 seconds)
* Simulates random task failures with a 20% failure probability
* Tracks completed and failed tasks
* Uses structured logging with timestamps and thread names
* Verifies that every task is processed exactly once

---

## Architecture

```
                  Main Thread
                       │
                       ▼
               +---------------+
               |  queue.Queue  |
               +---------------+
                 ▲    ▲    ▲    ▲
                 │    │    │    │
             Worker1 Worker2 Worker3 Worker4
```

Each worker repeatedly:

1. Retrieves a task from the queue.
2. Processes the task.
3. Marks it as completed or failed.
4. Requests the next available task.
5. Exits when the queue becomes empty.

---

## Concepts Covered

* `threading.Thread` for concurrent execution
* `queue.Queue` for thread-safe task sharing
* Worker thread lifecycle
* `task_done()` and `join()` for synchronization
* Exception handling during task execution
* Structured logging using the `logging` module

---

## How to Run

From the project root:

```bash
python phase1_threads/main.py
```

Or, if you are already inside the `phase1_threads` directory:

```bash
python main.py
```

---

## Sample Output

```
16:46:39.634  [Worker-1]  STARTED  task_001
16:46:39.634  [Worker-2]  STARTED  task_002
16:46:39.634  [Worker-3]  STARTED  task_003
16:46:39.634  [Worker-4]  STARTED  task_004
```

Notice that multiple workers start processing tasks at nearly the same time, demonstrating concurrent execution.

---

## Performance Observation

The program was tested with different numbers of worker threads.

| Workers | Approximate Runtime |
| ------: | ------------------: |
|       1 |               ~16 s |
|       4 |                ~5 s |
|      10 |              ~2.7 s |
|      50 |              ~1.5 s |

Increasing the number of worker threads improves throughput, but the speedup is not perfectly linear because of thread creation, scheduling overhead, logging, and differences in task execution times.

---

## Results

* All 50 tasks are processed.
* Tasks either complete successfully or fail due to simulated errors.
* The program verifies correctness using:

```python
assert tracker["completed"] + tracker["failed"] == TOTAL_JOBS
```

This ensures that no task is lost during execution.

---

## Current Limitation

The `tracker` dictionary is shared by multiple threads without synchronization.

```python
tracker["completed"] += 1
tracker["failed"] += 1
```

Although the program often appears to work correctly, these updates are **not thread-safe** and may lead to race conditions.

This issue will be addressed in **Phase 2** using `threading.Lock`.

---

## Learning Outcome

After completing this phase, I understood:

* How Python threads execute concurrently.
* How `queue.Queue` safely distributes work among multiple workers.
* How worker threads repeatedly process tasks until the queue is empty.
* The purpose of `task_done()` and `join()`.
* Why increasing the number of workers improves performance but eventually shows diminishing returns.
* Why shared mutable state requires synchronization.
