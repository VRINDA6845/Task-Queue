# Phase 2: Shared State and Race Conditions

## Objective

Demonstrate how multiple threads updating shared data can lead to race conditions, and show how `threading.Lock()` prevents those errors.

---

## The Problem

In Phase 1, every worker thread updated the same shared tracker:

```python
tracker["completed"] += 1
```

Although this looks like a single operation, it is logically a **read-modify-write** sequence:

```
Step 1: READ  the current value
Step 2: ADD   1
Step 3: WRITE the updated value back
```

If two threads execute these steps at the same time:

```
Thread 1 reads:  completed = 5
Thread 2 reads:  completed = 5

Thread 1 writes: completed = 6
Thread 2 writes: completed = 6

Correct result : 7
Actual result  : 6
```

One update is overwritten, causing the shared counter to become incorrect.

This is called a **race condition**.

---

## Project Structure

```
phase2_shared_state/
│
├── without_lock.py
├── with_lock.py
└── README.md
```

---

## `without_lock.py`

This version intentionally updates the shared `tracker` dictionary **without synchronization**.

### Features

* Creates 200 tasks
* Starts 16 worker threads
* Uses a shared tracker with **no lock**
* Adds a small artificial delay between reading and writing the counter to make the race condition easier to reproduce
* Runs the experiment 8 times

Because multiple threads overwrite each other's updates, the reported totals are often **less than 200**, even though every task was processed.

---

## `with_lock.py`

This version introduces a shared `threading.Lock`.

Every update to the shared tracker happens inside a protected critical section:

```python
with state_lock:
    current = tracker["completed"]
    tracker["completed"] = current + 1
```

Only one thread can execute this block at a time, preventing simultaneous updates to the shared counters.

The same experiment is repeated 8 times, and every run reports the correct total.

---

## How to Run

From the project root:

```bash
python phase2_shared_state/without_lock.py
```

```bash
python phase2_shared_state/with_lock.py
```

---

## Sample Output

### Without Lock

```
Run 1: completed=53  failed=29  total=82 / 200   *** CORRUPTED *** lost 118 updates
Run 2: completed=45  failed=38  total=83 / 200   *** CORRUPTED *** lost 117 updates
Run 3: completed=54  failed=32  total=86 / 200   *** CORRUPTED *** lost 114 updates

8 out of 8 runs had corrupted counts
```

### With Lock

```
Run 1: completed=131  failed=69  total=200 / 200  OK
Run 2: completed=138  failed=62  total=200 / 200  OK
Run 3: completed=152  failed=48  total=200 / 200  OK

0 out of 8 runs had corrupted counts
Lock successfully prevented race conditions across all runs.
```

---

## How the Lock Works

A single lock is shared by every worker thread.

```python
state_lock = threading.Lock()

with state_lock:
    tracker["completed"] += 1
```

When one thread acquires the lock:

* It enters the critical section.
* Every other thread attempting to acquire the same lock must wait.
* Once the first thread exits the block, the lock is automatically released and another waiting thread may continue.

This guarantees that shared counters are updated one thread at a time.

---

## Performance Trade-off

The locked version is slightly slower because worker threads sometimes wait for access to the shared tracker.

This is the trade-off between:

* **Without Lock:** Faster, but incorrect.
* **With Lock:** Correct, but with a small synchronization overhead.

In real systems, engineers minimize this overhead by keeping the locked section as short as possible.

---

## What I Learned

After completing this phase, I understood:

* Why shared mutable state is dangerous in concurrent programs.
* How race conditions occur during read-modify-write operations.
* Why race conditions are timing-dependent and may not appear every run.
* How `threading.Lock` protects critical sections.
* Why correctness is more important than raw speed when multiple threads share data.

---

## Key Takeaway

Concurrency alone is not enough. When multiple threads share mutable data, access to that data must be synchronized.

Using a shared `threading.Lock` ensures that only one thread updates the shared state at a time, preventing race conditions and producing correct results consistently.
