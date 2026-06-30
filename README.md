# Distributed Task Queue

A distributed task queue built from scratch in Python — no Celery, no Redis, no existing queue library. Pure Python, constructed phase by phase to understand how distributed systems actually work from the ground up.

---

## What This Is

Most beginner projects *use* infrastructure. This project *builds* it.

A task queue is what systems like Celery, Sidekiq, and AWS SQS do under the hood: accept a list of jobs, distribute them across multiple workers running simultaneously, track what succeeds and what fails, and recover when something crashes. This project implements that from scratch across four phases of increasing complexity.

---

## Architecture

```
Phase 1 & 2 — Single Process, Multiple Threads

        [ Main Program ]
               |
         [ queue.Queue ]
          /    |    \    \
        T1    T2    T3    T4       (worker threads)


Phase 3 — Separate Processes, ZeroMQ Sockets

        [ coordinator.py ]
               |
           (sockets)
          /    |    \
        W1    W2    W3             (worker processes, separate terminals)


Phase 4 — Same as Phase 3 + Fault Detection

        [ coordinator.py ]
          |          ▲
     tasks (PUSH)    | results (PULL)
          |          |
        W1, W2, W3 (worker processes)
          |
     heartbeats (PUSH) every 2s
          ▼
        coordinator watchdog thread
        detects silence → marks dead → requeues task
```

---

## Phases

### Phase 1 — Multithreaded Task Queue
**Files:** `phase1_threads/task_queue.py`

- 50 tasks loaded into a `queue.Queue`
- 4 worker threads all pull from the same queue simultaneously
- 20% random failure rate per task to simulate real-world errors
- Log timestamps prove tasks ran concurrently, not one after another
- Sanity check at the end confirms no tasks were lost

**Core concept:** threads, `queue.Queue`, concurrent execution

---

### Phase 2 — Shared State and Race Conditions
**Files:** `phase2_shared_state/without_lock.py`, `phase2_shared_state/with_lock.py`

- `without_lock.py`: 16 threads, 200 tasks, no lock — totals come out corrupted every run (lost 50–130 tasks per run)
- `with_lock.py`: identical code, one `threading.Lock()` added — totals are exactly 200 every run
- The artificial delay between read and write makes the race condition reliably reproducible

The race condition:
```
Thread 1 reads:  completed = 5
Thread 2 reads:  completed = 5    ← before Thread 1 writes back
Thread 1 writes: completed = 6
Thread 2 writes: completed = 6    ← overwrites Thread 1's update
Correct answer: 7. Actual result: 6. One task silently lost.
```

**Core concept:** race conditions, `threading.Lock`, critical sections

---

### Phase 3 — Separate Processes over ZeroMQ
**Files:** `phase3_sockets/coordinator.py`, `phase3_sockets/worker.py`

- Coordinator and workers are completely separate Python processes
- No shared memory — every piece of information travels as a JSON message over a ZMQ socket
- PUSH/PULL pattern: coordinator pushes tasks out, workers pull them in, workers push results back
- Coordinator sends one `no_more_tasks` shutdown message per worker after all results are collected so workers exit cleanly

```
Coordinator PUSH (5555) ──tasks──►   Worker PULL (5555)
Coordinator PULL (5556) ◄─results─   Worker PUSH (5556)
```

**Core concept:** inter-process communication, ZeroMQ, JSON message passing, coordinator-worker architecture

---

### Phase 4 — Heartbeat Monitoring and Fault Detection
**Files:** `phase4_fault_tolerance/coordinator.py`, `phase4_fault_tolerance/worker.py`

- Workers send a heartbeat ping every 2 seconds containing their current task
- Coordinator runs two background threads: one listening for heartbeats, one watchdog checking for silence
- If a worker goes silent for more than 5 seconds, it is marked dead and its last reported task is put back in the queue
- `deque` used instead of `list` for the pending task queue: `popleft()` is O(1), `list.pop(0)` is O(n)
- Heartbeat thread uses a ZMQ Poller with timeout instead of blocking `recv()` so it exits cleanly when signalled

```
Coordinator PUSH (5555) ──tasks──►      Worker PULL (5555)
Coordinator PULL (5556) ◄─results──     Worker PUSH (5556)
Coordinator PULL (5557) ◄─heartbeats─   Worker PUSH (5557)  ← new
```

**Key design decision:** with ZMQ PUSH/PULL, the coordinator cannot know in advance which worker picked up a given task. So each worker's heartbeat includes whatever task it is currently holding. That way if the worker dies, the coordinator knows exactly what to requeue.

**Core concept:** fault detection, heartbeats, task reassignment, combining threads and sockets

---

## Task Format

Every unit of work in this system is a Python dict:

```python
task = {
    "id": "task_001",
    "type": "process_data",
    "payload": {"input": "data_1"},
    "status": "pending",        # pending → active → done / failed
    "created_at": 1700000000,
}
```

---

## Message Protocol (Phase 3 and 4)

| Message | Direction | Purpose |
|---|---|---|
| `{"type": "task", "job": {...}}` | Coordinator → Worker | Send a task |
| `{"type": "no_more_tasks"}` | Coordinator → Worker | Shutdown signal |
| `{"type": "result", "job_id": ..., "worker_id": ..., "status": ...}` | Worker → Coordinator | Report result |
| `{"type": "heartbeat", "worker_id": ..., "current_task": ...}` | Worker → Coordinator | I'm alive |

---

## Setup

```bash
git clone https://github.com/VRINDA6845/Task-Queue.git
cd Task-Queue
pip install -r requirements.txt
```

---

## How to Run

### Phase 1
```bash
python phase1_threads/task_queue.py
```

### Phase 2
```bash
python phase2_shared_state/without_lock.py   # see the race condition
python phase2_shared_state/with_lock.py      # see the fix
```

### Phase 3 (four terminals)
```bash
# Terminal 1 — start first
python phase3_sockets/coordinator.py

# Terminals 2, 3, 4
python phase3_sockets/worker.py worker_1
python phase3_sockets/worker.py worker_2
python phase3_sockets/worker.py worker_3
```

### Phase 4 (four terminals — to test fault detection, kill a worker mid-task)
```bash
# Terminal 1 — start first
python phase4_fault_tolerance/coordinator.py

# Terminals 2, 3, 4
python phase4_fault_tolerance/worker.py worker_1
python phase4_fault_tolerance/worker.py worker_2
python phase4_fault_tolerance/worker.py worker_3
```

To trigger fault detection: once a worker logs `RECEIVED task_xxx`, press Ctrl+C in that terminal. Within 5 seconds the coordinator will log:
```
!! Worker worker_1 silent for 5.9s — marking DEAD
!! Reassigning task_xxx (was on worker_1)
```

---

## Commit History

```
Commit 1 — Day 1  : Project structure, design note, README
Commit 2 — Phase 1: Multithreaded task queue with worker threads
Commit 3 — Phase 2: Race condition demo and lock fix
Commit 4 — Phase 3: Coordinator and workers over ZeroMQ sockets
Commit 5 — Phase 4: Heartbeat monitoring and dead worker task reassignment
```

---

## What I Learned

- How threads share memory and why that causes race conditions
- Why a read-modify-write on a shared variable is not atomic even though it looks like one line
- How `threading.Lock` protects critical sections and eliminates data corruption
- How ZeroMQ PUSH/PULL sockets distribute work across separate processes
- Why separate processes require explicit message passing instead of shared memory
- How heartbeat-based fault detection works in practice
- Why a heartbeat needs to carry the current task ID (not just a ping) when using PUSH/PULL
- How to combine threading and sockets in the same program (Phase 4 coordinator)
- The difference between a daemon thread (killed automatically) and a non-daemon thread (must be joined)

---

## Tech Stack

- Python 3.10+
- `pyzmq` — ZeroMQ bindings (Phase 3 and 4 only)
- Standard library only for Phase 1 and 2: `threading`, `queue`, `collections`, `time`, `random`, `logging`