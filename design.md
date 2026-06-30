# Task Queue — Design Document

---

## Project Goal

Build a distributed task queue from scratch in Python across 4 phases of increasing complexity.
Each phase adds one real distributed systems concept on top of the last.
The goal is not a production-ready system — it is a working demonstration of how these systems
are structured and why each layer exists.

---

## Task Format

Every unit of work in the system is represented as a Python dict.
This format stays consistent across all 4 phases.

```python
task = {
    "id": "task_001",           # unique identifier, e.g. task_001 to task_050
    "type": "process_data",     # what kind of work this is
    "payload": {                # data the worker needs to do the job
        "input": "data_1"
    },
    "status": "pending",        # current state of this task
    "created_at": 1700000000,   # unix timestamp of creation
}
```

### Status Flow

```
pending → active → done
                 ↘ failed
```

| Status | Meaning |
|---|---|
| pending | In the queue, not yet picked up |
| active | A worker has it and is processing it |
| done | Worker completed it successfully |
| failed | Worker encountered an error |

---

## Coordinator Responsibilities

Consistent across all phases:
- Holds the task queue
- Distributes tasks to workers
- Tracks how many tasks completed vs failed
- Ensures every task is eventually accounted for

Added in Phase 4:
- Monitors worker heartbeats
- Detects workers that have gone silent
- Requeues tasks from dead workers

## Worker Responsibilities

Consistent across all phases:
- Accepts one task at a time
- Executes the task (simulated with a random sleep + 20% failure rate)
- Reports result back (done or failed)

Added in Phase 4:
- Sends a heartbeat every 2 seconds
- Heartbeat includes the task currently being processed (not just a ping)

---

## Phase-by-Phase Design

### Phase 1: Threads + queue.Queue

```
coordinator (main thread)
      |
      | puts 50 tasks
      ▼
 [ queue.Queue ]  ← thread-safe, no duplicates guaranteed by Queue itself
   /   |   \   \
  T1  T2  T3  T4     (worker threads, all pulling from the same queue)
```

Communication: shared memory via `queue.Queue`.
No explicit messages — threads read directly from the queue.
`queue.Queue` handles the "don't let two workers grab the same task" problem internally.

### Phase 2: Shared State + Locks

Same as Phase 1, but adds a shared results tracker:

```python
tracker = {
    "completed": 0,
    "failed": 0,
}
```

**Without a lock:** multiple threads do read → modify → write simultaneously.
Two threads can read the same value, both increment it, and one update is lost.
This is a race condition. Demonstrated with 16 threads and 200 tasks: totals
consistently corrupt (200 tasks in, ~140 counted).

**With a lock:** `threading.Lock()` wraps every write.
Only one thread can be inside the critical section at a time.
Result: totals are exactly 200 every single run.

### Phase 3: Separate Processes + ZeroMQ Sockets

Threads become separate programs. Shared memory is gone.
Every piece of information must travel explicitly as a JSON message.

```
Coordinator (coordinator.py)          Worker (worker.py, 3 instances)
────────────────────────────          ──────────────────────────────
PUSH socket (port 5555)  ──tasks──►   PULL socket (port 5555)
PULL socket (port 5556)  ◄─results─   PUSH socket (port 5556)
```

**ZMQ PUSH/PULL pattern:**
PUSH distributes messages round-robin across all connected PULL sockets.
Workers don't compete — ZMQ hands each task to exactly one worker.

**Message types:**

| Message | Direction | Content |
|---|---|---|
| task | Coordinator → Worker | `{"type": "task", "job": {...}}` |
| no_more_tasks | Coordinator → Worker | `{"type": "no_more_tasks"}` |
| result | Worker → Coordinator | `{"type": "result", "job_id": ..., "worker_id": ..., "status": ...}` |

**Shutdown:** coordinator sends one `no_more_tasks` per worker after all results
are collected. Workers exit cleanly on receiving it.

### Phase 4: Heartbeats + Fault Detection

Adds a third socket channel for heartbeats, and two background threads in the coordinator.

```
Coordinator                              Worker
───────────                              ──────
PUSH (5555) ──tasks──►                   PULL (5555)
PULL (5556) ◄──results──                 PUSH (5556)
PULL (5557) ◄──heartbeats every 2s──     PUSH (5557)   ← new

Background threads in coordinator:
  Thread 1: heartbeat_listener — records last_heard[worker_id] = now
  Thread 2: watchdog — every 1s, checks if any worker exceeded TIMEOUT
```

**Key design decision:**
With ZMQ PUSH/PULL, the coordinator does not know in advance which worker
received a given task. A heartbeat that only says "I'm alive" is not enough —
when the worker dies, the coordinator would know it's dead but not what to requeue.

Solution: each heartbeat carries the worker's current task:
```python
{"worker_id": "worker_1", "current_task": {"id": "task_007", ...}}
```
The coordinator tracks `current_task[worker_id]`. When a worker goes silent,
it requeues `current_task[worker_id]` immediately.

**Fault detection flow:**
```
1. Worker picks up task_007
2. Worker sends heartbeats: {"worker_id": "w1", "current_task": task_007}
3. Worker crashes — heartbeats stop
4. Watchdog notices silence > 5 seconds
5. Coordinator: marks w1 dead, appends task_007 back to pending_tasks
6. task_007 gets sent to next available worker
7. All 30 tasks still complete correctly
```

**Thread safety in Phase 4:**
- `state` dict (heartbeats, current tasks, dead workers) protected by `state_lock`
- `pending_tasks` deque protected by `tasks_lock`
- Two separate locks to avoid a deadlock between the watchdog and the main loop
- `heartbeat_listener` uses `zmq.Poller` with timeout instead of blocking `recv()`
  so it can check `stop_event` and exit cleanly without a ZMQ context crash

---

## Message Flow Summary

```
Phase 1-2           Phase 3                 Phase 4
─────────           ───────                 ───────
queue.Queue         JSON over ZMQ           JSON over ZMQ
                    2 socket channels       3 socket channels
                    task + result           task + result + heartbeat
no explicit         shutdown message        shutdown message
 shutdown           per worker              per worker
```

---

## What Each Phase Teaches

| Phase | Question answered |
|---|---|
| 1 | How do multiple workers share a list of jobs safely? |
| 2 | What breaks when they share *other* data, and how do you fix it? |
| 3 | What changes when workers can't share memory at all? |
| 4 | What happens when a worker dies mid-task, and how do you recover? |

These four questions, answered in order, form the core of distributed systems design.