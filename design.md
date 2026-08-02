# Task Queue — Design Document

---

## Project goal

Build a distributed task queue from scratch in Python, starting from a simple threading exercise and evolving it, one real distributed-systems concept at a time, into a fault-tolerant, persistent, containerized system with a REST API and live dashboard.

The goal was never a production-ready system meant to replace Celery. It's a working demonstration of how these systems are structured, and — just as importantly — an honest account of what tradeoffs were made and why, rather than a system that claims guarantees it doesn't actually provide.

---

## Task format

```python
task = {
    "id": "task_001",
    "type": "process_data",
    "payload": {"input": "data_1"},
    "status": "pending",
    "created_at": 1700000000,
    "worker_id": None,   # set once dispatched
}
```

### Status flow (final version)

```
pending → dispatched → running → done
                                ↘ failed
```

| Status | Meaning |
|---|---|
| `pending` | In the queue, not yet sent to any worker |
| `dispatched` | Sent to a worker; no confirmation yet that it started |
| `running` | Worker sent an explicit `started` acknowledgement |
| `done` | Worker completed it successfully |
| `failed` | Worker encountered an error |

The `dispatched` → `running` split was added specifically to support delivery-semantics analysis (see below) — without it, "sent" and "actually executing" were indistinguishable, which made it impossible to reason precisely about what a dead worker might have actually completed.

---

## Coordinator responsibilities

- Holds the task queue (`pending_tasks`, a `deque`)
- Distributes tasks to workers
- Tracks per-task status in a registry (`tasks: dict`)
- Monitors worker heartbeats
- Detects workers that have gone silent and requeues their in-flight task
- Answers state queries from the API over a dedicated REQ/REP channel
- Mirrors every task-state change to SQLite for durability
- On startup, recovers any non-terminal task from SQLite

## Worker responsibilities

- Accepts one task at a time
- Sends a `started` acknowledgement before executing
- Executes the task (demo workload: simulated sleep + 20% failure; benchmark workload: fixed 0.1s, no failure)
- Reports a final result (`done`/`failed`)
- Sends a heartbeat every 2 seconds, carrying whatever task it's currently holding

---

## Phase-by-phase design (the learning progression)

### Phase 1: Threads + `queue.Queue`

```
coordinator (main thread)
      |
      | puts 50 tasks
      ▼
 [ queue.Queue ]  ← thread-safe, no duplicate hand-out guaranteed by Queue itself
   /   |   \   \
  T1  T2  T3  T4     (worker threads, all pulling from the same queue)
```

Communication is shared memory via `queue.Queue`. No explicit messages — threads read directly from the queue, which internally handles "don't let two workers grab the same task."

### Phase 2: Shared state + locks

Added a shared tracker: `{"completed": 0, "failed": 0}`.

**Without a lock:** read → modify → write across multiple threads is not atomic even though it looks like one line. Two threads can read the same value, both increment, and one update silently disappears. Demonstrated with 16 threads / 200 tasks: totals consistently corrupt (losing 50–130 tasks per run).

**With a lock:** `threading.Lock()` around every write serializes the critical section. Totals come out exactly 200, every run. The tradeoff: locking adds overhead, and the locked version is measurably slower — the lesson isn't "locks make things faster," it's that correctness requires synchronization, and synchronization has a cost.

### Phase 3: Separate processes + ZeroMQ

Threads become independent processes. Shared memory is gone entirely; every piece of information must travel as an explicit JSON message.

```
Coordinator                          Worker (×3)
────────────                         ───────────
PUSH (5555)  ──tasks──►              PULL (5555)
PULL (5556)  ◄─results──             PUSH (5556)
```

**ZMQ PUSH/PULL semantics:** PUSH distributes round-robin across connected PULL sockets. Workers don't compete for tasks — ZMQ itself hands each task to exactly one connected puller.

**Shutdown:** coordinator sends one `no_more_tasks` message per worker after all results are collected, so workers exit cleanly rather than blocking forever on `recv()`.

### Phase 4: Heartbeats + fault detection

Adds a third socket channel and two background threads in the coordinator.

```
Coordinator                              Worker
───────────                              ──────
PUSH (5555) ──tasks──►                   PULL (5555)
PULL (5556) ◄──results──                 PUSH (5556)
PULL (5557) ◄──heartbeats every 2s──     PUSH (5557)   ← new

Thread 1: heartbeat_listener — records last_heartbeat[worker_id] = now
Thread 2: watchdog — every 1s, checks if any worker exceeded HEARTBEAT_TIMEOUT
```

**Key design decision:** with plain PUSH/PULL, the coordinator has no built-in way to know which worker received a given task. A heartbeat that only says "I'm alive" wouldn't be enough — if that worker died, the coordinator would know *that* it died but not *what to requeue*. So every heartbeat carries the worker's current task:

```python
{"worker_id": "worker_1", "current_task": {"id": "task_007", ...}}
```

The coordinator tracks `state["current_task"][worker_id]`. When a worker goes silent past `HEARTBEAT_TIMEOUT`, its last-known task is immediately appended back to `pending_tasks`.

**Thread safety:** `state` (heartbeats, current tasks, dead workers) is protected by `state_lock`; `pending_tasks` by `tasks_lock` — two separate locks specifically to avoid a deadlock scenario between the watchdog thread and the main dispatch loop. `heartbeat_listener` uses a `zmq.Poller` with a timeout instead of a blocking `recv()`, so it can check `stop_event` periodically and exit cleanly rather than hanging on shutdown.

---

## From batch script to long-running service

Phase 4 processed a fixed 30-task batch and exited once all were accounted for. The production system (`distributed_queue/`) removes that ceiling entirely:

- `TOTAL_JOBS` is gone. The main loop runs on `while not stop_event.is_set()` — no completion-count exit condition.
- A fourth socket (`SUBMIT_PORT = 5558`) accepts tasks *while the coordinator is already running*, via a `submission_listener` thread.
- Shutdown is `Ctrl+C` → `KeyboardInterrupt` → `stop_event.set()`, joined cleanly across all four background threads (heartbeat, submission, query, watchdog).

This is the actual conceptual shift the rest of the system depends on: from "run once, process N things, stop" to "run indefinitely, process whatever arrives."

---

## Delivery semantics: at-least-once, not exactly-once

This is the most important design decision in the project, and the one most likely to be hand-waved in a beginner's writeup — so it gets its own section rather than a passing mention.

**The failure case:**
```
worker receives task
       ↓
sends "started" ack → task status becomes "running"
       ↓
run_task() completes successfully
       ↓
worker crashes BEFORE the "result" message reaches the coordinator
       ↓
coordinator sees only silence
       ↓
after HEARTBEAT_TIMEOUT, watchdog marks the worker dead
       ↓
task_007 (last known "running" on that worker) → reset to "pending"
       ↓
requeued, sent to a different worker
       ↓
task executes a SECOND time
```

The coordinator cannot distinguish "this task never ran" from "this task ran, but its worker died before reporting back." Both look identical: silence, followed by timeout. There is no acknowledgment stronger than a heartbeat to close that gap without adding a whole separate consensus mechanism.

**The same risk exists across coordinator restarts, not just worker deaths** — see [Persistence & recovery](#persistence--recovery) below.

**Terminology, precisely:**

| Guarantee | Meaning |
|---|---|
| At-most-once | A task never executes twice, but might be silently lost |
| At-least-once | A task is guaranteed to eventually execute, but might execute more than once |
| Exactly-once | Each task appears to execute exactly once — requires idempotency keys and/or a distributed lock/consensus layer well beyond this project's scope |

**This system is at-least-once by design, not by accident.** The alternative (at-most-once) would mean a worker crash could permanently lose a task, which is worse for this project's purpose — better to risk a duplicate execution than to silently drop work.

**What mitigates the risk here, without fully solving it:**
- `seen_results: set()` — the coordinator deduplicates *results* it has already counted, so a task reported `done` twice doesn't corrupt the completed/failed statistics.
- A `session_stats["duplicate_results"]` counter, exposed via `GET /stats`, makes duplicate detection observable rather than silent.
- The `dispatched`/`running` status split narrows *when* a duplicate is possible — a task that never got a `started` ack was almost certainly never executed, so reassigning it carries essentially no duplication risk; the actual risk window is specifically between `started` and `result`.

**What this deliberately does not attempt:** per-task idempotency keys, worker-side deduplication, or distributed locking. Building true exactly-once semantics would be a legitimate next project on its own; claiming it here without building it would be dishonest.

---

## Persistence & recovery

### Why

Everything above lived only in the coordinator's RAM. Kill the process, and `tasks`, `pending_tasks`, and all worker state vanish — a crash doesn't just interrupt work, it erases the record that the work ever existed.

### Design: SQLite as a durable mirror, not a replacement

```
Task changes
    │
    ├── update tasks{} (in-memory, fast path)
    │
    └── db.save_task() → SQLite (durable path)
```

The in-memory dict stays the primary read path for the query responder and dispatch loop — SQLite is written to on every status transition but never read from during normal operation, only at startup. This keeps the hot path exactly as fast as before; persistence is pure write-through overhead on state-change events, which are infrequent relative to the heartbeat/poll loops.

**Why SQLite, not Redis:** this project's entire premise is building queue mechanics by hand instead of reaching for existing infrastructure. Redis would reintroduce exactly the kind of "use it instead of understanding it" shortcut the project set out to avoid. SQLite is a single file, stdlib-only (`sqlite3`), and durable enough for this scale — the concept (write-through + startup recovery) transfers directly to a "real" datastore if this were ever scaled up.

### Schema

```sql
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    type TEXT,
    payload TEXT,        -- JSON-encoded
    status TEXT,
    worker_id TEXT,
    created_at REAL,
    updated_at REAL
)
```

### Recovery on startup

```
Coordinator starts
       ↓
db.init_db()  — create table if absent
       ↓
db.load_incomplete_tasks()
       ↓
every row where status NOT IN ('done', 'failed')
       ↓
reset to "pending", requeued
       ↓
db.get_highest_task_number()
       ↓
job_counter resumes from there, avoiding ID collisions
```

Any task that was `dispatched` or `running` when the coordinator crashed is treated as `pending` on restart — the coordinator has no way to know whether the worker holding it is still alive and finishing the job, so it makes the same conservative choice as the watchdog does for a dead worker: assume the work needs to happen again. **This is a direct extension of the at-least-once guarantee above, applied to coordinator failures instead of just worker failures — not a separate, weaker guarantee.**

### Verified behavior

```bash
docker compose down     # containers removed, named volume kept
docker compose up       # new coordinator container, same tasks.db mounted, RECOVERED N tasks logged
```
vs.
```bash
docker compose down -v  # volume also removed — tasks.db genuinely gone
```
Both were tested directly, not assumed.

---

## Docker networking

**The problem:** `COORDINATOR_HOST = "localhost"` works fine outside Docker, but inside a container, `localhost` resolves to *that same container* — not a sibling service. A worker container using `localhost` would be trying to connect to itself.

**The fix:** environment-driven configuration.

```python
COORDINATOR_HOST = os.getenv("COORDINATOR_HOST", "localhost")
```

Docker Compose sets `COORDINATOR_HOST=coordinator` for every dependent service; Docker's internal DNS resolves the service name `coordinator` to the correct container's IP on the Compose network. Running locally without Docker, the environment variable is simply absent, so the default `localhost` applies — same code, two environments, no branching logic.

---

## The worker-resurrection bug

A genuine bug, found by testing failure recovery inside Docker rather than by inspection.

**Reproduction:**
```
worker_2 ALIVE
      ↓
docker compose stop worker_2   (heartbeats stop)
      ↓
watchdog: silence > HEARTBEAT_TIMEOUT → worker_2 added to dead_workers
      ↓
docker compose start worker_2   (heartbeats resume)
      ↓
dashboard still shows worker_2 as DEAD, despite "last heartbeat: 0s ago"
```

**Root cause:** `dead_workers` was a write-once set — nothing ever removed a worker_id from it once added, even after fresh heartbeats started arriving again. The watchdog's `if worker_id in dead_workers: continue` guard, which exists to avoid re-logging the same death repeatedly, had the side effect of also permanently suppressing recovery.

**The distinction this forced:** process/container health (is the container running?) is not the same thing as application-level worker membership (does the coordinator currently consider this worker part of the active pool?). A container can restart and be perfectly healthy while the coordinator's internal bookkeeping is still stuck on stale state from before the restart.

**Fix:** the heartbeat handler now removes a worker_id from `dead_workers` whenever a heartbeat is received from it, restoring it to `alive` status. Detection and recovery both live in the heartbeat/watchdog pair rather than being a one-directional latch.

---

## Benchmark methodology

Two workloads exist specifically to avoid contaminating performance numbers with the deliberately-slow, deliberately-flaky demo workload:

| Workload | Sleep | Failure rate | Purpose |
|---|---|---|---|
| Demo (`process_data`) | `random.uniform(8.0, 10.0)`s | 20% | Visible failure/reassignment on the dashboard |
| Benchmark (`"type": "benchmark"`) | fixed 0.1s | 0% | Throughput measurement |

`benchmark.py`:
1. Records existing task IDs already in the system before submitting anything (so old, already-completed tasks from previous runs can't inflate the count).
2. Submits N benchmark tasks through the live API — not by writing directly to the coordinator, so the measurement includes the full real path a task takes.
3. Polls `GET /tasks` until every submitted task reaches a terminal state.
4. Computes elapsed time and throughput from only the tasks it submitted.

### Results (100 tasks)

| Workers | Completed | Failed | Time | Throughput |
|---|---|---|---|---|
| 1 | 100 | 0 | 10.711s | 9.34 tasks/sec |
| 3 | 100 | 0 | 7.361s | 13.58 tasks/sec |

**~1.46× measured speedup, not linear 3×, and not claimed as such.** With a 0.1s task and three workers, fixed overhead (task dispatch, JSON serialization, socket round-trips) makes up a larger fraction of total time than it would for a longer-running task — this is a real, measured number reflecting this specific workload's characteristics, not a general claim about the system's scaling ceiling.

---

## What each phase teaches

| Phase / stage | Question answered |
|---|---|
| 1 | How do multiple workers share a list of jobs safely? |
| 2 | What breaks when they share *other* data, and how do you fix it? |
| 3 | What changes when workers can't share memory at all? |
| 4 | What happens when a worker dies mid-task, and how do you recover? |
| Dynamic submission | How does a batch script become a long-running service? |
| Delivery semantics | Are "sent" and "executed" the same guarantee? (No.) |
| Persistence | What survives a crash, and what has to be rebuilt from a durable log? |
| Docker networking | Why does the same hostname mean something different inside a container? |
| Benchmarking | Does adding workers actually scale linearly, or is that assumed rather than measured? |

These questions, answered in order, form the core of the project's actual engineering content — more than any individual line of code does.