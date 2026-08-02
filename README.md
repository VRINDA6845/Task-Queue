# Distributed Task Queue

A fault-tolerant distributed task processing system built from scratch in Python — no Celery, no Redis, no existing queue library. Coordinator and workers communicate over raw ZeroMQ sockets, state is exposed through a REST API and a live dashboard, and everything is containerized and deployable with Docker Compose.

Started as a threading exercise. Became a real distributed system with fault detection, persistence, and measured performance.

---

## Features

- Distributed worker processes communicating over ZeroMQ (no shared memory)
- Dynamic task submission via REST API — not a fixed batch
- Heartbeat-based worker failure detection and automatic task reassignment
- Explicit at-least-once delivery semantics, with duplicate-result detection
- SQLite persistence with crash recovery (unfinished tasks survive a coordinator restart)
- Live web dashboard (plain HTML/CSS/JS, no framework) showing workers, tasks, and stats in real time
- Fully Dockerized: coordinator, workers, and API each run as separate containers, orchestrated with Docker Compose
- Persistent Docker volume — task state survives container recreation, not just process life
- Benchmarked: measured throughput at 1 vs 3 workers, not estimated

---

## Architecture

```
                         USER / BROWSER
                              │
                              ▼
                    ┌──────────────────┐
                    │  Web Dashboard   │
                    │ HTML / CSS / JS  │
                    └────────┬─────────┘
                             │ HTTP
                             ▼
                    ┌──────────────────┐
                    │     FastAPI      │
                    │    REST API      │
                    └────────┬─────────┘
                             │ ZeroMQ (REQ/REP + PUSH/PULL)
                             ▼
                 ┌─────────────────────────┐
                 │       COORDINATOR       │
                 │                         │
                 │ Queue management        │
                 │ Worker tracking         │
                 │ Failure detection       │
                 │ Task reassignment       │
                 │ State management        │
                 └───┬────────┬────────┬───┘
                     │        │        │
                  ZeroMQ   ZeroMQ   ZeroMQ
                     │        │        │
                     ▼        ▼        ▼
                 ┌──────┐ ┌──────┐ ┌──────┐
                 │ W1   │ │ W2   │ │ W3   │
                 └──────┘ └──────┘ └──────┘

                         │
                         ▼
                  ┌──────────────┐
                  │    SQLite    │
                  │   tasks.db   │
                  └──────────────┘

All components → Docker containers → Docker Compose → persistent volume
```

---

## Communication channels

| Port | Direction | Purpose |
|---|---|---|
| 5555 | Coordinator → Worker (PUSH/PULL) | Task distribution |
| 5556 | Worker → Coordinator (PUSH/PULL) | `started` acks and final results |
| 5557 | Worker → Coordinator (PUSH/PULL) | Heartbeats, every 2s |
| 5558 | External → Coordinator (PUSH/PULL) | Task submission (used by the API and `submit_test.py`) |
| 5559 | FastAPI → Coordinator (REQ/REP) | State queries (`get_tasks`, `get_workers`, `get_stats`) |

Only the FastAPI layer (port 8000) is exposed publicly in a real deployment. The ZeroMQ ports stay internal to the coordinator/worker network — see [Security notes](#security-notes).

---

## Task lifecycle

```
pending → dispatched → running → done
                               ↘ failed
```

| Status | Meaning |
|---|---|
| `pending` | Submitted, waiting to be sent to a worker |
| `dispatched` | Sent to a worker, no confirmation yet that it started |
| `running` | Worker has confirmed it's actively processing (`started` ack received) |
| `done` | Worker completed it successfully |
| `failed` | Worker encountered an error |

A task can also return to `pending` if its worker is marked dead mid-processing (reassignment) or if it was still non-terminal when the coordinator itself restarted (recovery).

---

## Delivery semantics: at-least-once, not exactly-once

**This system does not guarantee exactly-once execution, and doesn't claim to.**

If a worker finishes a task but crashes before its result reaches the coordinator, the coordinator has no way to distinguish "never ran" from "ran, but didn't report back." After `HEARTBEAT_TIMEOUT` seconds of silence, the watchdog marks the worker dead and requeues whatever task it last reported holding — which may cause that task to execute a second time on a different worker.

The same risk applies across coordinator restarts: any task not in a terminal state (`done`/`failed`) when the coordinator crashes is reloaded from SQLite as `pending` and requeued, since the coordinator can't know whether the worker that had it is still alive and finishing the job.

What the system *does* guarantee:
- **At-least-once delivery** — a submitted task will eventually be attempted, even across worker and coordinator failures.
- **Result deduplication for accounting** — the coordinator tracks `seen_results` and a `duplicate_results` counter (exposed via `GET /stats`) so a task reported done twice doesn't corrupt completed/failed totals. This detects *duplicate results*, not necessarily duplicate *executions* — it's possible, though unlikely with PUSH/PULL, for the same result message to be an artifact rather than proof of a second run.

True exactly-once execution would require idempotency keys or a distributed lock per task, which this project deliberately doesn't implement — documenting the tradeoff honestly is the point, not hiding it.

---

## Persistence & recovery

Coordinator state (`tasks`, `pending_tasks`, worker heartbeats) lives in memory for speed. Every task status change is also written through to a `tasks.db` SQLite file — memory is the fast path, SQLite is the durable path.

On startup, the coordinator:
1. Loads every task not in a terminal state (`done`/`failed`) from SQLite, resets it to `pending`, and requeues it — recovering work that was interrupted by a crash.
2. Reads the highest existing task number from the database so new task IDs don't collide with recovered ones.

In Docker, `tasks.db` lives on a named volume (`task_data`), so it survives `docker compose down` / `up` cycles — not just the coordinator process staying alive. It does **not** survive `docker compose down -v`, which explicitly removes volumes; this was verified directly.

---

## REST API

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `/tasks` | Submit a new task: `{"type": "process_data", "payload": {...}}` |
| `GET` | `/tasks` | List all tasks and their current status |
| `GET` | `/tasks/{task_id}` | Get a single task's status |
| `GET` | `/workers` | List all known workers, alive/dead, current task |
| `GET` | `/stats` | Pending/active/completed/failed counts + `duplicate_results` |

Interactive docs at `http://localhost:8000/docs`. The dashboard itself is served from `http://localhost:8000/`.

The API never touches coordinator memory directly — every read goes through a ZeroMQ REQ/REP query channel (port 5559), keeping the coordinator as the single owner of its own state.

---

## Docker

Each component runs as its own container:

```
task-queue-coordinator-1
task-queue-worker_1-1
task-queue-worker_2-1
task-queue-worker_3-1
task-queue-api-1
```

Coordinator host resolution is environment-driven, since `localhost` inside a container refers to that container, not a sibling service:

```python
COORDINATOR_HOST = os.getenv("COORDINATOR_HOST", "localhost")
```

Docker Compose sets `COORDINATOR_HOST=coordinator` for every service, and Docker's internal DNS resolves that service name to the right container. Locally (outside Docker) it falls back to `localhost`.

All services run with `restart: unless-stopped`, so a container crash doesn't require manual intervention.

### Run everything

```bash
docker compose up
```

### Verify

```bash
docker compose ps
```

### Test fault tolerance in Docker

```bash
docker compose stop worker_2
# wait ~5s, check the dashboard — worker_2 should show DEAD, its task reassigned
docker compose start worker_2
# worker_2's heartbeats resume and it's recognized as ALIVE again
```

### Persistence across recreation (not just process life)

```bash
docker compose down     # containers removed, volume kept
docker compose up       # new coordinator container, same tasks.db, unfinished tasks recovered
```

```bash
docker compose down -v  # removes the volume too — tasks.db is actually gone this time
```

---

## Benchmarks

Two separate workloads exist on purpose:

- **Demo workload** — `time.sleep(random.uniform(8.0, 10.0))` + 20% simulated failure rate. Good for watching failure detection and reassignment happen live on the dashboard. Bad for measuring throughput.
- **Benchmark workload** (`"type": "benchmark"`) — deterministic `time.sleep(0.1)`, no artificial failures. Used only for performance measurement.

`benchmark.py` submits N benchmark tasks through the API, polls until they all reach a terminal state, and reports elapsed time / throughput — ignoring any pre-existing tasks already in the database so old runs don't contaminate the numbers.

### Results (100 tasks, measured)

| Workers | Tasks | Completed | Failed | Time | Throughput |
|---|---|---|---|---|---|
| 1 | 100 | 100 | 0 | 10.711s | 9.34 tasks/sec |
| 3 | 100 | 100 | 0 | 7.361s | 13.58 tasks/sec |

~1.46× speedup (not linear 3×) going from 1 to 3 workers — the honest number, not an extrapolated one. Fixed per-task overhead and the deliberately small 0.1s workload mean this isn't a workload where more workers scale linearly; a longer-running or more parallelizable task type would likely show a different ratio.

---

## Known limitations

- **Not exactly-once.** See [Delivery semantics](#delivery-semantics-at-least-once-not-exactly-once) above.
- **Single coordinator, no leader election.** If the coordinator itself is down, no new tasks can be dispatched — there's no failover to a second coordinator instance. This is a single point of failure by design; solving it would mean building distributed consensus, which is explicitly out of scope for this project.
- **SQLite, not built for high write concurrency.** Fine for this project's scale; wouldn't be the right choice for a system handling thousands of writes/sec across multiple coordinator instances.
- **Benchmarks are single-machine.** All measurements were taken with coordinator and workers on the same host (or same Docker network) — no measurement of behavior under real network latency/partition between physically separate machines.

---

## Setup

```bash
git clone https://github.com/VRINDA6845/Task-Queue.git
cd Task-Queue
pip install -r requirements.txt
```

## Running locally (no Docker)

Five terminals, from `distributed_queue/`:

```bash
# Terminal 1
python Coordinator.py

# Terminals 2-4
python Worker.py worker_1
python Worker.py worker_2
python Worker.py worker_3

# Terminal 5
uvicorn api.main:app --reload --port 8000
```

Then open `http://localhost:8000` for the dashboard, or `http://localhost:8000/docs` for the API.

## Running with Docker

```bash
docker compose up
```

Same URLs as above.

---

## Project evolution: Phases 1–4 (the learning journey)

The `distributed_queue/` system above is what phases 1–4 grew into. These four phases are kept in the repo unmodified, as the record of how each concept was introduced one at a time before being combined into the real system.

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

### Phase 1 — Multithreaded Task Queue
`phase1_threads/task_queue.py` — 50 tasks in a `queue.Queue`, 4 worker threads, 20% simulated failure rate, timestamped logs proving concurrent (not sequential) execution.

### Phase 2 — Shared State and Race Conditions
`phase2_shared_state/without_lock.py` vs `with_lock.py` — 16 threads, 200 tasks. Without a lock, totals corrupt every run (losing 50–130 tasks). With one `threading.Lock()`, totals are exactly 200 every time.

```
Thread 1 reads:  completed = 5
Thread 2 reads:  completed = 5    ← before Thread 1 writes back
Thread 1 writes: completed = 6
Thread 2 writes: completed = 6    ← overwrites Thread 1's update
Correct answer: 7. Actual result: 6.
```

### Phase 3 — Separate Processes over ZeroMQ
`phase3_sockets/coordinator.py`, `worker.py` — coordinator and workers as fully separate processes, no shared memory, PUSH/PULL sockets carrying JSON messages.

### Phase 4 — Heartbeat Monitoring and Fault Detection
`phase4_fault_tolerance/coordinator.py`, `worker.py` — heartbeats every 2s carrying the worker's current task, a watchdog thread detecting silence past a timeout, and automatic requeueing of a dead worker's in-flight task.

### Run any phase directly

```bash
python phase1_threads/task_queue.py

python phase2_shared_state/without_lock.py   # see the race condition
python phase2_shared_state/with_lock.py      # see the fix

# Phase 3 (four terminals)
python phase3_sockets/coordinator.py
python phase3_sockets/worker.py worker_1
python phase3_sockets/worker.py worker_2
python phase3_sockets/worker.py worker_3

# Phase 4 (four terminals — Ctrl+C a worker mid-task to trigger fault detection)
python phase4_fault_tolerance/coordinator.py
python phase4_fault_tolerance/worker.py worker_1
python phase4_fault_tolerance/worker.py worker_2
python phase4_fault_tolerance/worker.py worker_3
```

---

## What I learned

- How threads share memory and why that causes race conditions
- Why a read-modify-write on a shared variable isn't atomic, even on one line
- How `threading.Lock` protects critical sections and eliminates data corruption
- How ZeroMQ PUSH/PULL sockets distribute work across separate processes
- Why separate processes require explicit message passing instead of shared memory
- How heartbeat-based fault detection works, and why a heartbeat needs to carry the current task ID (not just a ping) when using PUSH/PULL
- Why task delivery and task execution aren't the same guarantee — and why at-least-once is the honest claim here, not exactly-once
- Why in-memory state disappears on restart, and how to recover it from a durable store
- Why `localhost` means something different inside a container than outside one
- Why a Docker container being disposable means persistent data needs its own volume
- Why 3 workers didn't produce 3× throughput, and what that says about fixed overhead vs. parallelizable work
- The difference between a daemon thread (killed automatically) and a non-daemon thread (must be joined)

---

## Tech stack

```
Language          Python 3.10+
Concurrency        threading, locks, deque
Distributed comms  ZeroMQ (pyzmq)
Backend API        FastAPI, Uvicorn
Frontend           HTML, CSS, JavaScript (no framework)
Persistence        SQLite
Containers          Docker, Docker Compose
Testing/measurement manual failure injection, custom benchmark script
```