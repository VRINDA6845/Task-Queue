# Phase 4: Fault Tolerance with Heartbeats and Dead Worker Detection

## Objective

Extend the distributed task queue built in Phase 3 by adding fault tolerance.

The coordinator continuously monitors worker health using **heartbeats**. If a worker stops responding, the coordinator detects the failure and automatically reassigns the unfinished task to another worker.

---

## What Changed from Phase 3

In Phase 3, workers and the coordinator communicated using two sockets:

* Tasks
* Results

The problem was that if a worker crashed while processing a task, the coordinator would wait forever because that task would never return.

Example:

```
Coordinator → Worker 2 : task_009

Worker 2 crashes

Coordinator:
Waiting...
Waiting...
Waiting...
```

The task is lost permanently.

Phase 4 fixes this problem.

---

## Heartbeat Mechanism

Each worker runs an additional background thread that sends a heartbeat to the coordinator every few seconds.

Heartbeat message:

```python
{
    "worker_id": "worker_1",
    "current_task": {...}    # or None if idle
}
```

The coordinator records:

* when it last heard from every worker
* which task each worker is currently processing

If a worker becomes silent for more than **5 seconds**, it is considered dead.

---

## Architecture

```
                    +----------------------+
                    |    coordinator.py    |
                    +----------------------+
                           ▲        ▲
                           │        │
               Results      │        │ Heartbeats
               (PULL 5556)  │        │ (PULL 5557)
                           │        │
                           │        │
Tasks (PUSH 5555)          │        │
           │               │        │
           ▼               ▼        ▼

      +-----------+   +-----------+   +-----------+
      | worker_1  |   | worker_2  |   | worker_3  |
      +-----------+   +-----------+   +-----------+
```

Communication channels:

* **Port 5555** → Tasks
* **Port 5556** → Results
* **Port 5557** → Heartbeats

---

## Coordinator Responsibilities

The coordinator performs three jobs simultaneously:

### 1. Send Tasks

Creates tasks and sends them to workers through a PUSH socket.

---

### 2. Receive Results

Collects completed or failed task results through a PULL socket.

Tracks:

* completed tasks
* failed tasks
* duplicate results (ignored)

---

### 3. Monitor Worker Health

A watchdog thread continuously checks:

```
Current Time - Last Heartbeat
```

If the silence exceeds the configured timeout:

```
Worker marked DEAD
```

If that worker was processing a task:

```
Task is placed back into the pending queue
```

and sent to another available worker.

---

## Worker Responsibilities

Each worker performs two jobs concurrently.

### Main Thread

* Waits for tasks
* Processes tasks
* Sends results back to the coordinator

---

### Heartbeat Thread

Every 2 seconds it sends:

* worker ID
* current task (or `None` if idle)

This allows the coordinator to know both:

* that the worker is alive
* which task would need reassignment if the worker crashes

---

## Failure Recovery

Example:

```
Coordinator sends task_009

↓

Worker_1 starts processing

↓

Worker_1 crashes

↓

Heartbeat stops

↓

Coordinator waits 5 seconds

↓

Worker_1 marked DEAD

↓

task_009 added back to queue

↓

Worker_2 receives task_009

↓

Task completes successfully
```

---

## How to Run

Open **four terminals**.

### Terminal 1

```bash
python phase4_fault_tolerance/coordinator.py
```

---

### Terminal 2

```bash
python phase4_fault_tolerance/worker.py worker_1
```

---

### Terminal 3

```bash
python phase4_fault_tolerance/worker.py worker_2
```

---

### Terminal 4

```bash
python phase4_fault_tolerance/worker.py worker_3
```

---

## Testing Fault Tolerance

To simulate a worker failure:

1. Start the coordinator and all workers.
2. Wait until a worker receives a task.
3. Press **Ctrl + C** in that worker's terminal while it is processing the task.

The coordinator should report messages similar to:

```
Worker worker_1 silent for 5.2s — marking DEAD

Reassigning task_009 (was on worker_1)

SENT task_009
```

The reassigned task will then be processed by another active worker.

---

## Concepts Learned

* Fault tolerance in distributed systems
* Heartbeat-based health monitoring
* Background threads
* Concurrent monitoring using watchdog threads
* Detecting failed workers
* Automatic task reassignment
* Safe sharing of state using locks
* Building a resilient coordinator-worker architecture

---

## Key Takeaway

Distributed systems must assume that workers can fail at any time.

Instead of waiting forever for a missing result, the coordinator continuously monitors worker health through heartbeats. If a worker stops responding, its unfinished task is automatically returned to the queue and processed by another worker.

This simple heartbeat mechanism is the foundation of fault tolerance used in many real-world distributed systems and task queues.
