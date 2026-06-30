# Phase 3: Distributed Task Queue with ZeroMQ

## Objective

Replace threads with completely separate processes that communicate using ZeroMQ sockets. Instead of sharing memory, the coordinator and workers exchange JSON messages over the network.

---

## What Changed from Phase 2

In Phase 1 and Phase 2, all threads lived inside one Python process and could access the same memory.

```
Phase 1 & 2 (Threads)

+-----------------------------------+
|          One Python Process       |
|                                   |
|  Thread 1                         |
|  Thread 2  ───► Shared Memory     |
|  Thread 3                         |
+-----------------------------------+
```

In Phase 3, the coordinator and workers are independent processes.

```
Phase 3 (Separate Processes)

           +------------------+
           |  coordinator.py  |
           +------------------+
              ▲            │
    Results   │            │ Tasks
   (PULL)     │            ▼ (PUSH)

     +------------+   +------------+   +------------+
     | worker_1   |   | worker_2   |   | worker_3   |
     +------------+   +------------+   +------------+
```

Since each process has its own memory, they cannot directly access each other's variables. All communication happens through ZeroMQ sockets.

---

## Architecture

```
Coordinator

PUSH Socket (Port 5555)
        │
        ▼
Workers receive tasks

Workers process tasks

PUSH Socket (Port 5556)
        │
        ▼
Coordinator receives results
```

The coordinator:

- Creates tasks
- Sends tasks to workers
- Receives results
- Tracks completed and failed jobs
- Sends shutdown messages when all work is finished

Each worker:

- Waits for a task
- Processes it
- Sends the result back
- Exits when it receives a shutdown message

---

## Message Format

Tasks and results are exchanged as JSON messages.

### Coordinator → Worker

```python
{
    "type": "task",
    "job": {
        ...
    }
}
```

### Shutdown Message

```python
{
    "type": "no_more_tasks"
}
```

### Worker → Coordinator

```python
{
    "type": "result",
    "job_id": "task_001",
    "worker_id": "worker_2",
    "status": "done"
}
```

---

## How to Run

Open **four terminals**.

### Terminal 1

```bash
python coordinator.py
```

### Terminal 2

```bash
python worker.py worker_1
```

### Terminal 3

```bash
python worker.py worker_2
```

### Terminal 4

```bash
python worker.py worker_3
```

The coordinator waits a few seconds before dispatching tasks to allow all workers to connect.

---

## Example Output

Coordinator:

```
SENT     task_001
SENT     task_002
...

DONE     task_004  worker=worker_2
DONE     task_001  worker=worker_1
FAILED   task_007  worker=worker_3
```

Workers:

```
RECEIVED task_014
DONE     task_014

RECEIVED task_021
FAILED   task_021
```

Notice that results arrive out of order because different workers finish at different times.

---

## Concepts Covered

- ZeroMQ PUSH/PULL sockets
- Inter-process communication (IPC)
- JSON message passing
- Coordinator-worker architecture
- Distributed task execution
- Graceful worker shutdown
- Separate processes with independent memory

---

## Current Limitation

The coordinator currently waits a fixed amount of time before sending tasks so workers have time to connect.

A production-ready system would replace this with a worker registration or heartbeat mechanism so tasks are dispatched only after workers announce they are ready.

---

## Key Takeaway

Moving from threads to separate processes changes the communication model completely.

Instead of sharing memory, every piece of information must be sent explicitly as a message. This message-passing model is the foundation of distributed systems, task queues, microservices, and many cloud-based architectures.