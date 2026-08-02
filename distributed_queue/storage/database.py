"""
storage/database.py

Durable mirror of the coordinator's in-memory `tasks` dict.

Design choice: SQLite, not Redis. This project hand-builds the queue
mechanics instead of reaching for Celery, so pulling in a separate
Redis server here would undercut that same goal. SQLite is a single
file, stdlib-only, and durable — enough to survive a coordinator
restart without hiding the concept behind a client library.

This module does NOT replace the in-memory `tasks` dict in
Coordinator.py. The coordinator still reads/writes `tasks` directly
for speed; every write also gets mirrored here so state survives a
crash. Think of this as a write-through cache in reverse — memory is
the fast path, SQLite is the durable path.
"""

import sqlite3
import json
import time
import threading
import os

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

os.makedirs(DATA_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, "tasks.db")

# SQLite handles concurrent writes poorly without serialization, and we
# already have multiple threads (main loop, submission_listener,
# watchdog) writing tasks. One lock, one connection per call — simplest
# thing that's correct, and this isn't a throughput hot path.
_db_lock = threading.Lock()


def init_db():
    """Create the tasks table if it doesn't exist yet. Call once at startup."""
    with _db_lock:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                type TEXT,
                payload TEXT,       -- JSON-encoded
                status TEXT,
                worker_id TEXT,
                created_at REAL,
                updated_at REAL
            )
        """)
        conn.commit()
        conn.close()


def save_task(task: dict):
    """
    Insert or update a task's row. Call this any time task['status']
    (or worker_id) changes in the in-memory registry, right after the
    in-memory update, inside the same lock that protects `tasks`.
    """
    with _db_lock:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""
            INSERT INTO tasks (id, type, payload, status, worker_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                status = excluded.status,
                worker_id = excluded.worker_id,
                updated_at = excluded.updated_at
        """, (
            task["id"],
            task.get("type"),
            json.dumps(task.get("payload", {})),
            task.get("status"),
            task.get("worker_id"),
            task.get("created_at", time.time()),
            time.time(),
        ))
        conn.commit()
        conn.close()


def load_incomplete_tasks() -> list:
    """
    On startup, load every task not in a terminal state (done/failed).

    We can't know whether the worker that had a task is still alive —
    the coordinator that would have found out via heartbeat timeout is
    the thing that just crashed. So every non-terminal task is reset to
    "pending" and requeued. This can produce a duplicate execution if
    the original worker is, in fact, still quietly finishing that task.
    That's a deliberate extension of the at-least-once guarantee from
    Step 10, now covering coordinator restarts too, not just worker
    deaths — see design.md.
    """
    with _db_lock:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM tasks WHERE status NOT IN ('done', 'failed')"
        ).fetchall()
        conn.close()

    recovered = []
    for row in rows:
        recovered.append({
            "id": row["id"],
            "type": row["type"],
            "payload": json.loads(row["payload"]) if row["payload"] else {},
            "status": "pending",   # reset regardless of what it was mid-flight
            "worker_id": None,
            "created_at": row["created_at"],
        })
    return recovered


def load_all_tasks() -> list:
    """Full history, terminal and non-terminal. Useful for API/debugging."""
    with _db_lock:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM tasks").fetchall()
        conn.close()

    all_tasks = []
    for row in rows:
        all_tasks.append({
            "id": row["id"],
            "type": row["type"],
            "payload": json.loads(row["payload"]) if row["payload"] else {},
            "status": row["status"],
            "worker_id": row["worker_id"],
            "created_at": row["created_at"],
        })
    return all_tasks


def get_highest_task_number() -> int:
    """
    So the submission listener's job_counter can resume from where it
    left off after a restart, instead of restarting at task_001 and
    colliding with IDs already recorded in the database.
    """
    with _db_lock:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("SELECT id FROM tasks").fetchall()
        conn.close()

    highest = 0
    for (task_id,) in rows:
        try:
            num = int(task_id.split("_")[1])
            highest = max(highest, num)
        except (IndexError, ValueError):
            continue
    return highest