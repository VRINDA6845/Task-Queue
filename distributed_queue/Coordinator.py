import zmq
import time
import threading
import logging
from collections import deque

from storage import database as db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d  [coordinator]  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

TASK_PORT = 5555
RESULT_PORT = 5556
HEARTBEAT_PORT = 5557
SUBMIT_PORT = 5558
QUERY_PORT = 5559

NUM_WORKERS = 3
HEARTBEAT_TIMEOUT = 5    # seconds of silence before a worker is considered dead
CHECK_INTERVAL = 1       # how often the watchdog thread checks for dead workers

state = {
    "last_heartbeat": {},      # worker_id -> timestamp we last heard from them
    "current_task": {},        # worker_id -> task they last said they were working on
    "dead_workers": set(),     # worker_ids already declared dead
}

state_lock = threading.Lock()
pending_tasks = deque()   # tasks waiting to be sent
tasks_lock = threading.Lock()
tasks = {}   # task_id -> full task information
tasks_registry_lock = threading.Lock()

# Duplicate-result count needs to be visible from query_responder's thread,
# so it can't just live as a local variable inside main()'s loop.
session_stats = {"duplicate_results": 0}
session_stats_lock = threading.Lock()


def heartbeat_listener(context, stop_event):
    heartbeat_socket = context.socket(zmq.PULL)
    heartbeat_socket.bind(f"tcp://*:{HEARTBEAT_PORT}")

    poller = zmq.Poller()
    poller.register(heartbeat_socket, zmq.POLLIN)

    while not stop_event.is_set():
        events = dict(poller.poll(timeout=500))

        if heartbeat_socket not in events:
            continue

        message = heartbeat_socket.recv_json()
        worker_id = message["worker_id"]

        with state_lock:

            # Worker has returned after previously being declared dead
            if worker_id in state["dead_workers"]:
                state["dead_workers"].remove(worker_id)
                log.info(f"RECOVERED {worker_id}")

            state["last_heartbeat"][worker_id] = time.time()
            state["current_task"][worker_id] = message.get("current_task")

    heartbeat_socket.close()


def submission_listener(context, stop_event, starting_counter=0):
    submit_socket = context.socket(zmq.PULL)
    submit_socket.bind(f"tcp://*:{SUBMIT_PORT}")

    poller = zmq.Poller()
    poller.register(submit_socket, zmq.POLLIN)

    # Resumes from the highest task number already in the database,
    # so a restart doesn't hand out task_001 again and collide with
    # an ID that's already on record.
    job_counter = starting_counter

    while not stop_event.is_set():
        events = dict(poller.poll(timeout=500))

        if submit_socket not in events:
            continue

        message = submit_socket.recv_json()

        job_counter += 1

        task = {
            "id": f"task_{job_counter:03d}",
            "type": message["type"],
            "payload": message["payload"],
            "status": "pending",
            "created_at": time.time(),
        }

        with tasks_lock:
            pending_tasks.append(task)
        with tasks_registry_lock:
            tasks[task["id"]] = task.copy()
        db.save_task(task)
        log.info(f"SUBMITTED {task['id']}")

    submit_socket.close()


def query_responder(context, stop_event):
    """
    REQ/REP socket so FastAPI (or anything else) can ask "what's your
    current state?" and get a synchronous answer back. Unlike the
    PUSH/PULL sockets used elsewhere, REP guarantees exactly one reply
    per request, which is what an HTTP GET needs.
    """
    query_socket = context.socket(zmq.REP)
    query_socket.bind(f"tcp://*:{QUERY_PORT}")

    poller = zmq.Poller()
    poller.register(query_socket, zmq.POLLIN)

    while not stop_event.is_set():
        events = dict(poller.poll(timeout=500))
        if query_socket not in events:
            continue

        request = query_socket.recv_json()
        action = request.get("action")

        if action == "get_tasks":
            with tasks_registry_lock:
                response = list(tasks.values())

        elif action == "get_task":
            with tasks_registry_lock:
                response = tasks.get(request.get("task_id"))

        elif action == "get_workers":
            with state_lock:
                response = [
                    {
                        "worker_id": wid,
                        "status": "dead" if wid in state["dead_workers"] else "alive",
                        "current_task": state["current_task"].get(wid),
                        "last_heartbeat": state["last_heartbeat"].get(wid),
                    }
                    for wid in state["last_heartbeat"]
                ]

        elif action == "get_stats":
            with tasks_registry_lock:
                statuses = [t["status"] for t in tasks.values()]
            with session_stats_lock:
                dup_count = session_stats["duplicate_results"]
            response = {
                "pending": statuses.count("pending"),
                # "active" covers both "dispatched, not yet started" and
                # "confirmed running" — the dashboard doesn't need the finer
                # distinction yet, though it's tracked per-task in get_tasks.
                "active": statuses.count("dispatched") + statuses.count("running"),
                "completed": statuses.count("done"),
                "failed": statuses.count("failed"),
                "duplicate_results": dup_count,
            }

        else:
            response = {"error": f"unknown action: {action}"}

        # REP sockets require exactly one send per recv, even on error paths,
        # or the socket gets stuck in a bad state for the next request.
        query_socket.send_json(response)

    query_socket.close()


def watchdog(stop_event):
    while not stop_event.is_set():
        time.sleep(CHECK_INTERVAL)
        now = time.time()

        with state_lock:
            for worker_id, last_seen in list(state["last_heartbeat"].items()):
                if worker_id in state["dead_workers"]:
                    continue

                silence = now - last_seen
                if silence > HEARTBEAT_TIMEOUT:
                    log.info(f"!! Worker {worker_id} silent for {silence:.1f}s — marking DEAD")
                    state["dead_workers"].add(worker_id)

                    lost_task = state["current_task"].get(worker_id)

                    if lost_task:
                        log.info(f"!! Reassigning {lost_task['id']} (was on {worker_id})")

                        with tasks_registry_lock:
                            if lost_task["id"] in tasks:
                                tasks[lost_task["id"]]["status"] = "pending"
                                db.save_task(tasks[lost_task["id"]])

                        with tasks_lock:
                            pending_tasks.append(lost_task)

                        state["current_task"][worker_id] = None


def main():
    context = zmq.Context()

    task_socket = context.socket(zmq.PUSH)
    task_socket.bind(f"tcp://*:{TASK_PORT}")

    result_socket = context.socket(zmq.PULL)
    result_socket.bind(f"tcp://*:{RESULT_PORT}")
    result_socket.RCVTIMEO = 500

    log.info("Coordinator started")

    # --- Step 11: persistence + recovery ---
    db.init_db()

    recovered = db.load_incomplete_tasks()
    if recovered:
        with tasks_lock, tasks_registry_lock:
            for task in recovered:
                pending_tasks.append(task)
                tasks[task["id"]] = task
        log.info(f"RECOVERED {len(recovered)} incomplete task(s) from previous session: "
                  f"{[t['id'] for t in recovered]}")
    else:
        log.info("No incomplete tasks found in database — starting clean")

    starting_counter = db.get_highest_task_number()
    # --- end Step 11 setup ---

    log.info(
        f"Tasks on {TASK_PORT}, results on {RESULT_PORT}, "
        f"heartbeats on {HEARTBEAT_PORT}, submissions on {SUBMIT_PORT}, "
        f"queries on {QUERY_PORT}"
    )
    log.info(f"Expecting {NUM_WORKERS} workers, heartbeat timeout {HEARTBEAT_TIMEOUT}s")

    stop_event = threading.Event()

    heartbeat_thread = threading.Thread(target=heartbeat_listener, args=(context, stop_event), daemon=False)
    heartbeat_thread.start()

    submission_thread = threading.Thread(
        target=submission_listener, args=(context, stop_event, starting_counter), daemon=False
    )
    submission_thread.start()

    query_thread = threading.Thread(target=query_responder, args=(context, stop_event), daemon=False)
    query_thread.start()

    watchdog_thread = threading.Thread(target=watchdog, args=(stop_event,), daemon=False)
    watchdog_thread.start()

    log.info("-" * 50)
    log.info("Waiting for task submissions on port 5558...")

    tracker = {"completed": 0, "failed": 0, "duplicate_results": 0}
    sent_count = 0
    seen_results = set()   # task_ids we've already counted, to avoid double counting
                            # a task that was reassigned and somehow reported twice

    try:
        while not stop_event.is_set():

            # Send out anything currently pending (new tasks, reassigned
            # ones, or tasks recovered from the database on startup)
            with tasks_lock:
                while pending_tasks:
                    job = pending_tasks.popleft()

                    with tasks_registry_lock:
                        if job["id"] in tasks:
                            tasks[job["id"]]["status"] = "dispatched"
                            db.save_task(tasks[job["id"]])

                    task_socket.send_json({"type": "task", "job": job})

                    sent_count += 1
                    log.info(f"SENT     {job['id']}  ({sent_count} sent total)")

            try:
                message = result_socket.recv_json()
            except zmq.error.Again:
                continue

            msg_type = message.get("type", "result")
            job_id = message["job_id"]
            worker_id = message["worker_id"]

            if msg_type == "started":
                # Worker has confirmed it began executing the task.
                # This does NOT affect seen_results/tracker — it's purely
                # an observability signal, distinguishing "dispatched but
                # maybe never started" from "confirmed running" so we know
                # more precisely what state a task was in if its worker dies.
                with tasks_registry_lock:
                    if job_id in tasks:
                        tasks[job_id]["status"] = "running"
                        tasks[job_id]["worker_id"] = worker_id
                        db.save_task(tasks[job_id])
                log.info(f"RUNNING  {job_id} worker={worker_id}")
                continue

            # msg_type == "result" from here on
            if job_id in seen_results:
                tracker["duplicate_results"] += 1
                with session_stats_lock:
                    session_stats["duplicate_results"] += 1
                log.info(
                    f"IGNORED  duplicate result for {job_id} (already counted) "
                    f"— duplicate_results={tracker['duplicate_results']}"
                )
                continue
            seen_results.add(job_id)

            with tasks_registry_lock:
                if job_id in tasks:
                    tasks[job_id]["status"] = message["status"]
                    tasks[job_id]["worker_id"] = worker_id
                    db.save_task(tasks[job_id])

            if message["status"] == "done":
                tracker["completed"] += 1
                log.info(
                    f"DONE     {job_id} worker={worker_id} "
                    f"(completed={tracker['completed']}, failed={tracker['failed']})"
                )
            else:
                tracker["failed"] += 1
                log.info(
                    f"FAILED   {job_id} worker={worker_id} "
                    f"(completed={tracker['completed']}, failed={tracker['failed']})"
                )

    except KeyboardInterrupt:
        log.info("Ctrl+C received — shutting down...")

    finally:
        stop_event.set()

    heartbeat_thread.join(timeout=2)
    submission_thread.join(timeout=2)
    query_thread.join(timeout=2)
    watchdog_thread.join(timeout=2)

    log.info("-" * 50)
    log.info("Final session statistics:")
    log.info(f"Completed : {tracker['completed']}")
    log.info(f"Failed    : {tracker['failed']}")
    log.info(f"Duplicate results detected: {tracker['duplicate_results']}")
    log.info(f"Dead workers detected: {state['dead_workers'] or 'none'}")

    log.info("Task registry:")
    with tasks_registry_lock:
        for task_id, task in tasks.items():
            log.info(
                f"{task_id}: status={task['status']}, "
                f"worker={task.get('worker_id', 'none')}"
            )

    log.info("-" * 50)
    log.info(f"Sending shutdown signal to {NUM_WORKERS} workers...")
    for _ in range(NUM_WORKERS):
        task_socket.send_json({"type": "no_more_tasks"})

    task_socket.close()
    result_socket.close()
    context.term()
    log.info("Coordinator shut down cleanly.")


if __name__ == "__main__":
    main()