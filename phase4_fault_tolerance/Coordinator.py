import zmq
import time
import threading
import logging
from collections import deque
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d  [coordinator]  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

TASK_PORT = 5555
RESULT_PORT = 5556
HEARTBEAT_PORT = 5557

TOTAL_JOBS = 30
NUM_WORKERS = 3
HEARTBEAT_TIMEOUT = 5    # seconds of silence before a worker is considered dead
CHECK_INTERVAL = 1       # how often the watchdog thread checks for dead workers

def build_task(job_id: int) -> dict:
    return {
        "id": f"task_{job_id:03d}",
        "type": "process_data",
        "payload": {"input": f"data_{job_id}"},
        "status": "pending",
        "created_at": time.time(),
    }

state = {
    "last_heartbeat": {},      # worker_id -> timestamp we last heard from them
    "current_task": {},        # worker_id -> task they last said they were working on
    "dead_workers": set(),     # worker_ids already declared dead 
}

state_lock = threading.Lock()
pending_tasks = deque()   # tasks waiting to be sent
tasks_lock = threading.Lock()

def heartbeat_listener(context, stop_event):
    heartbeat_socket = context.socket(zmq.PULL)
    heartbeat_socket.bind(f"tcp://*:{HEARTBEAT_PORT}")
 
    poller = zmq.Poller()
    poller.register(heartbeat_socket, zmq.POLLIN)
 
    while not stop_event.is_set():
        events = dict(poller.poll(timeout=500))  
        if heartbeat_socket not in events:
            continue   # nothing arrived in this window, loop back and check stop_event
 
        message = heartbeat_socket.recv_json()
        worker_id = message["worker_id"]
 
        with state_lock:
            state["last_heartbeat"][worker_id] = time.time()
            state["current_task"][worker_id] = message.get("current_task")
 
    heartbeat_socket.close()


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
    log.info(f"Tasks on {TASK_PORT}, results on {RESULT_PORT}, heartbeats on {HEARTBEAT_PORT}")
    log.info(f"Expecting {NUM_WORKERS} workers, heartbeat timeout {HEARTBEAT_TIMEOUT}s")
 
    stop_event = threading.Event()
    heartbeat_thread = threading.Thread(target=heartbeat_listener, args=(context, stop_event), daemon=False)
    heartbeat_thread.start()
    threading.Thread(target=watchdog, args=(stop_event,), daemon=True).start()
 
    log.info("Waiting 2 seconds for workers to connect...")
    time.sleep(15)
    log.info("-" * 50)
 
    with tasks_lock:
        for i in range(1, TOTAL_JOBS + 1):
            pending_tasks.append(build_task(i))
 
    tracker = {"completed": 0, "failed": 0}
    sent_count = 0
    seen_results = set()   # task_ids we've already counted, to avoid double counting
                            # a task that was reassigned and somehow reported twice
 
    while tracker["completed"] + tracker["failed"] < TOTAL_JOBS:
 
        # Send out anything currently pending (new tasks or reassigned ones)
        with tasks_lock:
            while pending_tasks:
                job = pending_tasks.popleft()
                task_socket.send_json({"type": "task", "job": job})
                sent_count += 1
                log.info(f"SENT     {job['id']}  ({sent_count} sent total)")
 
        try:
            message = result_socket.recv_json()
        except zmq.error.Again:
            continue
 
        job_id = message["job_id"]
        worker_id = message["worker_id"]
 
        if job_id in seen_results:
            log.info(f"IGNORED  duplicate result for {job_id} (already counted)")
            continue
        seen_results.add(job_id)
 
        if message["status"] == "done":
            tracker["completed"] += 1
            log.info(f"DONE     {job_id}  worker={worker_id}  "
                     f"({tracker['completed']+tracker['failed']}/{TOTAL_JOBS})")
        else:
            tracker["failed"] += 1
            log.info(f"FAILED   {job_id}  worker={worker_id}  "
                     f"({tracker['completed']+tracker['failed']}/{TOTAL_JOBS})")
 
    stop_event.set()
    heartbeat_thread.join(timeout=2)   
 
    log.info("-" * 50)
    log.info(f"All {TOTAL_JOBS} tasks accounted for")
    log.info(f"Completed : {tracker['completed']}")
    log.info(f"Failed    : {tracker['failed']}")
    log.info(f"Dead workers detected: {state['dead_workers'] or 'none'}")
 
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

