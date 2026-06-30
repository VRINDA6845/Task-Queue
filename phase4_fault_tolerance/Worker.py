import zmq
import time
import random
import logging
import sys
import threading

WORKER_ID = sys.argv[1] if len(sys.argv) > 1 else "worker_1"

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s.%(msecs)03d  [{WORKER_ID}]  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)
 
COORDINATOR_HOST = "localhost"
TASK_PORT = 5555
RESULT_PORT = 5556
HEARTBEAT_PORT = 5557
HEARTBEAT_INTERVAL = 2  

current_task_lock = threading.Lock()
current_task_holder = {"task": None}

# Simulate real work — sleep is longer here so you have time to
# Ctrl+C mid-task and watch fault detection kick in
def run_task(job: dict) -> str:
    time.sleep(random.uniform(8.0, 10.0))
 
    if random.random() < 0.2:
        raise ValueError(f"Simulated failure on {job['id']}")
 
    return "done"
 
 
def heartbeat_sender(context, stop_event):
    heartbeat_socket = context.socket(zmq.PUSH)
    heartbeat_socket.connect(f"tcp://{COORDINATOR_HOST}:{HEARTBEAT_PORT}")
 
    while not stop_event.is_set():
        with current_task_lock:
            task_snapshot = current_task_holder["task"]
 
        heartbeat_socket.send_json({
            "worker_id": WORKER_ID,
            "current_task": task_snapshot,
        })
        time.sleep(HEARTBEAT_INTERVAL)
 
    heartbeat_socket.close()
 
 
def main():
    context = zmq.Context()
 
    task_socket = context.socket(zmq.PULL)
    task_socket.connect(f"tcp://{COORDINATOR_HOST}:{TASK_PORT}")
 
    result_socket = context.socket(zmq.PUSH)
    result_socket.connect(f"tcp://{COORDINATOR_HOST}:{RESULT_PORT}")
 
    log.info(f"Worker started, connected to coordinator at {COORDINATOR_HOST}")
    log.info(f"Heartbeat every {HEARTBEAT_INTERVAL}s on port {HEARTBEAT_PORT}")
    log.info("Waiting for tasks...")
    log.info("-" * 50)
 
    # Start the heartbeat thread — runs independently of task processing
    stop_event = threading.Event()
    threading.Thread(target=heartbeat_sender, args=(context, stop_event), daemon=True).start()
 
    jobs_handled = 0
 
    while True:
        message = task_socket.recv_json()
 
        if message["type"] == "no_more_tasks":
            log.info("Received shutdown signal. Exiting.")
            break
 
        job = message["job"]
        log.info(f"RECEIVED {job['id']}  payload={job['payload']['input']}")
 
        # Record what we're working on so the heartbeat thread can report it
        with current_task_lock:
            current_task_holder["task"] = job
 
        try:
            run_task(job)
            status = "done"
            log.info(f"DONE     {job['id']}")
 
        except ValueError as e:
            status = "failed"
            log.info(f"FAILED   {job['id']}  reason={e}")
 
        # Clear current task now that we're done with it
        with current_task_lock:
            current_task_holder["task"] = None
 
        result_socket.send_json({
            "type": "result",
            "job_id": job["id"],
            "worker_id": WORKER_ID,
            "status": status,
        })
 
        jobs_handled += 1
 
    stop_event.set()
    log.info("-" * 50)
    log.info(f"Worker handled {jobs_handled} jobs total. Shutting down.")
 
    task_socket.close()
    result_socket.close()
    context.term()
 
 
if __name__ == "__main__":
    main()