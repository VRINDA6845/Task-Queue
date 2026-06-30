import zmq
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d  [coordinator]  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

TASK_PORT = 5555      # coordinator pushes tasks out on this port
RESULT_PORT = 5556    # coordinator pulls results in on this port
TOTAL_JOBS = 50
NUM_WORKERS = 3

def build_task(job_id: int) -> dict:
    return {
        "id": f"task_{job_id:03d}",
        "type": "process_data",
        "payload": {"input": f"data_{job_id}"},
        "status": "pending",
        "created_at": time.time(),
    }

def main():
    # ZMQ setup
    context = zmq.Context()
 
    # PUSH socket — coordinator sends tasks out through this
    task_socket = context.socket(zmq.PUSH)
    task_socket.bind(f"tcp://*:{TASK_PORT}")
 
    # PULL socket — coordinator receives results through this
    result_socket = context.socket(zmq.PULL)
    result_socket.bind(f"tcp://*:{RESULT_PORT}")
 
    log.info(f"Coordinator started")
    log.info(f"Pushing tasks on port  {TASK_PORT}")
    log.info(f"Pulling results on port {RESULT_PORT}")
    log.info(f"Waiting 2 seconds for workers to connect...")
    time.sleep(15)   # give workers time to connect before we start sending
    log.info("-" * 50)
 
    # Send all tasks
    log.info(f"Sending {TOTAL_JOBS} tasks...")
    for i in range(1, TOTAL_JOBS + 1):
        job = build_task(i)
        # send_json() automatically converts the Python dictionary to JSON before sending it.
        task_socket.send_json({"type": "task", "job": job})
        log.info(f"SENT     {job['id']}")
 
    log.info("-" * 50)
    log.info("All tasks sent. Waiting for results...")
 
    # Collect results
    tracker = {"completed": 0, "failed": 0}
 
    while tracker["completed"] + tracker["failed"] < TOTAL_JOBS:
        # recv_json blocks until a result arrives
        message = result_socket.recv_json()
 
        if message["status"] == "done":
            tracker["completed"] += 1
            log.info(f"DONE     {message['job_id']}  "
                     f"worker={message['worker_id']}  "
                     f"({tracker['completed'] + tracker['failed']}/{TOTAL_JOBS})")
        else:
            tracker["failed"] += 1
            log.info(f"FAILED   {message['job_id']}  "
                     f"worker={message['worker_id']}  "
                     f"({tracker['completed'] + tracker['failed']}/{TOTAL_JOBS})")
 
    log.info("-" * 50)
    log.info(f"All {TOTAL_JOBS} tasks accounted for")
    log.info(f"Completed : {tracker['completed']}")
    log.info(f"Failed    : {tracker['failed']}")
    
    # Tell every worker there is no more work
    log.info("-" * 50)
    log.info(f"Sending shutdown signal to {NUM_WORKERS} workers...")
    for _ in range(NUM_WORKERS):
        task_socket.send_json({"type": "no_more_tasks"})

    # Clean up sockets
    task_socket.close()
    result_socket.close()
    context.term()
    log.info("Coordinator shut down cleanly.")
 
 
if __name__ == "__main__":
    main()