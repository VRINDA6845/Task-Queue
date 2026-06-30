import zmq
import time
import random
import logging
import sys

# Worker ID comes from command line so multiple workers have different names
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

# Simulate real work with a random sleep
# 20% chance of failure
def run_task(job: dict) -> str:
    time.sleep(random.uniform(0.1, 0.5))
    
    if random.random() < 0.2:
        raise ValueError(f"Simulated failure on {job['id']}")
 
    return "done"

def main():
    # ZMQ setup
    context = zmq.Context()
 
    # PULL socket — worker receives tasks through this
    task_socket = context.socket(zmq.PULL)
    task_socket.connect(f"tcp://{COORDINATOR_HOST}:{TASK_PORT}")
 
    # PUSH socket — worker sends results through this
    result_socket = context.socket(zmq.PUSH)
    result_socket.connect(f"tcp://{COORDINATOR_HOST}:{RESULT_PORT}")
 
    log.info(f"Worker started, connected to coordinator at {COORDINATOR_HOST}")
    log.info(f"Pulling tasks from port  {TASK_PORT}")
    log.info(f"Pushing results to port  {RESULT_PORT}")
    log.info("Waiting for tasks...")
    log.info("-" * 50)
 
    jobs_handled = 0
 
    while True:
        # recv_json blocks here — worker waits until a task arrives
        message = task_socket.recv_json()
 
        # Coordinator signals no more tasks — exit the loop cleanly
        if message["type"] == "no_more_tasks":
            log.info("Received shutdown signal. Exiting.")
            break
 
        job = message["job"]
        log.info(f"RECEIVED {job['id']}  payload={job['payload']['input']}")
 
        try:
            run_task(job)
            status = "done"
            log.info(f"DONE     {job['id']}")
 
        except ValueError as e:
            status = "failed"
            log.info(f"FAILED   {job['id']}  reason={e}")
 
        # Send result back to coordinator
        result_socket.send_json({
            "type": "result",
            "job_id": job["id"],
            "worker_id": WORKER_ID,
            "status": status,
        })
 
        jobs_handled += 1
 
    log.info("-" * 50)
    log.info(f"Worker handled {jobs_handled} jobs total. Shutting down.")
 
    task_socket.close()
    result_socket.close()
    context.term()
 
 
if __name__ == "__main__":
    main()