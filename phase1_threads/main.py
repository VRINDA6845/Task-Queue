import threading
import queue
import time
import random
import logging

# MORE STRUCTURED INFO ABOUT TASK COMPLETION
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d  [%(threadName)-10s]  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# TASK FORMAT WHICH WORKER HAVE TO PERFORM
def build_task(job_id: int) -> dict:
    return {
        "id": f"task_{job_id:03d}",
        "type": "process_data",
        "payload": {"input": f"data_{job_id}"},
        "status": "pending",
        "created_at": time.time(),
    }

# THIS IS WHAT A WORKER ACTUALLY DOES WITH A TASK
# sleeps a random amount between 0.1 to 0.5 and has a 20% chance of raising an exception to stimulate failures
def run_task(job: dict) -> str:
    time.sleep(random.uniform(0.1, 0.5))

    if random.random() < 0.2:
        raise ValueError(f"Task {job['id']} failed: simulated error")

    return "done"

# EACH THREAD RUNS THIS FUNCTION IN A LOOP
# keeps pulling tasks until queue is empty
def process_jobs(job_queue: queue.Queue, tracker: dict):

    while True:
        try:
            job = job_queue.get(block=True, timeout=1)
        except queue.Empty:
            log.info("No more tasks. Worker exiting.")
            break

        job["status"] = "active"
        log.info(f"STARTED  {job['id']}  payload={job['payload']['input']}")
        begin = time.time()

        try:
            run_task(job)
            job["status"] = "done"
            duration = time.time() - begin
            log.info(f"DONE     {job['id']}  ({duration:.2f}s)")
            tracker["completed"] += 1

        except ValueError as e:
            job["status"] = "failed"
            duration = time.time() - begin
            log.info(f"FAILED   {job['id']}  ({duration:.2f}s)  reason={e}")
            tracker["failed"] += 1

        finally:
            job_queue.task_done()


def main():
    TOTAL_JOBS = 50
    TOTAL_WORKERS = 4

    log.info(f"Starting task queue: {TOTAL_JOBS} tasks, {TOTAL_WORKERS} workers")
    log.info("-" * 60)

    job_queue = queue.Queue()

    tracker = {
        "completed": 0,
        "failed": 0,
    }

    # Fill the queue with 50 tasks
    for i in range(1, TOTAL_JOBS + 1):
        job_queue.put(build_task(i))
    log.info(f"Loaded {TOTAL_JOBS} tasks into queue")
    log.info("-" * 60)

    begin_all = time.time()

    # Spin up 4 worker threads
    thread_pool = []
    for i in range(TOTAL_WORKERS):
        t = threading.Thread(
            target=process_jobs,
            args=(job_queue, tracker),
            name=f"Worker-{i+1}",
        )
        thread_pool.append(t)
        t.start()

    # Wait for all tasks to be processed
    job_queue.join()

    for t in thread_pool:
        t.join()

    total_duration = time.time() - begin_all

    log.info("-" * 60)
    log.info(f"All tasks processed in {total_duration:.2f}s")
    log.info(f"Completed : {tracker['completed']}")
    log.info(f"Failed    : {tracker['failed']}")
    log.info(f"Total     : {tracker['completed'] + tracker['failed']}")

    assert tracker["completed"] + tracker["failed"] == TOTAL_JOBS, \
        "BUG: some tasks were lost!"
    log.info("Sanity check passed: no tasks were lost.")


if __name__ == "__main__":
    main()