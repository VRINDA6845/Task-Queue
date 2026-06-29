import threading
import queue
import time
import random
import logging

# Define a specific format for logging tasks
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d  [%(threadName)-10s]  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# Creates and returns a task dictionary.
# No processing happens here.
def build_task(job_id: int) -> dict:
    return {
        "id": f"task_{job_id:03d}",
        "type": "process_data",
        "payload": {"input": f"data_{job_id}"},
        "status": "pending",
        "created_at": time.time(),
    }

def run_task(job: dict) -> str:
    time.sleep(random.uniform(0.001, 0.005))
    if random.random() < 0.3:
        raise ValueError(f"{job['id']} failed: simulated error")
    return "done"

# This is a Safe version with lock
# threading.Lock() - makes sure that only one thread can write to tracker at a time
# "with lock:" means acquire the lock, do the work, release it automatically

def process_jobs_safe(job_queue: queue.Queue, tracker: dict, lock: threading.Lock):
 
    while True:
        try:
            job = job_queue.get(block=True, timeout=1)
        except queue.Empty:
            break
 
        try:
            run_task(job)
 
            # only one thread can be inside this block at a time
            with lock:
                current = tracker["completed"]
                # Artificial delay added only to make the race condition easier to reproduce.
                time.sleep(0.0001)
                tracker["completed"] = current + 1
 
        except Exception:
            with lock:
                current = tracker["failed"]
                # Artificial delay added only to make the race condition easier to reproduce.
                time.sleep(0.0001)
                tracker["failed"] = current + 1
 
        finally:
            job_queue.task_done()


def run_with_lock(num_tasks: int, num_workers: int) -> dict:
    job_queue = queue.Queue()
 
    tracker = {"completed": 0,"failed": 0,}
 
    # Single lock shared by all threads
    # whoever holds it can write — everyone else waits
    lock = threading.Lock()
 
    for i in range(1, num_tasks + 1):
        job_queue.put(build_task(i))
 
    thread_pool = []
    for i in range(num_workers):
        t = threading.Thread(
            target=process_jobs_safe,
            args=(job_queue, tracker, lock),
            name=f"Worker-{i+1}",
        )
        thread_pool.append(t)
        t.start()
 
    job_queue.join()
    for t in thread_pool:
        t.join()
 
    return tracker

def main():
    TOTAL_JOBS = 200
    TOTAL_WORKERS = 16
 
    log.info("=" * 60)
    log.info("RUNNING WITH LOCK — all totals should be exactly 200")
    log.info("=" * 60)
 
    wrong_count = 0
    for run in range(8):
        result = run_with_lock(TOTAL_JOBS, TOTAL_WORKERS)
        total = result["completed"] + result["failed"]
        is_wrong = total != TOTAL_JOBS
        if is_wrong:
            wrong_count += 1
        status = "OK" if not is_wrong else f"*** STILL BROKEN *** lost {TOTAL_JOBS - total}"
        log.info(
            f"Run {run+1}: completed={result['completed']:3d}  "
            f"failed={result['failed']:3d}  "
            f"total={total:3d} / {TOTAL_JOBS}  {status}"
        )
 
    log.info("=" * 60)
    log.info(f"{wrong_count} out of 8 runs had corrupted counts")
    if wrong_count == 0:
        log.info("Lock works. Zero data corruption across all runs.")
    log.info("=" * 60)
 
 
if __name__ == "__main__":
    main()
 
 