import time
import json
import urllib.request

API_URL = "http://localhost:8000"

# Start small. Once this works, we'll increase it.
NUM_TASKS = 100

POLL_INTERVAL = 0.2


def submit_task(index):
    """Submit one benchmark task to FastAPI."""

    data = {
        "type": "benchmark",
        "payload": {
            "input": f"benchmark_{index}"
        }
    }

    body = json.dumps(data).encode("utf-8")

    request = urllib.request.Request(
        f"{API_URL}/tasks",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(request) as response:
        return json.loads(response.read().decode("utf-8"))


def get_tasks():
    """Get all tasks currently known by the coordinator."""

    with urllib.request.urlopen(f"{API_URL}/tasks") as response:
        return json.loads(response.read().decode("utf-8"))


def main():

    print("=" * 55)
    print(" DISTRIBUTED TASK QUEUE BENCHMARK")
    print("=" * 55)

    # --------------------------------------------------
    # 1. Record tasks that already existed
    # --------------------------------------------------

    existing_tasks = get_tasks()

    existing_ids = {
        task["id"]
        for task in existing_tasks
    }

    print(f"Existing tasks : {len(existing_ids)}")
    print(f"Submitting     : {NUM_TASKS} benchmark tasks")
    print()

    # --------------------------------------------------
    # 2. Start timer and submit benchmark tasks
    # --------------------------------------------------

    start_time = time.perf_counter()

    for i in range(NUM_TASKS):
        submit_task(i + 1)

    print("All benchmark tasks submitted.")
    print("Waiting for completion...")

    # --------------------------------------------------
    # 3. Wait until OUR benchmark tasks finish
    # --------------------------------------------------

    while True:

        tasks = get_tasks()

        benchmark_tasks = [
            task
            for task in tasks
            if task["id"] not in existing_ids
            and task.get("type") == "benchmark"
        ]

        completed = sum(
            1 for task in benchmark_tasks
            if task["status"] == "done"
        )

        failed = sum(
            1 for task in benchmark_tasks
            if task["status"] == "failed"
        )

        finished = completed + failed

        print(
            f"\rCompleted: {finished}/{NUM_TASKS}",
            end="",
            flush=True
        )

        if len(benchmark_tasks) >= NUM_TASKS and finished >= NUM_TASKS:
            break

        time.sleep(POLL_INTERVAL)

    # --------------------------------------------------
    # 4. Calculate results
    # --------------------------------------------------

    end_time = time.perf_counter()

    elapsed = end_time - start_time

    throughput = NUM_TASKS / elapsed

    print("\n")
    print("=" * 55)
    print(" RESULTS")
    print("=" * 55)

    print(f"Tasks submitted : {NUM_TASKS}")
    print(f"Completed       : {completed}")
    print(f"Failed          : {failed}")
    print(f"Total time      : {elapsed:.3f} seconds")
    print(f"Throughput      : {throughput:.2f} tasks/sec")

    print("=" * 55)


if __name__ == "__main__":
    main()