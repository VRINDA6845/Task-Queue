import zmq
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import os

app = FastAPI(title="Distributed Task Queue API")
context = zmq.Context()

COORDINATOR_HOST = os.getenv("COORDINATOR_HOST", "localhost")
SUBMIT_PORT = 5558
QUERY_PORT = 5559


class TaskSubmission(BaseModel):
    type: str
    payload: dict


def submit_task(task_type: str, payload: dict):
    s = context.socket(zmq.PUSH)
    s.connect(f"tcp://{COORDINATOR_HOST}:{SUBMIT_PORT}")
    s.send_json({"type": task_type, "payload": payload})
    s.close()


def query(action: str, **kwargs) -> dict:
    s = context.socket(zmq.REQ)
    s.connect(f"tcp://{COORDINATOR_HOST}:{QUERY_PORT}")
    s.RCVTIMEO = 2000
    s.SNDTIMEO = 2000
    try:
        s.send_json({"action": action, **kwargs})
        result = s.recv_json()
    except zmq.error.Again:
        raise HTTPException(status_code=503, detail="Coordinator did not respond")
    finally:
        s.close()
    return result


@app.post("/tasks")
def create_task(task: TaskSubmission):
    submit_task(task.type, task.payload)
    return {"status": "submitted"}


@app.get("/tasks")
def list_tasks():
    return query("get_tasks")


@app.get("/tasks/{task_id}")
def get_task(task_id: str):
    result = query("get_task", task_id=task_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return result


@app.get("/workers")
def list_workers():
    return query("get_workers")


@app.get("/stats")
def get_stats():
    return query("get_stats")


# Mounted last so it only catches paths not matched by the API routes above
# (e.g. "/", "/index.html"). This serves the dashboard from the same
# server/port as the API, so the browser never has to make a cross-origin
# request — no CORS setup needed.
FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"

app.mount(
    "/",
    StaticFiles(directory=str(FRONTEND_DIR), html=True),
    name="frontend"
)
