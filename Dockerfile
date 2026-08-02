FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY distributed_queue/ ./distributed_queue/

ENV PYTHONUNBUFFERED=1

CMD ["python", "distributed_queue/Coordinator.py"]