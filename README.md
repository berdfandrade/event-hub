# Event Hub

**Event Hub** is a Python API built with FastAPI that **receives events from other systems via webhooks and processes them asynchronously in the background**, reliably and without blocking the sender.

Instead of making the system sending the event wait for the entire processing workflow to finish, Event Hub immediately responds with `202 Accepted`, stores the event, and delegates the work to a _worker_. If something goes wrong during processing, the event is automatically retried. If it continues to fail, it is moved to a separate queue for inspection and can be manually reprocessed.

## What is it for?

Use Event Hub when multiple services need to notify each other that something has happened. For example:

- An authentication service emits `user.created`, and Event Hub sends a welcome email.
- An online store emits `order.paid`, and Event Hub updates inventory and generates an invoice.
- An external provider (payment gateway, CRM, etc.) sends webhooks that must be handled reliably without losing events.

What it solves:

| Problem                                     | How Event Hub handles it                                                         |
| ------------------------------------------- | -------------------------------------------------------------------------------- |
| Slow processing blocking the event sender   | Immediately responds with `202` and processes the event asynchronously           |
| Temporary failures (API downtime, timeouts) | Automatic retries with increasing delays (exponential backoff)                   |
| Events that keep failing                    | After N attempts, they are moved to a _dead-letter queue_ and marked as `failed` |
| Duplicate webhook deliveries                | The `Idempotency-Key` header prevents the same event from being processed twice  |
| Worker crashes during processing            | The event is returned to the queue when the worker restarts                      |
| "What happened to my event?"                | Every event has a queryable status exposed through the API                       |

## How it works

```text
 External Service                 Event Hub API                     Worker(s)
 ────────────────                 ─────────────                     ─────────
 POST /webhooks/events ───────►  1. Stores event in MongoDB (pending)
                                 2. Adds event ID to the Redis queue
       ◄────── 202 Accepted ──   3. Responds immediately
                                                    ┌──────────►  4. Fetches event from queue
                                                    │             5. Executes handlers
                                                    │             6. Updates status in MongoDB
                                  Retry queue ──────┘                ├─ success → completed
                                  (with delay)    ◄─────────────────┼─ error   → retrying
                                                                     └─ exhausted → failed (dead-letter)
```

- **MongoDB** stores the complete event and its status history (the source of truth).
- **Redis** stores only event IDs in the queues: the main queue, the scheduled retry queue, and the dead-letter queue.
- **Worker** runs as a separate process from the API. You can run as many workers as needed to handle higher workloads.

### Event lifecycle

```text
pending ──► processing ──► completed
                 │
                 ├──► retrying ──► processing ──► ...
                 │
                 └──► failed ──(POST /events/{id}/retry)──► pending
```

| Status       | Meaning                                                                |
| ------------ | ---------------------------------------------------------------------- |
| `pending`    | Received and queued, waiting for a worker                              |
| `processing` | A worker is executing the handlers                                     |
| `retrying`   | Processing failed, but another attempt is scheduled after a delay      |
| `completed`  | All handlers executed successfully                                     |
| `failed`     | All retry attempts were exhausted (or the event could not be enqueued) |

## Running locally

Prerequisites: Python 3.12+ and Docker.

Create and activate a virtual environment:

```bash
python -m venv .venv && source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Create a `.env` file in the project root:

```env
MONGO_URI=mongodb://localhost:27017
MONGO_DATABASE=eventhub
REDIS_URL=redis://localhost:6379/0
```

Start MongoDB and Redis, then run the API and worker in separate terminals:

```bash
docker compose up -d
```

```bash
uvicorn app.main:app --reload
```

```bash
python -m app.workers.event_worker
```

> During development, you can also run the worker inside the API process by setting `RUN_EMBEDDED_WORKER=true`:

```bash
RUN_EMBEDDED_WORKER=true uvicorn app.main:app --reload
```

Interactive API documentation is available at [http://localhost:8000/docs](http://localhost:8000/docs).

## Usage example

Send an event:

```bash
curl -i -X POST http://localhost:8000/webhooks/events \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: order-123" \
  -d '{"name": "order.paid", "source": "shop", "payload": {"order_id": 123}}'
```

Response (`202 Accepted`, with a `Location: /events/<id>` header):

```json
{
  "id": "178f3ad6-0b24-42fa-8589-b6b32effa24b",
  "name": "order.paid",
  "source": "shop",
  "payload": { "order_id": 123 },
  "status": "pending",
  "attempts": 0,
  "last_error": null,
  "idempotency_key": "order-123",
  "created_at": "2026-11-11T12:55:46.456000Z",
  "updated_at": "2026-11-11T12:55:46.456000Z",
  "processed_at": null
}
```

Track the event:

```bash
curl http://localhost:8000/events/178f3ad6-0b24-42fa-8589-b6b32effa24b
```

## Endpoints

| Method | Route                | Description                                                                                                                                                           |
| ------ | -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `POST` | `/webhooks/events`   | Receives an event and responds with `202`. If an existing `Idempotency-Key` is reused, responds with `200` and returns the original event without processing it again |
| `GET`  | `/events`            | Lists events, newest first. Filters: `status`, `name`, `source`, `limit`, `skip`                                                                                      |
| `GET`  | `/events/{id}`       | Retrieves an event's details and current status                                                                                                                       |
| `POST` | `/events/{id}/retry` | Re-enqueues a `failed` event (resets the attempt counter)                                                                                                             |
| `GET`  | `/events/stats`      | Returns event counts by status and queue sizes                                                                                                                        |
| `GET`  | `/health`            | Checks MongoDB and Redis connectivity                                                                                                                                 |

## Creating handlers

A _handler_ is a function that performs the actual work when an event is received. Create a module inside `app/handlers/`:

```python
# app/handlers/orders.py
from app.schemas.event import Event
from app.services.event.handlers import registry


@registry.on("order.paid")
async def update_stock(event: Event):
    order_id = event.payload["order_id"]
    ...  # If an exception is raised, the event will be retried
```

Then import it in `app/handlers/__init__.py`:

```python
from . import default, orders  # noqa: F401
```

Rules:

- A single event can have multiple handlers. They execute sequentially: wildcard (`"*"`) handlers run first, followed by event-specific handlers. Within each group, execution follows registration order. If a handler fails, subsequent handlers are skipped for that attempt.
- Handlers registered with `@registry.on("*")` run for every event (`default.py` uses this to handle logging).
- Any exception or timeout (`WORKER_HANDLER_TIMEOUT`) is treated as a failure and triggers a retry.
- Delivery follows an **at-least-once** guarantee: in rare situations, such as a worker crashing midway through processing, a handler may execute more than once for the same event. Make sure your handlers can safely handle duplicate executions (i.e., make them idempotent).

## Configuration

Environment variables read from `.env` or the system environment:

| Variable                  | Default  | Description                                                    |
| ------------------------- | -------- | -------------------------------------------------------------- |
| `MONGO_URI`               | —        | MongoDB connection URI (required)                              |
| `MONGO_DATABASE`          | —        | Database name (required)                                       |
| `REDIS_URL`               | —        | Redis connection URL (required)                                |
| `EVENT_QUEUE_PREFIX`      | `events` | Prefix used for Redis keys                                     |
| `WORKER_CONCURRENCY`      | `4`      | Number of events processed concurrently by each worker         |
| `WORKER_MAX_ATTEMPTS`     | `5`      | Maximum number of attempts before marking an event as `failed` |
| `WORKER_RETRY_BASE_DELAY` | `2`      | Initial retry delay in seconds (doubles with each attempt)     |
| `WORKER_RETRY_MAX_DELAY`  | `300`    | Maximum delay between retries, in seconds                      |
| `WORKER_HANDLER_TIMEOUT`  | `30`     | Maximum execution time for each handler, in seconds            |
| `RUN_EMBEDDED_WORKER`     | `false`  | Runs the worker inside the API process                         |

## Project structure

```text
app/
├── main.py                  # FastAPI application, lifecycle, and /health
├── controllers/
│   ├── webhook.py           # POST /webhooks/events
│   └── events.py             # Event queries, statistics, and retries
├── schemas/event.py          # Event, EventCreate, and status models
├── services/
│   ├── event/
│   │   ├── event.py         # MongoDB persistence and status transitions
│   │   ├── event_queue.py   # Redis queues (main, retries, dead-letter)
│   │   ├── processor.py     # Executes handlers and determines success/retry/failure
│   │   └── handlers.py      # Handler registry (@registry.on)
│   └── redis/redis.py       # Redis client wrapper
├── handlers/                # Custom event handlers
├── workers/event_worker.py  # Worker process
├── database/                # MongoDB and Redis connections
└── core/settings.py         # Application settings
tests/                       # Tests (pytest + testcontainers)
```

## Tests

Tests spin up temporary MongoDB and Redis instances using _testcontainers_, so Docker must be running:

```bash
pytest
```

> `tests/database/test_mongodb_connection.py` tests the connection to the MongoDB instance configured in `.env` and only passes when that instance is running.
