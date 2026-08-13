"""Worker consumer that drains the Redis-backed event queue and persists events to MongoDB.

Run this as a separate process (e.g. `python -m app.workers.consumer`) alongside the FastAPI app.
"""
import asyncio
import json
import logging
import signal
from typing import Optional

from app.database.mongodb import connect_to_mongo, disconnect_from_mongo, get_database
from app.database.redis import connect_to_redis, disconnect_from_redis, get_redis
from app.services.redis import RedisService
from app.services.event.event import EventService
from app.services.event.event_queue import EventQueue

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

SHUTDOWN = False


def _signal_handler(signum, frame):
    global SHUTDOWN
    logger.info("Received signal %s, shutting down...", signum)
    SHUTDOWN = True


async def main(poll_interval: float = 0.1):
    # connect to services
    await connect_to_mongo()
    await connect_to_redis()

    redis_client = get_redis()
    redis_service = RedisService(redis_client)

    db = get_database()
    service = EventService(db)

    queue_name = EventQueue.QUEUE_NAME

    logger.info("Worker started, listening on queue '%s'", queue_name)

    try:
        while not SHUTDOWN:
            try:
                payload = await redis_service.blocking_pop(queue_name)

                if payload is None:
                    # no item; short sleep to avoid busy loop
                    await asyncio.sleep(poll_interval)
                    continue

                try:
                    data = json.loads(payload)
                except Exception:
                    logger.exception("Failed to decode JSON payload: %s", payload)
                    continue

                try:
                    await service.create_from_dict(data)
                    logger.info("Persisted event from queue: %s", data.get("name"))
                except Exception:
                    logger.exception("Failed to persist event to DB: %s", data)
                    # swallow and continue; could push to a DLQ in future

            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error while consuming queue, sleeping briefly before retry")
                await asyncio.sleep(1)

    finally:
        await disconnect_from_redis()
        await disconnect_from_mongo()
        logger.info("Worker shutdown complete")


if __name__ == "__main__":
    # handle signals for graceful shutdown
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    asyncio.run(main())
