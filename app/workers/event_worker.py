import asyncio
import logging
import signal
import socket
import os

from app.core.settings import settings
from app.services.event.event_queue import EventQueue
from app.services.event.processor import EventProcessor

logger = logging.getLogger(__name__)


def default_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


class EventWorker:
    """
    Consome a fila de eventos com N consumidores concorrentes e
    um agendador que devolve retries vencidos para a fila.
    """

    def __init__(
        self,
        queue: EventQueue,
        processor: EventProcessor,
        worker_id: str | None = None,
        concurrency: int = settings.worker_concurrency,
        poll_timeout: float = settings.worker_poll_timeout,
        scheduler_interval: float = settings.worker_scheduler_interval,
    ):
        self.queue = queue
        self.processor = processor
        self.worker_id = worker_id or default_worker_id()
        self.concurrency = concurrency
        self.poll_timeout = poll_timeout
        self.scheduler_interval = scheduler_interval
        self._stop = asyncio.Event()

    def stop(self):
        logger.info("Worker %s stopping...", self.worker_id)
        self._stop.set()

    async def run(self):
        recovered = await self.queue.recover(self.worker_id)
        if recovered:
            logger.warning("Recovered %d in-flight events from previous run", recovered)

        logger.info(
            "Worker %s started (concurrency=%d)", self.worker_id, self.concurrency
        )

        tasks = [
            asyncio.create_task(self._consume(i), name=f"consumer-{i}")
            for i in range(self.concurrency)
        ]
        tasks.append(asyncio.create_task(self._schedule(), name="scheduler"))

        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.info("Worker %s stopped", self.worker_id)

    async def _consume(self, index: int):
        while not self._stop.is_set():
            try:
                event_id = await self.queue.reserve(self.worker_id, self.poll_timeout)
            except Exception:
                logger.exception("Consumer %d failed to read from queue", index)
                await self._sleep(1)
                continue

            if event_id is None:
                continue

            try:
                await self.processor.process(event_id)
            except Exception:
                # Erro de infraestrutura (Mongo/Redis). Mantém na lista de processing
                # para ser recuperado no próximo start do worker.
                logger.exception("Unexpected error processing event %s", event_id)
                continue

            await self.queue.ack(self.worker_id, event_id)

    async def _schedule(self):
        while not self._stop.is_set():
            try:
                promoted = await self.queue.promote_due()
                if promoted:
                    logger.debug("Promoted %d retries to the queue", promoted)
            except Exception:
                logger.exception("Scheduler failed to promote retries")
            await self._sleep(self.scheduler_interval)

    async def _sleep(self, seconds: float):
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except TimeoutError:
            pass


def build_worker(database, redis, worker_id: str | None = None) -> EventWorker:
    import app.handlers  # noqa: F401  (registra os handlers)
    from app.services.event import EventService, registry
    from app.services.redis import RedisService

    queue = EventQueue(RedisService(redis))
    processor = EventProcessor(EventService(database), queue, registry)
    return EventWorker(queue, processor, worker_id=worker_id)


async def main():
    from app.database import mongodb
    from app.database import redis as redis_db

    await mongodb.connect_to_mongo()
    await redis_db.connect_to_redis()

    worker = build_worker(mongodb.get_database(), redis_db.get_redis())

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, worker.stop)

    try:
        await worker.run()
    finally:
        await redis_db.disconnect_from_redis()
        await mongodb.disconnect_from_mongo()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(main())
