import asyncio

from app.schemas.event import Event, EventStatus
from app.services.event import EventProcessor, EventService, HandlerRegistry
from app.workers.event_worker import EventWorker


async def wait_for_status(service, event_id, status, timeout=10):
    async def poll():
        while True:
            event = await service.find_by_id(event_id)
            if event and event.status == status:
                return event
            await asyncio.sleep(0.05)

    return await asyncio.wait_for(poll(), timeout)


async def test_worker_processes_events_end_to_end(mongodb_client, event_queue):
    service = EventService(mongodb_client)
    await service.ensure_indexes()
    registry = HandlerRegistry()
    attempts_seen = []

    @registry.on("flaky.event")
    async def flaky(event):
        attempts_seen.append(event.attempts)
        if event.attempts < 2:
            raise RuntimeError("temporary failure")

    @registry.on("ok.event")
    async def ok(event):
        pass

    processor = EventProcessor(
        service, event_queue, registry, max_attempts=3, retry_base_delay=0.1
    )
    worker = EventWorker(
        event_queue,
        processor,
        worker_id="test-worker",
        concurrency=2,
        poll_timeout=0.1,
        scheduler_interval=0.05,
    )
    task = asyncio.create_task(worker.run())

    try:
        ok_event = await service.create(Event(name="ok.event", payload={}, source="t"))
        flaky_event = await service.create(Event(name="flaky.event", payload={}, source="t"))
        await event_queue.publish(str(ok_event.id))
        await event_queue.publish(str(flaky_event.id))

        await wait_for_status(service, ok_event.id, EventStatus.COMPLETED)
        done = await wait_for_status(service, flaky_event.id, EventStatus.COMPLETED)

        assert done.attempts == 2
        assert attempts_seen == [1, 2]
    finally:
        worker.stop()
        await asyncio.wait_for(task, 5)

    stats = await event_queue.stats()
    assert stats == {"queued": 0, "scheduled_retries": 0, "dead_letter": 0}
    assert await event_queue.redis.length(event_queue.processing_key("test-worker")) == 0
