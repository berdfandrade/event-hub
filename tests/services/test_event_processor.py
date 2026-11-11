import asyncio

import pytest

from app.schemas.event import Event, EventStatus
from app.services.event import EventProcessor, EventService, HandlerRegistry


@pytest.fixture
def registry():
    return HandlerRegistry()


@pytest.fixture
def service(mongodb_client):
    return EventService(mongodb_client)


@pytest.fixture
def processor(service, event_queue, registry):
    return EventProcessor(
        service,
        event_queue,
        registry,
        max_attempts=3,
        retry_base_delay=1,
        retry_max_delay=10,
        handler_timeout=0.5,
    )


async def create_event(service, name="user.created") -> Event:
    return await service.create(Event(name=name, payload={"user_id": 1}, source="test"))


async def test_successful_event_is_completed(processor, service, registry):
    calls = []

    @registry.on("user.created")
    async def handler(event):
        calls.append(event.id)

    event = await create_event(service)
    await processor.process(str(event.id))

    stored = await service.find_by_id(event.id)
    assert calls == [event.id]
    assert stored.status == EventStatus.COMPLETED
    assert stored.attempts == 1
    assert stored.processed_at is not None


async def test_wildcard_and_specific_handlers_run(processor, service, registry):
    calls = []

    @registry.on("*")
    async def any_handler(event):
        calls.append("any")

    @registry.on("user.created")
    async def specific(event):
        calls.append("specific")

    @registry.on("other.event")
    async def other(event):
        calls.append("other")

    event = await create_event(service)
    await processor.process(str(event.id))

    assert calls == ["any", "specific"]


async def test_failure_schedules_retry(processor, service, event_queue, registry):
    @registry.on("user.created")
    async def handler(event):
        raise RuntimeError("boom")

    event = await create_event(service)
    await processor.process(str(event.id))

    stored = await service.find_by_id(event.id)
    assert stored.status == EventStatus.RETRYING
    assert stored.last_error == "RuntimeError: boom"
    assert (await event_queue.stats())["scheduled_retries"] == 1


async def test_timeout_counts_as_failure(processor, service, registry):
    @registry.on("user.created")
    async def handler(event):
        await asyncio.sleep(5)

    event = await create_event(service)
    await processor.process(str(event.id))

    stored = await service.find_by_id(event.id)
    assert stored.status == EventStatus.RETRYING
    assert "TimeoutError" in stored.last_error


async def test_exhausted_attempts_go_to_dead_letter(processor, service, event_queue, registry):
    @registry.on("user.created")
    async def handler(event):
        raise ValueError("invalid")

    event = await create_event(service)
    for _ in range(3):
        await processor.process(str(event.id))

    stored = await service.find_by_id(event.id)
    assert stored.status == EventStatus.FAILED
    assert stored.attempts == 3
    assert (await event_queue.stats())["dead_letter"] == 1


async def test_completed_event_is_not_processed_again(processor, service, registry):
    calls = []

    @registry.on("user.created")
    async def handler(event):
        calls.append(1)

    event = await create_event(service)
    await processor.process(str(event.id))
    await processor.process(str(event.id))

    assert calls == [1]


async def test_invalid_id_is_discarded(processor):
    await processor.process("not-a-uuid")


def test_retry_delay_is_exponential_and_capped(processor):
    assert [processor.retry_delay(n) for n in range(1, 6)] == [1, 2, 4, 8, 10]


async def test_reset_for_retry_only_for_failed(service):
    event = await create_event(service)
    assert await service.reset_for_retry(event.id) is None

    await service.mark_failed(event.id, "x", from_statuses=[EventStatus.PENDING])
    reset = await service.reset_for_retry(event.id)

    assert reset.status == EventStatus.PENDING
    assert reset.attempts == 0


async def test_idempotency_key(service):
    first, created = await service.create_or_get(
        Event(name="a", payload={}, source="s", idempotency_key="k1")
    )
    await service.ensure_indexes()
    second, created_again = await service.create_or_get(
        Event(name="a", payload={}, source="s", idempotency_key="k1")
    )

    assert created is True
    assert created_again is False
    assert second.id == first.id
