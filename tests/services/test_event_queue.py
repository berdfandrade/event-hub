import asyncio


async def test_publish_reserve_and_ack(event_queue):
    await event_queue.publish("event-1")

    reserved = await event_queue.reserve("worker-a", timeout=1)

    assert reserved == "event-1"
    assert (await event_queue.stats())["queued"] == 0
    assert await event_queue.redis.length(event_queue.processing_key("worker-a")) == 1

    await event_queue.ack("worker-a", "event-1")

    assert await event_queue.redis.length(event_queue.processing_key("worker-a")) == 0


async def test_reserve_returns_none_on_timeout(event_queue):
    assert await event_queue.reserve("worker-a", timeout=0.1) is None


async def test_queue_is_fifo(event_queue):
    for i in range(3):
        await event_queue.publish(f"event-{i}")

    reserved = [await event_queue.reserve("w", timeout=1) for _ in range(3)]

    assert reserved == ["event-0", "event-1", "event-2"]


async def test_recover_moves_unacked_events_back(event_queue):
    await event_queue.publish("event-1")
    await event_queue.publish("event-2")
    await event_queue.reserve("worker-a", timeout=1)
    await event_queue.reserve("worker-a", timeout=1)

    recovered = await event_queue.recover("worker-a")

    assert recovered == 2
    assert (await event_queue.stats())["queued"] == 2


async def test_schedule_retry_is_promoted_only_when_due(event_queue):
    await event_queue.schedule_retry("later", delay=60)
    await event_queue.schedule_retry("soon", delay=0.05)

    await asyncio.sleep(0.1)
    promoted = await event_queue.promote_due()

    assert promoted == 1
    assert await event_queue.reserve("w", timeout=1) == "soon"
    assert (await event_queue.stats())["scheduled_retries"] == 1


async def test_dead_letter(event_queue):
    await event_queue.dead_letter("bad")
    assert (await event_queue.stats())["dead_letter"] == 1

    await event_queue.remove_dead_letter("bad")
    assert (await event_queue.stats())["dead_letter"] == 0
