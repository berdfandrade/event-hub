from uuid import uuid4

import pytest

from app.schemas.event import Event
from app.services.event import EventService


@pytest.mark.asyncio
async def test_should_create_event(mongodb_client):
    service = EventService(mongodb_client)

    event = Event(
        name="User Created",
        payload={"user_id": 123},
        source="auth-service",
    )

    created_event = await service.create(event)

    assert created_event.id == event.id
    assert created_event.name == "User Created"


@pytest.mark.asyncio
async def test_should_find_event_by_id(mongodb_client):
    service = EventService(mongodb_client)
    print(type(mongodb_client))
    event = Event(
        name="Order Created",
        payload={"order_id": 456},
        source="order-service",
    )

    await service.create(event)

    found_event = await service.find_by_id(event.id)

    assert found_event is not None
    assert found_event.id == event.id
    assert found_event.name == "Order Created"


@pytest.mark.asyncio
async def test_should_return_none_when_event_not_found(mongodb_client):
    service = EventService(mongodb_client)

    result = await service.find_by_id(uuid4())

    assert result is None


@pytest.mark.asyncio
async def test_should_find_all_events(mongodb_client):
    service = EventService(mongodb_client)

    event1 = Event(
        name="Event 1",
        payload={"value": 1},
        source="service-a",
    )

    event2 = Event(
        name="Event 2",
        payload={"value": 2},
        source="service-b",
    )

    await service.create(event1)
    await service.create(event2)

    events = await service.find_all()

    assert len(events) == 2
    assert events[0].name == "Event 1"
    assert events[1].name == "Event 2"


@pytest.mark.asyncio
async def test_should_update_event(mongodb_client):
    service = EventService(mongodb_client)

    event = Event(
        name="Old Name",
        payload={"old": True},
        source="service",
    )

    await service.create(event)

    updated_event = Event(
        name="New Name",
        payload={"updated": True},
        source="new-service",
    )

    result = await service.update(
        event.id,
        updated_event,
    )

    assert result is not None
    assert result.name == "New Name"
    assert result.payload == {"updated": True}

    assert result.id == event.id


@pytest.mark.asyncio
async def test_should_return_none_when_updating_non_existing_event(mongodb_client):
    service = EventService(mongodb_client)

    result = await service.update(
        uuid4(),
        Event(
            name="Does not exist",
            payload={},
            source="test",
        ),
    )

    assert result is None


@pytest.mark.asyncio
async def test_should_delete_event(mongodb_client):
    service = EventService(mongodb_client)

    event = Event(
        name="Delete Me",
        payload={},
        source="test",
    )

    await service.create(event)

    deleted = await service.delete(event.id)

    assert deleted is True

    found_event = await service.find_by_id(event.id)

    assert found_event is None


@pytest.mark.asyncio
async def test_should_return_false_when_deleting_non_existing_event(mongodb_client):
    service = EventService(mongodb_client)

    result = await service.delete(uuid4())

    assert result is False
