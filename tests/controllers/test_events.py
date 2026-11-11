from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from app.dependencies.event import get_event_queue, get_event_service
from app.main import app
from app.schemas.event import Event, EventStatus


@pytest.fixture
def service():
    service = AsyncMock()
    app.dependency_overrides[get_event_service] = lambda: service
    yield service
    app.dependency_overrides.clear()


@pytest.fixture
def queue():
    queue = AsyncMock()
    app.dependency_overrides[get_event_queue] = lambda: queue
    yield queue
    app.dependency_overrides.clear()


@pytest.fixture
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


def make_event(**kwargs) -> Event:
    return Event(name="order.created", payload={}, source="shop", **kwargs)


async def test_get_event(client, service):
    event = make_event()
    service.find_by_id.return_value = event

    response = await client.get(f"/events/{event.id}")

    assert response.status_code == 200
    assert response.json()["id"] == str(event.id)


async def test_get_event_not_found(client, service):
    service.find_by_id.return_value = None

    response = await client.get(f"/events/{uuid4()}")

    assert response.status_code == 404


async def test_list_events_with_filters(client, service):
    service.find_all.return_value = [make_event()]

    response = await client.get("/events", params={"status": "failed", "limit": 10})

    assert response.status_code == 200
    assert len(response.json()) == 1
    kwargs = service.find_all.await_args.kwargs
    assert kwargs["status"] == EventStatus.FAILED
    assert kwargs["limit"] == 10


async def test_stats(client, service, queue):
    service.count_by_status.return_value = {"pending": 1, "completed": 2}
    queue.stats.return_value = {"queued": 1, "scheduled_retries": 0, "dead_letter": 3}

    response = await client.get("/events/stats")

    assert response.status_code == 200
    assert response.json() == {
        "by_status": {"pending": 1, "completed": 2},
        "queued": 1,
        "scheduled_retries": 0,
        "dead_letter": 3,
    }


async def test_retry_failed_event(client, service, queue):
    event = make_event(status=EventStatus.PENDING)
    service.reset_for_retry.return_value = event

    response = await client.post(f"/events/{event.id}/retry")

    assert response.status_code == 202
    queue.remove_dead_letter.assert_awaited_once_with(str(event.id))
    queue.publish.assert_awaited_once_with(str(event.id))


async def test_retry_non_failed_event_returns_409(client, service, queue):
    event = make_event(status=EventStatus.COMPLETED)
    service.reset_for_retry.return_value = None
    service.find_by_id.return_value = event

    response = await client.post(f"/events/{event.id}/retry")

    assert response.status_code == 409
    queue.publish.assert_not_awaited()


async def test_retry_unknown_event_returns_404(client, service, queue):
    service.reset_for_retry.return_value = None
    service.find_by_id.return_value = None

    response = await client.post(f"/events/{uuid4()}/retry")

    assert response.status_code == 404
