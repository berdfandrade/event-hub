from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from httpx import ASGITransport, AsyncClient

from app.dependencies.event import get_event_queue, get_event_service
from app.main import app
from app.schemas.event import Event


@pytest.fixture
def mock_event_service():
    service = AsyncMock()
    service.create_or_get.side_effect = lambda event: (event, True)
    app.dependency_overrides[get_event_service] = lambda: service

    yield service

    app.dependency_overrides.clear()


@pytest.fixture
def mock_event_queue():
    queue = AsyncMock()
    app.dependency_overrides[get_event_queue] = lambda: queue

    yield queue

    app.dependency_overrides.clear()


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


PAYLOAD = {"name": "user.created", "payload": {"user_id": 123}, "source": "test"}


async def test_receive_event_enqueues_and_returns_202(
    client, mock_event_service, mock_event_queue
):
    response = await client.post("/webhooks/events", json=PAYLOAD)

    assert response.status_code == 202

    data = response.json()
    assert data["name"] == "user.created"
    assert data["payload"] == {"user_id": 123}
    assert data["source"] == "test"
    assert data["status"] == "pending"
    assert response.headers["Location"] == f"/events/{data['id']}"

    mock_event_service.create_or_get.assert_awaited_once()
    mock_event_queue.publish.assert_awaited_once_with(data["id"])


async def test_receive_event_passes_idempotency_key(
    client, mock_event_service, mock_event_queue
):
    await client.post("/webhooks/events", json=PAYLOAD, headers={"Idempotency-Key": "abc"})

    event: Event = mock_event_service.create_or_get.await_args.args[0]
    assert event.idempotency_key == "abc"


async def test_duplicate_idempotency_key_returns_200_without_enqueue(
    client, mock_event_service, mock_event_queue
):
    existing = Event(**PAYLOAD, idempotency_key="abc")
    mock_event_service.create_or_get.side_effect = None
    mock_event_service.create_or_get.return_value = (existing, False)

    response = await client.post(
        "/webhooks/events", json=PAYLOAD, headers={"Idempotency-Key": "abc"}
    )

    assert response.status_code == 200
    assert response.json()["id"] == str(existing.id)
    mock_event_queue.publish.assert_not_awaited()


async def test_enqueue_failure_returns_503_and_marks_failed(
    client, mock_event_service, mock_event_queue
):
    mock_event_queue.publish.side_effect = ConnectionError("redis down")

    response = await client.post("/webhooks/events", json=PAYLOAD)

    assert response.status_code == 503
    event_id = response.json()["detail"]["event_id"]
    mock_event_service.mark_failed.assert_awaited_once()
    assert mock_event_service.mark_failed.await_args.args[0] == UUID(event_id)


@pytest.mark.parametrize(
    "body",
    [
        {"payload": {}, "source": "test"},
        {"name": "", "payload": {}, "source": "test"},
        {"name": "x", "payload": {}},
    ],
)
async def test_invalid_body_returns_422(client, mock_event_service, mock_event_queue, body):
    response = await client.post("/webhooks/events", json=body)

    assert response.status_code == 422
