import pytest
from httpx import ASGITransport, AsyncClient
from unittest.mock import AsyncMock
from app.main import app

from app.dependencies.event import get_event_service


@pytest.fixture
def mock_event_service():
    service = AsyncMock()
    app.dependency_overrides[get_event_service] = lambda: service

    yield service

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_receive_event(mock_event_service):
    payload = {"name": "user.created", "payload": {"user_id": 123}, "source": "test"}

    mock_event_service.create.return_value = {
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "name": "user.created",
        "payload": {"user_id": 123},
        "source": "test",
    }

    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/webhooks/events", json=payload)

    assert response.status_code == 201

    data = response.json()

    assert data["name"] == "user.created"
    assert data["payload"] == {"user_id": 123}
    assert data["source"] == "test"

    mock_event_service.create.assert_awaited_once()
