from app.services.event import EventService


def test_should_create_event():
    event = EventService.create_event(
        name="user.created",
        source="user-service",
        payload={
            "user_id": "123",
            "email": "john@example.com",
        },
    )

    assert event.name == "user.created"
    assert event.source == "user-service"
    assert event.payload == {
        "user_id": "123",
        "email": "john@example.com",
    }
    assert event.id is not None
    assert event.created_at is not None
