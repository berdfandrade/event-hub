async def test_should_insert_event(mongodb_client):
    database = mongodb_client.eventhub
    collection = database.events

    event = {
        "name": "user.created",
        "source": "user-service",
        "payload": {
            "user_id": "123",
        },
    }

    result = await collection.insert_one(event)

    assert result.inserted_id is not None

    saved_event = await collection.find_one({"_id": result.inserted_id})

    assert saved_event["name"] == "user.created"
    assert saved_event["source"] == "user-service"
    assert saved_event["payload"]["user_id"] == "123"
