from uuid import UUID
import json
from typing import Any

from motor.motor_asyncio import AsyncIOMotorDatabase

from app.schemas.event import Event
from app.services.redis import RedisService
from app.services.event.event_queue import EventQueue


class EventService:
    def __init__(self, database: AsyncIOMotorDatabase, redis_service: RedisService | None = None):
        self.collection = database["events"]
        self.redis_service = redis_service
        self.queue = EventQueue(redis_service) if redis_service is not None else None

    async def create(self, event: Event) -> Event:
        """
        If Redis is available, publish the event to the queue (fast path).
        Otherwise, persist directly to MongoDB.
        """
        if self.queue is not None:
            await self.queue.publish(event)
            return event

        await self.collection.insert_one(event.model_dump(mode="json"))
        return event

    async def create_from_dict(self, data: dict[str, Any]) -> Event:
        """Validate a dict as an Event and persist it to MongoDB.

        This is intended for workers that consume the queue and then persist
        events to the database.
        """
        event = Event.model_validate(data)
        await self.collection.insert_one(event.model_dump(mode="json"))
        return event

    async def find_by_id(self, event_id: UUID) -> Event | None:
        document = await self.collection.find_one({"id": str(event_id)})

        if document is None:
            return None

        return Event.model_validate(document)

    async def find_all(self) -> list[Event]:
        documents = await self.collection.find().to_list(length=None)
        return [Event.model_validate(document) for document in documents]

    async def update(self, event_id: UUID, event: Event) -> Event | None:
        result = await self.collection.update_one(
            {"id": str(event_id)},
            {
                "$set": event.model_dump(
                    mode="json",
                    exclude={"id", "created_at"},
                )
            },
        )

        if result.matched_count == 0:
            return None

        return await self.find_by_id(event_id)

    async def delete(self, event_id: UUID) -> bool:
        result = await self.collection.delete_one({"id": str(event_id)})
        return result.deleted_count == 1