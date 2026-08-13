from uuid import UUID

from motor.motor_asyncio import AsyncIOMotorDatabase

from app.schemas.event import Event


class EventService:
    def __init__(self, database: AsyncIOMotorDatabase):
        self.collection = database["events"]

    async def create(self, event: Event) -> Event:
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