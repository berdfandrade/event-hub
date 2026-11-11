from typing import Any
from uuid import UUID

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError

from app.schemas.event import Event, EventStatus, utcnow

_DATETIME_FIELDS = ("created_at", "updated_at", "processed_at")


def _to_document(event: Event) -> dict[str, Any]:
    document = event.model_dump(mode="json")
    # Datas ficam como datetime nativo no Mongo para ordenar/filtrar corretamente
    for field in _DATETIME_FIELDS:
        document[field] = getattr(event, field)
    return document


def _to_event(document: dict | None) -> Event | None:
    if document is None:
        return None
    return Event.model_validate(document)


class EventService:
    def __init__(self, database: AsyncIOMotorDatabase):
        self.collection = database["events"]

    async def ensure_indexes(self):
        await self.collection.create_index("id", unique=True)
        await self.collection.create_index([("status", ASCENDING), ("created_at", ASCENDING)])
        await self.collection.create_index("name")
        await self.collection.create_index(
            "idempotency_key",
            unique=True,
            partialFilterExpression={"idempotency_key": {"$type": "string"}},
        )

    # --- CRUD -------------------------------------------------------------

    async def create(self, event: Event) -> Event:
        await self.collection.insert_one(_to_document(event))
        return event

    async def create_or_get(self, event: Event) -> tuple[Event, bool]:
        """
        Cria o evento respeitando a idempotency_key.
        Retorna (evento, criado). Se a chave já existir, retorna o evento original.
        """
        try:
            return await self.create(event), True
        except DuplicateKeyError:
            if event.idempotency_key is None:
                raise
            existing = await self.find_by_idempotency_key(event.idempotency_key)
            if existing is None:
                raise
            return existing, False

    async def find_by_id(self, event_id: UUID) -> Event | None:
        return _to_event(await self.collection.find_one({"id": str(event_id)}))

    async def find_by_idempotency_key(self, key: str) -> Event | None:
        return _to_event(await self.collection.find_one({"idempotency_key": key}))

    async def find_all(
        self,
        status: EventStatus | None = None,
        name: str | None = None,
        source: str | None = None,
        limit: int = 0,
        skip: int = 0,
        newest_first: bool = False,
    ) -> list[Event]:
        query: dict[str, Any] = {}
        if status is not None:
            query["status"] = str(status)
        if name is not None:
            query["name"] = name
        if source is not None:
            query["source"] = source

        cursor = (
            self.collection.find(query)
            .sort("created_at", DESCENDING if newest_first else ASCENDING)
            .skip(skip)
            .limit(limit)
        )
        documents = await cursor.to_list(length=None)
        return [Event.model_validate(document) for document in documents]

    async def update(self, event_id: UUID, event: Event) -> Event | None:
        document = _to_document(event)
        document.pop("id")
        document.pop("created_at")
        document["updated_at"] = utcnow()

        result = await self.collection.update_one({"id": str(event_id)}, {"$set": document})

        if result.matched_count == 0:
            return None

        return await self.find_by_id(event_id)

    async def delete(self, event_id: UUID) -> bool:
        result = await self.collection.delete_one({"id": str(event_id)})
        return result.deleted_count == 1

    async def count_by_status(self) -> dict[str, int]:
        counts = {str(status): 0 for status in EventStatus}
        pipeline = [{"$group": {"_id": "$status", "count": {"$sum": 1}}}]
        async for row in self.collection.aggregate(pipeline):
            counts[row["_id"]] = row["count"]
        return counts

    # --- transições de status (usadas pelo worker) ------------------------

    async def _transition(
        self,
        event_id: UUID,
        from_statuses: list[EventStatus],
        update: dict[str, Any],
    ) -> Event | None:
        update.setdefault("$set", {})["updated_at"] = utcnow()
        document = await self.collection.find_one_and_update(
            {"id": str(event_id), "status": {"$in": [str(s) for s in from_statuses]}},
            update,
            return_document=ReturnDocument.AFTER,
        )
        return _to_event(document)

    async def mark_processing(self, event_id: UUID) -> Event | None:
        """
        Marca como processing e incrementa attempts.
        Retorna None se o evento não existir ou já estiver finalizado
        (completed/failed), o que torna o processamento idempotente.
        `processing` é aceito para reprocessar eventos recuperados após queda do worker.
        """
        return await self._transition(
            event_id,
            [EventStatus.PENDING, EventStatus.RETRYING, EventStatus.PROCESSING],
            {"$set": {"status": str(EventStatus.PROCESSING)}, "$inc": {"attempts": 1}},
        )

    async def mark_completed(self, event_id: UUID) -> Event | None:
        return await self._transition(
            event_id,
            [EventStatus.PROCESSING],
            {
                "$set": {
                    "status": str(EventStatus.COMPLETED),
                    "processed_at": utcnow(),
                    "last_error": None,
                }
            },
        )

    async def mark_retrying(self, event_id: UUID, error: str) -> Event | None:
        return await self._transition(
            event_id,
            [EventStatus.PROCESSING],
            {"$set": {"status": str(EventStatus.RETRYING), "last_error": error}},
        )

    async def mark_failed(
        self,
        event_id: UUID,
        error: str,
        from_statuses: list[EventStatus] | None = None,
    ) -> Event | None:
        return await self._transition(
            event_id,
            from_statuses or [EventStatus.PROCESSING],
            {"$set": {"status": str(EventStatus.FAILED), "last_error": error}},
        )

    async def reset_for_retry(self, event_id: UUID) -> Event | None:
        """Reabre um evento que falhou para ser processado novamente do zero."""
        return await self._transition(
            event_id,
            [EventStatus.FAILED],
            {"$set": {"status": str(EventStatus.PENDING), "attempts": 0}},
        )
