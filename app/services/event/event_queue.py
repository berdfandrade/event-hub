import json
from app.schemas.event import Event
from app.services.redis import RedisService


class EventQueue:
    QUEUE_NAME = "event_queue"

    def __init__(self, redis_service: RedisService):
        self.redis = redis_service

        async def publish(self, event: Event):
            data = event.model_dump(mode="json")
            await self.redis.push
