from fastapi import Depends
from redis.asyncio import Redis

from app.database.mongodb import get_database
from app.database.redis import get_redis
from app.services.event import EventQueue, EventService
from app.services.redis import RedisService


def get_event_service(database=Depends(get_database)) -> EventService:
    return EventService(database)


def get_event_queue(redis: Redis = Depends(get_redis)) -> EventQueue:
    return EventQueue(RedisService(redis))
