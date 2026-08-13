from fastapi import Depends
from app.database.mongodb import get_database
from app.database.redis import get_redis
from app.services.redis import RedisService
from app.services.event import EventService


def get_event_service(database=Depends(get_database)):
    """Return an EventService wired with the database and a RedisService.

    Attempt to obtain the global Redis client; if not initialized, fall back
    to providing an EventService that writes directly to MongoDB.
    """
    try:
        redis_client = get_redis()
        redis_service = RedisService(redis_client)
    except Exception:
        redis_service = None

    return EventService(database, redis_service)
