import logging

from redis.asyncio import Redis

from app.core.settings import settings

logger = logging.getLogger(__name__)

client: Redis | None = None


def create_redis_client() -> Redis:
    return Redis.from_url(
        settings.redis_url,
        decode_responses=True,
    )


async def connect_to_redis():
    global client

    client = create_redis_client()

    await client.ping()

    logger.info("Successfully connected to Redis.")


async def disconnect_from_redis():
    global client

    if client is not None:
        await client.aclose()
        client = None

        logger.info("Successfully disconnected from Redis.")


def get_redis() -> Redis:
    if client is None:
        raise RuntimeError("Redis client is not initialized")

    return client
