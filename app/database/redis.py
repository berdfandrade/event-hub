from redis.asyncio import Redis
from app.core.settings import settings
import logging

logger = logging.getLogger(__name__)

client: Redis | None = None


def create_redis_client() -> Redis:
    return Redis.from_url(
        settings.redis_url,
        decode_responses=True,
    )


async def connect_to_redis():
    """Initialize a global Redis client for the process."""
    global client

    if client is None:
        client = create_redis_client()

        try:
            # test connection
            await client.ping()
            logger.info("Successfully connected to Redis.")
        except Exception:
            logger.exception("Failed to connect to Redis")
            raise


async def disconnect_from_redis():
    global client

    if client is not None:
        await client.close()
        client = None
        logger.info("Successfully disconnected from Redis.")


def get_redis() -> Redis:
    if client is None:
        raise RuntimeError("Redis client is not initialized")

    return client
