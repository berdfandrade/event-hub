import pytest
from redis.asyncio import Redis

from app.services.redis import RedisService


@pytest.fixture
async def redis_client():
    redis = Redis(
        host="localhost",
        port=6379,
    )

    yield redis

    await redis.flushdb()
    await redis.aclose()


@pytest.mark.asyncio
async def test_should_connect_to_redis(redis_client):
    service = RedisService(redis_client)

    await service.set(
        "test:key",
        "hello redis",
    )

    result = await service.get("test:key")

    assert result == "hello redis"
