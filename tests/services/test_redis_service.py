import pytest
from redis.asyncio import Redis
from testcontainers.community.redis import RedisContainer

from app.services.redis import RedisService


@pytest.fixture
async def redis_client():
    with RedisContainer("redis:7-alpine") as redis_container:
        redis = Redis(
            host=redis_container.get_container_host_ip(),
            port=int(redis_container.get_exposed_port(6379)),
        )

        yield redis

        await redis.aclose()


@pytest.mark.asyncio
async def test_should_set_and_get_value(redis_client):
    service = RedisService(redis_client)

    await service.set(
        "user:1",
        "Bernardo",
    )

    result = await service.get("user:1")

    assert result == "Bernardo"


@pytest.mark.asyncio
async def test_should_return_none_when_key_not_exists(redis_client):
    service = RedisService(redis_client)

    result = await service.get("invalid:key")

    assert result is None


@pytest.mark.asyncio
async def test_should_delete_key(redis_client):
    service = RedisService(redis_client)

    await service.set(
        "user:1",
        "Bernardo",
    )

    deleted = await service.delete("user:1")

    assert deleted is True

    result = await service.get("user:1")

    assert result is None


@pytest.mark.asyncio
async def test_should_set_key_with_expiration(redis_client):
    service = RedisService(redis_client)

    await service.set(
        "temporary:key",
        "value",
        expire=10,
    )

    result = await service.get("temporary:key")

    assert result == "value"
