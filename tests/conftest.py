import pytest
from motor.motor_asyncio import AsyncIOMotorClient
from testcontainers.community.mongodb import MongoDbContainer

MONGO_TEST_VERSION = "mongo:7.0"


@pytest.fixture(scope="session")
def mongodb_container():
    with MongoDbContainer(MONGO_TEST_VERSION) as mongo:
        yield mongo


@pytest.fixture
async def mongodb_client(mongodb_container):
    client = AsyncIOMotorClient(mongodb_container.get_connection_url())

    db = client["test_database"]

    await db["events"].delete_many({})
    yield db

    client.close()


from redis.asyncio import Redis
from testcontainers.community.redis import RedisContainer

from app.services.event import EventQueue
from app.services.redis import RedisService

REDIS_TEST_VERSION = "redis:7-alpine"


@pytest.fixture(scope="session")
def redis_container():
    with RedisContainer(REDIS_TEST_VERSION) as redis:
        yield redis


@pytest.fixture
async def redis_test_client(redis_container):
    client = Redis(
        host=redis_container.get_container_host_ip(),
        port=int(redis_container.get_exposed_port(6379)),
        decode_responses=True,
    )
    await client.flushdb()

    yield client

    await client.flushdb()
    await client.aclose()


@pytest.fixture
def event_queue(redis_test_client):
    return EventQueue(RedisService(redis_test_client), prefix="test-events")
