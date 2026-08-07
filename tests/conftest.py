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
