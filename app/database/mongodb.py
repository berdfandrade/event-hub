import asyncio
import logging

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from app.core.settings import settings

logger = logging.getLogger(__name__)

client: AsyncIOMotorClient | None = None


async def connect_to_mongo():
    global client

    client = AsyncIOMotorClient(settings.mongo_uri)

    await client.admin.command("ping")

    logger.info("Successfully connected to MongoDB.")


async def disconnect_from_mongo():
    global client

    if client is not None:
        client.close()

        logger.info("Successfully disconnected from MongoDB.")


def get_database() -> AsyncIOMotorDatabase:
    if client is None:
        raise RuntimeError("MongoDB client is not initialized")

    return client[settings.mongo_database]


async def main():
    try:
        await connect_to_mongo()
    finally:
        await disconnect_from_mongo()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    asyncio.run(main())
