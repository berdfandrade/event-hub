from motor.motor_asyncio import AsyncIOMotorClient
from app.core.settings import settings


async def test_mongodb_connection():
    client = AsyncIOMotorClient(settings.mongo_uri)

    result = await client.admin.command("ping")

    assert result["ok"] == 1.0

    client.close()
