from redis.asyncio import Redis


class RedisService:
    def __init__(self, redis: Redis):
        self.redis = redis

    async def set(
        self,
        key: str,
        value: str,
        expire: int | None = None,
    ):
        await self.redis.set(
            key,
            value,
            ex=expire,
        )

    async def get(self, key: str) -> str | None:
        result = await self.redis.get(key)

        if result is None:
            return None

        if isinstance(result, bytes):
            return result.decode("utf-8")

        return result

    async def delete(self, key: str) -> bool:
        result = await self.redis.delete(key)
        return result > 0
