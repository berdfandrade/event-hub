from redis.asyncio import Redis


def _decode(value) -> str | None:
    if value is None:
        return None

    if isinstance(value, bytes):
        return value.decode("utf-8")

    return value


class RedisService:
    def __init__(self, redis: Redis):
        self.redis = redis

    async def ping(self) -> bool:
        return bool(await self.redis.ping())

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
        return _decode(await self.redis.get(key))

    async def delete(self, key: str) -> bool:
        result = await self.redis.delete(key)
        return result > 0

    # --- listas -----------------------------------------------------------

    async def push(self, key: str, value: str):
        await self.redis.rpush(key, value)

    async def blocking_pop(self, key: str) -> str | None:
        result = await self.redis.blpop(key)

        if result is None:
            return None

        _, value = result

        return _decode(value)

    async def blocking_move(
        self, source: str, destination: str, timeout: float
    ) -> str | None:
        """Move atomicamente o primeiro item de `source` para o fim de `destination`."""
        return _decode(
            await self.redis.blmove(source, destination, timeout, "LEFT", "RIGHT")
        )

    async def move(self, source: str, destination: str) -> str | None:
        return _decode(await self.redis.lmove(source, destination, "LEFT", "RIGHT"))

    async def remove(self, key: str, value: str, count: int = 1) -> int:
        return await self.redis.lrem(key, count, value)

    async def length(self, key: str) -> int:
        return await self.redis.llen(key)

    async def list_range(self, key: str, start: int = 0, end: int = -1) -> list[str]:
        return [_decode(v) for v in await self.redis.lrange(key, start, end)]

    # --- sorted sets ------------------------------------------------------

    async def sorted_add(self, key: str, value: str, score: float):
        await self.redis.zadd(key, {value: score})

    async def sorted_range_by_score(
        self, key: str, max_score: float, limit: int = 100
    ) -> list[str]:
        values = await self.redis.zrangebyscore(
            key, "-inf", max_score, start=0, num=limit
        )
        return [_decode(v) for v in values]

    async def sorted_remove(self, key: str, value: str) -> int:
        return await self.redis.zrem(key, value)

    async def sorted_size(self, key: str) -> int:
        return await self.redis.zcard(key)
