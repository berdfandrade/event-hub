import time

from app.core.settings import settings
from app.services.redis import RedisService


class EventQueue:
    """
    Fila confiável de eventos no Redis.

    As mensagens são apenas IDs de eventos; o estado completo fica no MongoDB.

    Chaves:
      {prefix}:queue               -> lista de eventos prontos para processar
      {prefix}:processing:{worker} -> eventos reservados por um worker (ainda sem ack)
      {prefix}:delayed             -> sorted set de retries agendados (score = timestamp)
      {prefix}:dead                -> dead-letter queue (falharam todas as tentativas)
    """

    def __init__(self, redis_service: RedisService, prefix: str | None = None):
        self.redis = redis_service
        self.prefix = prefix or settings.event_queue_prefix

    @property
    def queue_key(self) -> str:
        return f"{self.prefix}:queue"

    @property
    def delayed_key(self) -> str:
        return f"{self.prefix}:delayed"

    @property
    def dead_key(self) -> str:
        return f"{self.prefix}:dead"

    def processing_key(self, worker_id: str) -> str:
        return f"{self.prefix}:processing:{worker_id}"

    async def publish(self, event_id: str):
        await self.redis.push(self.queue_key, str(event_id))

    async def reserve(self, worker_id: str, timeout: float) -> str | None:
        """Bloqueia até `timeout` segundos esperando um evento e o move para a lista de processing."""
        return await self.redis.blocking_move(
            self.queue_key, self.processing_key(worker_id), timeout
        )

    async def ack(self, worker_id: str, event_id: str):
        await self.redis.remove(self.processing_key(worker_id), str(event_id))

    async def recover(self, worker_id: str) -> int:
        """Devolve para a fila eventos que ficaram presos (ex.: worker caiu no meio)."""
        recovered = 0
        while await self.redis.move(self.processing_key(worker_id), self.queue_key):
            recovered += 1
        return recovered

    async def schedule_retry(self, event_id: str, delay: float):
        await self.redis.sorted_add(self.delayed_key, str(event_id), time.time() + delay)

    async def promote_due(self, limit: int = 100) -> int:
        """Move retries cujo horário já chegou de volta para a fila principal."""
        due = await self.redis.sorted_range_by_score(
            self.delayed_key, time.time(), limit
        )
        promoted = 0
        for event_id in due:
            # ZREM só retorna 1 para um único worker, evitando duplicação
            if await self.redis.sorted_remove(self.delayed_key, event_id):
                await self.publish(event_id)
                promoted += 1
        return promoted

    async def dead_letter(self, event_id: str):
        await self.redis.push(self.dead_key, str(event_id))

    async def remove_dead_letter(self, event_id: str):
        await self.redis.remove(self.dead_key, str(event_id), count=0)

    async def stats(self) -> dict[str, int]:
        return {
            "queued": await self.redis.length(self.queue_key),
            "scheduled_retries": await self.redis.sorted_size(self.delayed_key),
            "dead_letter": await self.redis.length(self.dead_key),
        }
