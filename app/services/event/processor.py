import asyncio
import logging
from uuid import UUID

from app.core.settings import settings
from app.services.event.event import EventService
from app.services.event.event_queue import EventQueue
from app.services.event.handlers import HandlerRegistry

logger = logging.getLogger(__name__)


class EventProcessor:
    """Processa um único evento: roda os handlers e decide entre completed / retry / failed."""

    def __init__(
        self,
        service: EventService,
        queue: EventQueue,
        registry: HandlerRegistry,
        max_attempts: int = settings.worker_max_attempts,
        retry_base_delay: float = settings.worker_retry_base_delay,
        retry_max_delay: float = settings.worker_retry_max_delay,
        handler_timeout: float = settings.worker_handler_timeout,
    ):
        self.service = service
        self.queue = queue
        self.registry = registry
        self.max_attempts = max_attempts
        self.retry_base_delay = retry_base_delay
        self.retry_max_delay = retry_max_delay
        self.handler_timeout = handler_timeout

    def retry_delay(self, attempts: int) -> float:
        """Backoff exponencial: base, 2*base, 4*base... limitado a retry_max_delay."""
        return min(self.retry_base_delay * 2 ** max(attempts - 1, 0), self.retry_max_delay)

    async def process(self, raw_event_id: str):
        try:
            event_id = UUID(raw_event_id)
        except ValueError:
            logger.error("Invalid event id in queue, discarding: %r", raw_event_id)
            return

        event = await self.service.mark_processing(event_id)
        if event is None:
            logger.info("Event %s not found or already finalized, skipping", event_id)
            return

        handlers = self.registry.get(event.name)
        if not handlers:
            logger.warning("No handlers registered for event %r (%s)", event.name, event_id)

        try:
            for handler in handlers:
                await asyncio.wait_for(handler(event), timeout=self.handler_timeout)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__
            await self._handle_failure(event_id, event.attempts, error)
            return

        await self.service.mark_completed(event_id)
        logger.info("Event %s (%s) completed", event_id, event.name)

    async def _handle_failure(self, event_id: UUID, attempts: int, error: str):
        if attempts >= self.max_attempts:
            await self.service.mark_failed(event_id, error)
            await self.queue.dead_letter(str(event_id))
            logger.error(
                "Event %s failed after %d attempts, sent to dead-letter: %s",
                event_id,
                attempts,
                error,
            )
            return

        delay = self.retry_delay(attempts)
        await self.service.mark_retrying(event_id, error)
        await self.queue.schedule_retry(str(event_id), delay)
        logger.warning(
            "Event %s failed (attempt %d/%d), retrying in %.1fs: %s",
            event_id,
            attempts,
            self.max_attempts,
            delay,
            error,
        )
