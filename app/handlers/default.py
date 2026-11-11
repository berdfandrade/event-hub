import logging

from app.schemas.event import Event
from app.services.event.handlers import WILDCARD, registry

logger = logging.getLogger(__name__)


@registry.on(WILDCARD)
async def log_event(event: Event):
    logger.info(
        "Processing event %s name=%s source=%s attempt=%d",
        event.id,
        event.name,
        event.source,
        event.attempts,
    )


# Exemplo de handler específico:
#
# @registry.on("user.created")
# async def send_welcome_email(event: Event):
#     await email_client.send(to=event.payload["email"], template="welcome")
