import logging

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status

from app.dependencies.event import get_event_queue, get_event_service
from app.schemas.event import Event, EventCreate, EventStatus
from app.services.event import EventQueue, EventService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


@router.post(
    "/events",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=Event,
    responses={
        200: {"description": "Evento já recebido antes (mesma Idempotency-Key)"},
        503: {"description": "Evento salvo, mas não foi possível enfileirá-lo"},
    },
)
async def receive_event(
    body: EventCreate,
    response: Response,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    service: EventService = Depends(get_event_service),
    queue: EventQueue = Depends(get_event_queue),
):
    """
    Recebe um evento e o enfileira para processamento assíncrono.
    Responde imediatamente com 202; acompanhe o status em `GET /events/{id}`.
    """
    event = Event(**body.model_dump(), idempotency_key=idempotency_key)

    event, created = await service.create_or_get(event)

    if not created:
        response.status_code = status.HTTP_200_OK
        return event

    try:
        await queue.publish(str(event.id))
    except Exception as exc:
        logger.exception("Failed to enqueue event %s", event.id)
        await service.mark_failed(
            event.id, f"Enqueue failed: {exc}", from_statuses=[EventStatus.PENDING]
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "message": "Event stored but could not be queued; retry via POST /events/{id}/retry",
                "event_id": str(event.id),
            },
        )

    response.headers["Location"] = f"/events/{event.id}"
    return event
