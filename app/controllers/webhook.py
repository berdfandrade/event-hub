from fastapi import APIRouter, Depends, status
from app.schemas.event import Event
from app.dependencies.event import get_event_service
from app.services.event import EventService

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


@router.post("/events", status_code=status.HTTP_201_CREATED)
async def receive_event(
    event: Event, service: EventService = Depends(get_event_service)
):
    created_event = await service.create(event)
    return created_event
