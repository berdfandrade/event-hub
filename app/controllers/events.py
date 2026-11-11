from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.dependencies.event import get_event_queue, get_event_service
from app.schemas.event import Event, EventStats, EventStatus
from app.services.event import EventQueue, EventService

router = APIRouter(prefix="/events", tags=["events"])


@router.get("", response_model=list[Event])
async def list_events(
    status_filter: EventStatus | None = Query(default=None, alias="status"),
    name: str | None = None,
    source: str | None = None,
    limit: int = Query(default=50, ge=1, le=500),
    skip: int = Query(default=0, ge=0),
    service: EventService = Depends(get_event_service),
):
    return await service.find_all(
        status=status_filter,
        name=name,
        source=source,
        limit=limit,
        skip=skip,
        newest_first=True,
    )


@router.get("/stats", response_model=EventStats)
async def event_stats(
    service: EventService = Depends(get_event_service),
    queue: EventQueue = Depends(get_event_queue),
):
    return EventStats(by_status=await service.count_by_status(), **await queue.stats())


@router.get("/{event_id}", response_model=Event)
async def get_event(
    event_id: UUID,
    service: EventService = Depends(get_event_service),
):
    event = await service.find_by_id(event_id)
    if event is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Event not found")
    return event


@router.post("/{event_id}/retry", response_model=Event, status_code=status.HTTP_202_ACCEPTED)
async def retry_event(
    event_id: UUID,
    service: EventService = Depends(get_event_service),
    queue: EventQueue = Depends(get_event_queue),
):
    """Reenfileira um evento com status `failed`."""
    event = await service.reset_for_retry(event_id)

    if event is None:
        if await service.find_by_id(event_id) is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Event not found")
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Only failed events can be retried",
        )

    await queue.remove_dead_letter(str(event_id))
    await queue.publish(str(event_id))
    return event
