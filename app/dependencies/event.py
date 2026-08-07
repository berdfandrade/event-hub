from fastapi import Depends
from app.database.mongodb import get_database
from app.services.event import EventService


def get_event_service(database=Depends(get_database)):
    return EventService(database)
