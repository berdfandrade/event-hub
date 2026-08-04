from app.schemas.event import Event


class EventService:
    @staticmethod
    def create_event(
        name: str,
        payload: dict,
        source: str,
    ) -> Event:
        return Event(
            name=name,
            payload=payload,
            source=source,
        )
