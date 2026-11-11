from collections import defaultdict
from collections.abc import Awaitable, Callable

from app.schemas.event import Event

EventHandler = Callable[[Event], Awaitable[None]]

WILDCARD = "*"


class HandlerRegistry:
    """
    Registro de handlers por nome de evento.

        @registry.on("user.created")
        async def send_welcome_email(event: Event): ...

    Handlers registrados em "*" rodam para todos os eventos.
    Se um handler levantar exceção, o evento é reagendado (retry).
    """

    def __init__(self):
        self._handlers: dict[str, list[EventHandler]] = defaultdict(list)

    def on(self, event_name: str) -> Callable[[EventHandler], EventHandler]:
        def decorator(handler: EventHandler) -> EventHandler:
            self.register(event_name, handler)
            return handler

        return decorator

    def register(self, event_name: str, handler: EventHandler):
        if handler not in self._handlers[event_name]:
            self._handlers[event_name].append(handler)

    def get(self, event_name: str) -> list[EventHandler]:
        return [*self._handlers.get(WILDCARD, []), *self._handlers.get(event_name, [])]

    def clear(self):
        self._handlers.clear()


registry = HandlerRegistry()
