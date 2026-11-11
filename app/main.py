import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from app.controllers import events, webhook
from app.core.settings import settings
from app.database import mongodb
from app.database import redis as redis_db
from app.services.event import EventService

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await mongodb.connect_to_mongo()
    await redis_db.connect_to_redis()
    await EventService(mongodb.get_database()).ensure_indexes()

    worker = None
    worker_task = None
    if settings.run_embedded_worker:
        from app.workers.event_worker import build_worker

        worker = build_worker(mongodb.get_database(), redis_db.get_redis())
        worker_task = asyncio.create_task(worker.run())
        logger.info("Embedded event worker started")

    try:
        yield
    finally:
        if worker is not None and worker_task is not None:
            worker.stop()
            await worker_task
        await redis_db.disconnect_from_redis()
        await mongodb.disconnect_from_mongo()


app = FastAPI(
    title="Event Hub",
    description="Recebe eventos via webhook e os processa de forma assíncrona.",
    lifespan=lifespan,
)


app.include_router(webhook.router)
app.include_router(events.router)


@app.get("/")
async def root():
    return {"message": "Hello World!"}


@app.get("/health", tags=["health"])
async def health():
    checks = {}

    try:
        await mongodb.get_database().command("ping")
        checks["mongodb"] = "ok"
    except Exception as exc:
        checks["mongodb"] = f"error: {exc}"

    try:
        await redis_db.get_redis().ping()
        checks["redis"] = "ok"
    except Exception as exc:
        checks["redis"] = f"error: {exc}"

    healthy = all(value == "ok" for value in checks.values())
    return JSONResponse(
        status_code=200 if healthy else 503,
        content={"status": "ok" if healthy else "degraded", "checks": checks},
    )
