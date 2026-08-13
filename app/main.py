from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.controllers import webhook
from app.database.mongodb import (
    connect_to_mongo,
    disconnect_from_mongo,
)
from app.database.redis import connect_to_redis, disconnect_from_redis


@asynccontextmanager
async def lifespan(app: FastAPI):
    # connect to infrastructure services used by the app
    await connect_to_mongo()
    await connect_to_redis()
    yield
    await disconnect_from_redis()
    await disconnect_from_mongo()


app = FastAPI(
    lifespan=lifespan,
)


app.include_router(webhook.router)


@app.get("/")
async def root():
    return {"message": "Hello World!"}
