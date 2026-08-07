from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.controllers import webhook
from app.database.mongodb import (
    connect_to_mongo,
    disconnect_from_mongo,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await connect_to_mongo()
    yield
    await disconnect_from_mongo()


app = FastAPI(
    lifespan=lifespan,
)


app.include_router(webhook.router)


@app.get("/")
async def root():
    return {"message": "Hello World!"}
