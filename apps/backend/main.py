from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.api import connections, schedules, runs, admin, attractions
from app.core.config import settings
from app.core.database import init_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(
    title="Hanoi Smart Tourism Lakehouse API",
    description="API quản lý pipeline dữ liệu du lịch Hà Nội",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://frontend:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(connections.router, prefix="/api/connections", tags=["Connections"])
app.include_router(schedules.router, prefix="/api/schedules", tags=["Schedules"])
app.include_router(runs.router, prefix="/api/runs", tags=["Pipeline Runs"])
app.include_router(attractions.router, prefix="/api/attractions", tags=["Attractions"])
app.include_router(admin.router, prefix="/api/admin", tags=["Admin"])


@app.get("/")
async def root():
    return {
        "project": "Hanoi Smart Tourism Lakehouse",
        "version": "1.0.0",
        "docs": "/docs",
    }