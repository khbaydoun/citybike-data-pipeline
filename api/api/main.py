"""HTTP API: per-station aggregates read from Redis (contract: docs/api-contract.md)."""

from contextlib import asynccontextmanager

import redis
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from redis.asyncio import Redis

from .config import Config
from .repository import StationRepository
from .schemas import BikeBalance, BusiestStation, ErrorResponse, LastActivity, TripStats


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = Config()
    client = Redis.from_url(
        cfg.redis_url, decode_responses=True,
        socket_timeout=cfg.redis_timeout_s, socket_connect_timeout=cfg.redis_timeout_s,
    )
    app.state.repo = StationRepository(client)
    yield
    await client.aclose()


app = FastAPI(title="Citibike station activity API", lifespan=lifespan)


def get_repo(request: Request) -> StationRepository:
    return request.app.state.repo


@app.exception_handler(redis.RedisError)
async def redis_unavailable(_: Request, exc: redis.RedisError):
    return JSONResponse(status_code=503, content={"detail": "Data store unavailable"})


@app.middleware("http")
async def no_store(request: Request, call_next):
    response = await call_next(request)
    response.headers["Cache-Control"] = "no-store"  # values change with every event
    return response


NOT_FOUND = {404: {"model": ErrorResponse}, 503: {"model": ErrorResponse}}


def _found(value, detail: str):
    if value is None:
        raise HTTPException(status_code=404, detail=detail)
    return value


@app.get("/stations/busiest", response_model=BusiestStation, responses=NOT_FOUND)
async def busiest(repo: StationRepository = Depends(get_repo)):
    return _found(await repo.busiest(), "No station activity yet")


@app.get("/stations/{station_id}/last-activity", response_model=LastActivity, responses=NOT_FOUND)
async def last_activity(station_id: str, repo: StationRepository = Depends(get_repo)):
    return _found(await repo.last_activity(station_id), f"Station '{station_id}' not found")


@app.get("/stations/{station_id}/bike-balance", response_model=BikeBalance, responses=NOT_FOUND)
async def bike_balance(station_id: str, repo: StationRepository = Depends(get_repo)):
    return _found(await repo.bike_balance(station_id), f"Station '{station_id}' not found")


@app.get("/stations/{station_id}/trip-stats", response_model=TripStats, responses=NOT_FOUND)
async def trip_stats(station_id: str, repo: StationRepository = Depends(get_repo)):
    return _found(await repo.trip_stats(station_id), f"Station '{station_id}' not found")


@app.get("/health")
async def health():
    """Liveness: the process is up. Deliberately doesn't touch Redis."""
    return {"status": "ok"}


@app.get("/ready", responses={503: {"model": ErrorResponse}})
async def ready(repo: StationRepository = Depends(get_repo)):
    """Readiness: Redis is reachable (a RedisError becomes 503 via the handler above)."""
    await repo.ping()
    return {"status": "ready"}
