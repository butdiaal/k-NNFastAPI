import uvicorn
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.api.routes import router

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages the lifecycle of the application"""
    yield
    logging.info("Application shutdown.")


app = FastAPI(
    lifespan=lifespan,
    title="Vector Search API",
    description="API for inserting, searching, and deleting vector data in ClickHouse.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.include_router(router)


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=4000,
        reload=True,
        log_level="info",
    )
