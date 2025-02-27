import logging
import uvicorn
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from app.api.routes import router
from app.services.vector_service import ContentStorage
from app.db.repository import ClickHouseRepository
from app.config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_TABLE,
    CLICKHOUSE_IDS,
    CLICKHOUSE_VECTORS,
)

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages the lifecycle of the application, including ClickHouse connection."""
    content_storage = ContentStorage(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )
    try:

        content_storage.connect()

        repository = ClickHouseRepository(connection=content_storage)
        await repository.ensure_db_and_table(
            table_name=CLICKHOUSE_TABLE,
            ids=CLICKHOUSE_IDS,
            vectors=CLICKHOUSE_VECTORS,
        )
        yield
    except Exception as e:
        logging.error(f"Error during application startup: {e}")
        raise HTTPException(status_code=500, detail="Failed to initialize application.")
    finally:

        content_storage.close()
        logging.info("ClickHouse connection closed.")


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
        port=8000,
        reload=True,
        log_level="info",
    )
