import sys
from fastapi import APIRouter, Query
from pathlib import Path
import logging

from app.models.enum import DistanceMeasure
from app.models.schemas import (
    InsertRequest,
    SearchRequest,
    DeleteRequest,
    BaseResponse,
    StatusCode,
)
from app.models.exceptions import handle_exception
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

APPDIR = Path(__file__).absolute().parent.parent
sys.path.append(str(APPDIR))
router = APIRouter()
storage = ContentStorage(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DATABASE,
)
repository = ClickHouseRepository(connection=storage)


@router.post("/insert", response_model=BaseResponse)
async def insert_data(request: InsertRequest):
    """Insert data into ClickHouse in batches."""
    try:
        await repository.ensure_db_and_table(
            table_name=CLICKHOUSE_TABLE,
            id_column=CLICKHOUSE_IDS,
            vector_column=CLICKHOUSE_VECTORS,
        )

        await storage.insert_data(
            data=request.data,
            table_name=CLICKHOUSE_TABLE,
            id_column=CLICKHOUSE_IDS,
            vector_column=CLICKHOUSE_VECTORS,
        )
        return BaseResponse(
            status=StatusCode.SUCCESS,
            message=f"Successfully inserted {len(request.data)} records.",
            result=None,
        )

    except Exception as e:
        logging.error(f"Insert error: {e}")
        raise handle_exception(e)


@router.post("/search", response_model=BaseResponse)
async def search_similar_vectors_db(
    request: SearchRequest,
    count: int = Query(default=10, description="Number of results to return"),
    measure_type: DistanceMeasure = DistanceMeasure.L2,
):
    """Search for similar vectors in ClickHouse."""
    try:
        similar_vectors = await storage.search_vectors(
            input_vectors=request.vectors,
            count=count,
            measure_type=measure_type,
            table_name=CLICKHOUSE_TABLE,
            id_column=CLICKHOUSE_IDS,
            vector_column=CLICKHOUSE_VECTORS,
        )

        return BaseResponse(
            status=StatusCode.SUCCESS,
            message="Successfully retrieved similar vectors.",
            result=similar_vectors,
        )
    except Exception as e:
        logging.error(f"Search error: {e}")
        raise handle_exception(e)


@router.post("/delete", response_model=BaseResponse)
async def delete_records(
    request: DeleteRequest,
):
    """Delete records from ClickHouse by their IDs."""
    try:
        await storage.delete_by_ids(
            ids=request.ids, table_name=CLICKHOUSE_TABLE, id_column=CLICKHOUSE_IDS
        )
        return BaseResponse(
            status=StatusCode.SUCCESS,
            message=f"Deleted {len(request.ids)} records.",
            result=None,
        )
    except Exception as e:
        logging.error(f"Delete error: {e}")
        raise handle_exception(e)
