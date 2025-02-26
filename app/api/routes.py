import sys
from fastapi import APIRouter, Query, HTTPException, Depends
from pathlib import Path
from app.models.schemas import InsertRequest, SearchRequest, DeleteRequest
from app.services.vector_service import ContentStorage
from pydantic import BaseModel
from typing import List, Dict, Any

APPDIR = Path(__file__).absolute().parent.parent
sys.path.append(str(APPDIR))
router = APIRouter()
storage = ContentStorage()
storage.connect()


@router.post("/insert")
async def insert_data(
    request: InsertRequest,
    table: str = Query(default="element", description="ClickHouse table name"),
    id_column: str = Query(
        default="doc_id", description="Column name for document IDs"
    ),
    vector_column: str = Query(
        default="centroid", description="Column name for vector data"
    ),
    batch_size: int = Query(default=1000, description="Batch size for insertion"),
    max_workers: int = Query(default=4, description="Max number of worker threads"),
):
    """Insert data into ClickHouse in batches."""
    try:
        await storage.insert_data(
            table, id_column, vector_column, request.data, batch_size, max_workers
        )
        return {
            "message": f"Successfully inserted {len(request.data)} records into ClickHouse in batches."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search")
async def search_similar_vectors_db(
    request: SearchRequest,
    table: str = Query(default="element", description="ClickHouse table name"),
    id_column: str = Query(
        default="doc_id", description="Column name for document IDs"
    ),
    vector_column: str = Query(
        default="centroid", description="Column name for vector data"
    ),
    count: int = Query(default=10, description="Number of results to return"),
    offset: int = Query(default=0, description="Offset for pagination"),
):
    """Search for similar vectors in ClickHouse."""
    try:
        similar_vectors = await storage.search_vectors(
            request.vectors,
            table,
            id_column,
            vector_column,
            count,
            offset,
            request.measure_type,
        )
        return {
            "message": "Successfully retrieved similar vectors.",
            "results": similar_vectors,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/delete")
async def delete_records(
    request: DeleteRequest,
    table: str = Query(default="element", description="ClickHouse table name"),
    id_column: str = Query(
        default="doc_id", description="Column name for document IDs"
    ),
):
    """Delete records from ClickHouse by their IDs."""
    try:
        await storage.delete_by_ids(table, id_column, request.ids)
        return {"message": f"Deleted {len(request.ids)} records from '{table}'."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
