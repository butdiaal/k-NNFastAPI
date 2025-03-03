from pydantic import BaseModel
from typing import List, Any, Tuple, Optional
from app.models.enum import StatusCode


class BaseResponse(BaseModel):
    """Unified response schema for all API endpoints."""

    status: StatusCode
    message: str
    result: Optional[Any] = None


class InsertRequest(BaseModel):
    """Schema for inserting multiple vector entries."""

    data: List[Tuple[str, List[float]]]


class SearchRequest(BaseModel):
    """Schema for searching similar vectors in ClickHouse."""

    vectors: List[List[float]]
    measure_type: str = "l2"
    count: int = 10


class DeleteRequest(BaseModel):
    """Schema for deleting records by their IDs."""

    ids: List[str]
