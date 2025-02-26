from pydantic import BaseModel
from typing import List, Dict, Any, Tuple


class InsertRequest(BaseModel):
    """Schema for inserting multiple vector entries."""

    data: List[Tuple[str, List[float]]]


class InsertResponse(BaseModel):
    """Schema for the response after a successful insert operation."""

    message: str


class SearchRequest(BaseModel):
    """Schema for searching similar vectors in ClickHouse."""

    vectors: List[List[float]]
    measure_type: str = "l2"
    count: int = 10


class SearchResultItem(BaseModel):
    """Schema representing a single search result."""

    id: str
    distance: float


class SearchResponse(BaseModel):
    """Schema for the response of a vector similarity search."""

    message: str
    results: Dict[int, Dict[str, Any]]


class DeleteRequest(BaseModel):
    """Schema for deleting records by their IDs."""

    ids: List[str]


class DeleteResponse(BaseModel):
    """Schema for the response after a successful delete operation."""

    message: str
