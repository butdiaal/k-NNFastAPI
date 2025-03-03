from enum import Enum


class DistanceMeasure(str, Enum):
    L2 = "l2"
    COSINE = "cosine"


class StatusCode(str, Enum):
    """Status codes for API responses."""

    SUCCESS = "success"
    ERROR = "error"
