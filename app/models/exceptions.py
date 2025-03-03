from fastapi import HTTPException
from starlette.status import (
    HTTP_400_BAD_REQUEST,
    HTTP_404_NOT_FOUND,
    HTTP_500_INTERNAL_SERVER_ERROR,
)


class DatabaseError(Exception):
    """Database error."""

    def __init__(
        self, message="Database error", status_code=HTTP_500_INTERNAL_SERVER_ERROR
    ):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class NotFoundError(Exception):
    """Error when data is not found."""

    def __init__(self, message="Data not found", status_code=HTTP_404_NOT_FOUND):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class ValidationError(Exception):
    """Data validation error."""

    def __init__(self, message="Invalid input data", status_code=HTTP_400_BAD_REQUEST):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


def handle_exception(exc: Exception):
    """Error handler. It forwards the required status code."""
    if isinstance(exc, (DatabaseError, NotFoundError, ValidationError)):
        return HTTPException(status_code=exc.status_code, detail=exc.message)
    return HTTPException(
        status_code=HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error"
    )
