"""
This module contains subclasses of fastapi HTTPExceptions, enriched with
http status codes and meaningful but protected payloads.
Those exceptions are not supposed to be trapped.
"""
from fastapi import HTTPException, status


class UnauthorizedException(HTTPException):
    def __init__(self):
        super().__init__(status_code=status.HTTP_401_UNAUTHORIZED)


class NotFoundException(HTTPException):
    def __init__(self, kind: str, instance_id: str | int):
        super().__init__(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"{kind} with id {instance_id} not found",
        )
