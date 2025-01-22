import mimetypes
from io import BytesIO
from typing import Generic

from fastapi import Response
from pydantic import BaseModel, Field, RootModel, computed_field

from .filters import QueryModelBase
from .models import DataType, DbModel


class BaseDataResponse(BaseModel):
    """Base class for all responses to handle common behaviour"""

    model_config = {"from_attributes": True}


class PaginationMeta(BaseDataResponse):
    """Pagination metadata container"""

    page: int | None
    per_page: int | None
    total: int
    page_total: int

    @computed_field
    def pages(self) -> int | None:
        if not self.per_page:
            return
        return (self.total + self.per_page - 1) // self.per_page


class ProjectedResponse(RootModel[dict]):
    model_config = {"from_attributes": True}


class ResourceResponse(BaseDataResponse, Generic[DataType]):
    """Single-resource response"""

    data: DataType | ProjectedResponse = None

    @staticmethod
    def build(data: type[DbModel]) -> dict:
        return {"data": data}


class PaginatedResponse(BaseDataResponse, Generic[DataType]):
    """Multi-resource paginate response"""

    data: list[DataType | ProjectedResponse] = Field(default_factory=list)
    meta: PaginationMeta

    @classmethod
    def build(
        cls, data: list[type[DbModel]], filters: QueryModelBase, total: int
    ) -> dict:
        return {
            "data": data,
            "meta": {
                "page": filters.page,
                "per_page": filters.per_page,
                "total": total,
                "page_total": len(data),
            },
        }


mimetypes.add_type("application/vnd.ms-excel", "xlsx")


class DownloadResponse(Response):
    def __init__(self, file: BytesIO, filename: str, *args, **kwargs):
        """
        Wrapper on the Response object that given a bytestream and a filename
        enriches the response with a mimetype, the filename header, and the bytes
        data closing the stream.
        """
        custom_h = kwargs.pop("headers", None) or {}
        try:
            media_type = mimetypes.types_map[f".{filename.split('.')[-1]}"]
        except KeyError:
            media_type = "application/octet-stream"
        file.seek(0)
        super().__init__(
            file.getvalue(),
            *args,
            headers={"Content-Disposition": f'inline; filename="{filename}"'}
            | custom_h,
            media_type=media_type,
        )
        file.close()
