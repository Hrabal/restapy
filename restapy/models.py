from typing import Annotated, Type, TypeVar

from pydantic import AfterValidator, BaseModel
from sqlalchemy.inspection import inspect
from sqlmodel import SQLModel

from .utils import classproperty

DataType = TypeVar("DataType")


class DbModel(SQLModel):
    """Wrapper on the base SQLModel class"""

    @classproperty
    def primary_key(cls) -> str:
        """Expose the model's PK"""
        return inspect(cls).primary_key

    @classproperty
    def fapi_body(cls) -> Type[Annotated]:
        """The model's annotation to use in fastapi endpoints signatures."""
        return Annotated[cls, AfterValidator(cls.model_validate)]

    def update(self, data: dict | type[BaseModel]):
        """Bulk update the instance data."""
        if isinstance(data, BaseModel):
            data = data.model_dump(exclude_unset=True)
        for k, v in (data or {}).items():
            if k not in self.model_fields or hasattr(
                self.model_fields[k], "primary_key"
            ):
                continue
            setattr(self, k, v)
