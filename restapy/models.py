from typing import Annotated, Type, TypeVar

from pydantic import AfterValidator, BaseModel
from sqlalchemy.inspection import inspect
from sqlmodel import SQLModel

from .utils import classproperty

DataType = TypeVar("DataType")


class AnnotationModel(BaseModel):

    @classproperty
    def body(cls) -> Type[Annotated]:
        return Annotated[cls, AfterValidator(cls.model_validate)]


class BaseSQLModel(SQLModel, AnnotationModel):

    @classproperty
    def pk_field(cls) -> str:
        return inspect(cls).primary_key[0].name

    def update(self, data: dict | BaseModel):
        """Bulk update  the instance data."""
        if isinstance(data, BaseModel):
            data = data.model_dump(exclude_unset=True)
        for k, v in (data or {}).items():
            if k not in self.model_fields or hasattr(
                self.model_fields[k], "primary_key"
            ):
                continue
            setattr(self, k, v)


SQLModelType = TypeVar("SQLModelType", bound=BaseSQLModel)
