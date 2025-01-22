from enum import StrEnum
from itertools import chain
from typing import (
    Annotated,
    Any,
    ClassVar,
    Iterator,
    Literal,
    Self,
    Type,
    Union,
    get_args,
)

from fastapi import Query as HttpQueryPars
from pydantic import BaseModel, EmailStr, Field, GetCoreSchemaHandler, create_model
from pydantic_core import CoreSchema, core_schema
from sqlalchemy import and_, func, or_

from .models import BaseSQLModel, SQLModelType
from .utils import classproperty


class Conditions(StrEnum):
    """Types of filters exposed by the api."""
    eq = "eq"
    ne = "ne"
    gt = "gt"
    lt = "lt"
    ge = "ge"
    le = "le"
    like = "like"
    ilike = "ilike"

    @classproperty
    def likes(cls) -> set[Self]:
        return {cls.like, cls.ilike}

    @classproperty
    def exacts(cls) -> set[Self]:
        return {cls.eq, cls.ne}

    @classproperty
    def comparisons(cls) -> set[Self]:
        return {cls.ge, cls.le, cls.gt, cls.lt}


class QueryModelBase(BaseModel):
    model_config = {
        "extra": "forbid",
        "arbitrary_types_allowed": True,
        "populate_by_name": True,
    }
    base_fields: ClassVar[set[str]] = {"page", "per_page", "order_by", "project"}
    model: ClassVar[SQLModelType] = None
    search_mth: ClassVar[str] = None

    page: int = 0
    per_page: int = Field(None, alias="perPage")
    order_by: list = Field(None, alias="orderBy")
    project: list = None

    @property
    def offset(self) -> int:
        return self.page * self.per_page

    @classproperty
    def query_pars(cls) -> Type[Annotated]:
        return Annotated[cls, HttpQueryPars()]

    @classmethod
    def parse_filter(cls, field: str) -> tuple[str, Conditions, bool]:
        multi = field.endswith("__in")
        field_parts = field[: -4 if multi else None].split("__")
        field_name = field_parts[0]
        if len(field_parts) == 1:
            return field_name, Conditions.eq, multi
        return field_name, Conditions[field_parts[1]], multi

    @property
    def model_filters(self) -> Iterator[tuple]:
        model_attrs = set(self.model.model_fields.keys())
        data = self.model_dump(exclude_unset=True)
        for k, f in self.model_fields.items():
            if k not in data:
                continue
            v = data[k]
            try:
                sql_cond_filter = hasattr(f.annotation, "_sql_cond")
            except AttributeError:
                sql_cond_filter = False
            if k in model_attrs or sql_cond_filter:
                yield k, v

    @property
    def has_custom_filters(self) -> bool:
        model_attrs = set(self.model.model_fields.keys())
        for k, f in self.model_fields.items():
            k, _, _ = self.parse_filter(k)
            if k not in self.base_fields | model_attrs and not hasattr(
                f.annotation, "_sql_cond"
            ):
                return True
        return False


class QueryPars:
    @classmethod
    def build(
        cls,
        *fields,
        search_method: str = None,
        **kwfields,
    ):
        model = fields[0]._annotations["parententity"].entity
        class_attrs = {
            "order_by": (cls._order_by_annotation(model), Field(None, alias="orderBy")),
            "project": (list[Literal[tuple(model.model_fields.keys())]], None),
        }
        for f in fields:
            class_attrs.update(cls._field_filter_attrs(f, model))

        for f, f_typ in kwfields.items():
            class_attrs[f] = (f_typ, None)

        return create_model(
            f"{model.__class__.__name__}FiltersBase",
            **class_attrs,
            model=model,
            search_mth=search_method,
            __base__=QueryModelBase,
        )

    @staticmethod
    def _cond_valid_for(cond: Conditions, types: set[type]) -> bool:
        if bool in types:
            return cond in Conditions.exacts
        if str in types:
            return cond not in Conditions.comparisons
        return cond not in Conditions.likes

    @staticmethod
    def camel(snake_str: str) -> str:
        first, *others = snake_str.split("_")
        return "".join([first.lower(), *map(str.title, others)])

    @classmethod
    def field_names(cls, field_name: str, cond: Conditions) -> tuple[str, str, str]:
        camelname = cls.camel(field_name)
        if cond == Conditions.eq:
            return field_name, camelname, f"{field_name}__in"
        fname = f"{field_name}__{cond}"
        return fname, f"{camelname}[{cond}]", f"{fname}__in"

    @classmethod
    def _field_filter_attrs(cls, field, model) -> dict:
        pydantic_field = model.model_fields[field.name]
        out = {}
        model_types = cls._normalize_field_types(field)
        for cond in Conditions:
            if not cls._cond_valid_for(cond, model_types):
                continue

            attr_name, alias, attr_name_multi = cls.field_names(field.name, cond)

            if bool in model_types:
                annotation = bool
            else:
                if cond in Conditions.exacts:
                    types = model_types | {
                        None,
                    }
                    annotation = Union[tuple(types)]
                else:
                    annotation = Union[tuple(model_types)]

            pyd_field = Field(None, alias=alias, description=pydantic_field.description)

            out[attr_name] = (annotation, pyd_field)

            if cond in Conditions.exacts and bool not in model_types:
                pyd_multi_field = Field(
                    None, alias=f"{alias}[]", description=pydantic_field.description
                )
                out[attr_name_multi] = (
                    Union[tuple(list[t] for t in model_types)],
                    pyd_multi_field,
                )
        return out

    @staticmethod
    def _order_by_annotation(model: BaseSQLModel) -> Type[Annotated]:
        fields = model.model_fields.keys()
        order_by_fields = chain(fields, (f"{k}.desc" for k in fields))
        return list[Literal[tuple(sorted(order_by_fields))]]

    @staticmethod
    def _normalize_field_types(field) -> set:
        model = field._annotations["parententity"].entity
        model_notation = model.model_fields[field.name].annotation
        model_types = get_args(model_notation)
        if not model_types:
            types = {model_notation}
        else:
            types = {t for t in model_types if t is not type(None)}
        return {t if t is not EmailStr else str for t in types}

    @staticmethod
    def levenshtein(field, max_distance: int):
        @classmethod
        def pydantic_levenshtein_schema(
            cls, source_type: Any, handler: GetCoreSchemaHandler
        ) -> CoreSchema:
            return core_schema.no_info_after_validator_function(cls, handler(str))

        def sql_cond(self, model: BaseSQLModel):
            return and_(func.levenshtein(getattr(model, field.name), self))

        return type(
            "Levenshtein",
            (str,),
            {
                "field": field,
                "max_distance": max_distance,
                "_sql_cond": sql_cond,
                "__get_pydantic_core_schema__": pydantic_levenshtein_schema,
            },
        )

    @staticmethod
    def multi_like(*fields, i=True):
        @classmethod
        def pydantic_multilike_schema(
            cls, source_type: Any, handler: GetCoreSchemaHandler
        ) -> CoreSchema:
            return core_schema.no_info_after_validator_function(cls, handler(str))

        def sql_cond(self, model: BaseSQLModel):
            return and_(
                or_(
                    *(
                        getattr(
                            getattr(model, f.name),
                            Conditions.ilike if self.ignorecase else Conditions.like,
                        )(f"%{part}%")
                        for f in self.fields
                    )
                )
                for part in self.split()
            )

        return type(
            "MultiLike",
            (str,),
            {
                "fields": fields,
                "ignorecase": i,
                "_sql_cond": sql_cond,
                "__get_pydantic_core_schema__": pydantic_multilike_schema,
            },
        )
