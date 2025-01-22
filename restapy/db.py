from functools import wraps
from typing import Any, Callable, Iterator, Sequence

from pydantic import BaseModel
from sqlalchemy import ColumnElement, ColumnOperators, and_, delete, func
from sqlalchemy.orm.query import Query
from sqlmodel import Session, create_engine

from .exceptions import NotFoundException
from .filters import Conditions, QueryModelBase
from .models import BaseSQLModel, SQLModelType


class DbInterface:
    """
    Base class for the database interaction classes.
    Built on top of SQLAlchemy/SQLModel, exposes common
    utility methods to get/update/upsert/delete/search models.
    """

    create_engine = create_engine

    def __init__(self, session: Session):
        self.session = session

    def upsert(
        self, model: SQLModelType, instance_id: int | str, data: dict | BaseModel
    ) -> SQLModelType:
        """Public api to update a model by primary key, returns the updated instance."""
        try:
            instance = self.get(model, instance_id)
        except NotFoundException:
            instance = model()
        instance.update(data)
        self.session.add(instance)
        return instance

    def update(
        self, model: SQLModelType, instance_id: int | str, data: dict | BaseModel
    ) -> SQLModelType:
        """Public api to update a model by primary key, returns the updated instance."""
        instance = self.get(model, instance_id)
        instance.update(data)
        self.session.add(instance)
        return instance

    def delete(self, model: SQLModelType, instance_id: int | str) -> None:
        """Public api to delete an instance by id."""
        instance = self.session.get(model, instance_id)
        self.session.delete(instance)

    def get(self, model: SQLModelType, instance_id: int | str) -> BaseSQLModel:
        """Public api to get a model instance by primary key."""
        instance = self.session.get(model, instance_id)
        if not instance:
            raise NotFoundException(model.__class__.__name__, instance_id)
        return instance

    def search(
        self, filters: QueryModelBase, *args, **kwargs
    ) -> tuple[Sequence[BaseSQLModel], int]:
        """Public api to perform a select using a QueryModel."""
        if filters.has_custom_filters:
            try:
                search_mth = getattr(self, filters.search_mth)
            except (TypeError, AttributeError):
                raise NotImplementedError(
                    f"Method {filters.search_mth} for {filters.__class__.__name__} missing in {self.__class__.__name__}"
                )
            return search_mth(filters, *args, **kwargs)
        return self._search(filters)

    def query_from_filters(self, filters: QueryModelBase, session: Session) -> Query:
        if not filters.model:
            raise AttributeError(f"No model defined for {filters}")

        # Extraxt select fields or model
        if filters.project:
            what = [getattr(filters.model, f) for f in filters.project]
        else:
            what = [filters.model]

        # Build the query using filters
        query = session.query(*what).filter(self._where_from_filters(filters))
        if filters.order_by:
            # Adds the provided order by
            query = query.order_by(*self.order_attrs(filters))
        return query

    @staticmethod
    def count_query(query: Query, model: SQLModelType) -> Query:
        """Returns the count(*) SA query with the same joins/where of the original"""
        return (
            query.statement.select_from(model)
            .with_only_columns(func.count())
            .order_by(None)
        )

    @staticmethod
    def paginate_query(
        query: Query,
        filters: QueryModelBase,
    ) -> Query:
        """Adds limit/offset to a query, using filter's vals"""
        return query.limit(filters.per_page).offset(filters.offset)

    @staticmethod
    def order_attrs(filters: QueryModelBase) -> Iterator[ColumnOperators]:
        """Yields the SA order by attributes"""
        for f in filters.order_by:
            verse = ["asc", "desc"][int(bool(".desc" in f))]
            yield getattr(getattr(filters.model, f.replace(".desc", "")), verse)()

    @classmethod
    def pagination_queries(
        cls, filters: QueryModelBase, query: Query
    ) -> tuple[Query, Query | None]:
        """
        Enriches the given query with the pagination pars from the filters.
        Builds a twin count non-paginated query.
        """
        count_query = None
        if filters.per_page:
            count_query = cls.count_query(query, filters.model)
            query = query.limit(filters.per_page).offset(filters.offset)
        return query, count_query

    def _search(self, filters: QueryModelBase) -> tuple[Sequence[BaseSQLModel], int]:
        """
        Builds the complete query from a filter, creates a count twin query.
        Executes the query and returns the results, and the count.
        """
        base_query = self.query_from_filters(filters, self.session)

        query, count_query = self.pagination_queries(filters, base_query)
        count = None
        if count_query is not None:
            count = self.session.execute(count_query).scalar()

        data = query.all()
        if filters.project:
            data = [r._mapping for r in data]
        return data, count or len(data)

    @classmethod
    def _sql_cond(
        cls, filter_field: str, value: Any, filters: QueryModelBase
    ) -> Callable:
        """
        Builds the SQLAlchemy's `<model>.<property> <comparator> <value>`
        (i.e: Users.age <= 12) instance to use in the where method starting
        from a RestApi filter.
        If the provided filter have a `_sql_cond` method is used instead.
        """
        # Check for method override
        if hasattr(value, "_sql_cond"):
            return value._sql_cond(filters.model)
        # Filter name parsing
        model_attr, condition, multi = QueryModelBase.parse_filter(filter_field)
        # Transform the condition into a method name to get from the model
        if condition in {Conditions.like, Conditions.ilike}:
            getter_attr = condition
        elif multi:
            getter_attr = {Conditions.eq: "in_", Conditions.ne: "not_in"}[condition]
        else:
            getter_attr = f"__{condition}__"
        # Normalize value for like operations
        if condition in Conditions.likes:
            value = f"%{value}%"
        # Get the comparator method from the model's attribute
        # and call it with the value.
        return getattr(getattr(filters.model, model_attr), getter_attr)(value)

    @classmethod
    def _where_from_filters(cls, filters: QueryModelBase) -> ColumnElement[bool]:
        """Builds an AND-separated SQLAlchemy object, to use in a where, from the given filters."""
        return and_(
            True,
            *(cls._sql_cond(k, v, filters) for k, v in filters.model_filters),
        )

    @staticmethod
    def transaction(fun):
        """Decorator to wrap DbInterface-using methods in transactions."""

        @wraps(fun)
        def wrap_func(self, *args, **kwargs):
            try:
                result = fun(self, *args, **kwargs)
                self.db.session.commit()
            except Exception as e:
                self.db.session.rollback()
                raise
            return result

        return wrap_func
