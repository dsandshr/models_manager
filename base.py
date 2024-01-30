import logging
import re
from typing import Any, Dict, List, NoReturn, Optional, overload, Sequence, Set, Tuple, Union

import sqlalchemy as sa
from sqlalchemy import MetaData
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql.expression import and_
from typing_extensions import Self

from app.apps.base.schemas import BaseSorting, Pagination
from app.exceptions import DatabaseIntegrityError, DatabaseValidationError
from app.service.utils import utcnow


logger = logging.getLogger(__name__)
metadata = MetaData()
Base = declarative_base(metadata=metadata)

RE_ALREADY_EXIST_COMPILED = re.compile(r"Key \((.*)\)=\(.*\) already exists|$")
RE_NOT_PRESENT_COMPILED = re.compile(r"Key \((.*)\)=\(.*\) is not present in table|$")


class BaseModel(Base):
    """Base model with audit fields"""

    __abstract__ = True

    __search_like_fields__: Set = set()

    def __str__(self):
        return f"<{type(self).__name__}({self.id=})>"

    id = sa.Column(sa.Integer, primary_key=True)
    creator_id = sa.Column(sa.String, nullable=True)
    created_at = sa.Column(sa.DateTime(timezone=True), default=utcnow)
    updated_at = sa.Column(sa.DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    @classmethod
    def _raise_validation_exception(cls, exc: IntegrityError) -> NoReturn:
        info = exc.orig.args  # type: ignore
        if info and (match := re.findall(RE_ALREADY_EXIST_COMPILED, info[0])) and match[0]:
            raise DatabaseValidationError(f"Unique constraint violated for {cls.__name__}", match[0]) from exc
        if info and (match := re.findall(RE_NOT_PRESENT_COMPILED, info[0])) and match[0]:
            raise DatabaseValidationError(f"Foreign key constraint violated for {cls.__name__}", match[0]) from exc
        logger.error("Integrity error for %s: %s", cls.__name__, exc)
        raise DatabaseIntegrityError(f"Integrity error for {cls.__name__}") from exc

    async def save(self, db: AsyncSession) -> None:
        """Сохранить запись в БД"""
        obj_id = self.id
        db.add(self)
        class_name = type(self).__name__

        try:
            await db.flush()
        except IntegrityError as exc:
            self._raise_validation_exception(exc)

        if obj_id is None:
            logger.info(f"{class_name} with id={self.id} is created")
        else:
            logger.info(f"{class_name} with id={obj_id} is updated")

        await db.refresh(self)

    def update_attrs(self, **kwargs: Any) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)

    @classmethod
    async def get_by_id(cls, db: AsyncSession, item_id: Any) -> Optional[Self]:
        return await cls.get_one_by_filter(db, [cls.id == item_id])

    async def delete(self, db: AsyncSession) -> None:
        await db.delete(self)
        await db.flush()
        class_name = type(self).__name__
        logger.debug(f"{class_name} with id={self.id} was deleted")

    @classmethod
    async def get_one_by_filter(cls, db: AsyncSession, conditions: List[Any]) -> Optional[Self]:
        """
        Вспомогательный внутренний метод
        для извлечения одной записи со всеми полями cls по фильтру
        """
        query = sa.select(cls).where(and_(*conditions))
        result = await db.execute(query)
        return result.scalars().unique().first()

    @overload
    @classmethod
    async def filter(
        cls,
        db: AsyncSession,
        *,
        filters: Optional[Dict[str, Any]] = None,
        conditions: Optional[List[Any]] = None,
        sorting: Optional[BaseSorting] = None,
    ) -> Sequence[Self]:
        """
        Вспомогательный метод для извлечения записей со всеми полями cls по фильтру без пагинации
        """

    @overload
    @classmethod
    async def filter(
        cls,
        db: AsyncSession,
        *,
        pagination: Pagination,
        filters: Optional[Dict[str, Any]] = None,
        conditions: Optional[List[Any]] = None,
        sorting: Optional[BaseSorting] = None,
    ) -> Tuple[Sequence[Self], int]:
        """
        Вспомогательный метод для извлечения записей со всеми полями cls по фильтру с пагинацией
        """

    @classmethod
    async def filter(
        cls,
        db: AsyncSession,
        filters: Optional[Dict[str, Any]] = None,
        conditions: Optional[List[Any]] = None,
        pagination: Optional[Pagination] = None,
        sorting: Optional[BaseSorting] = None,
    ) -> Union[Sequence[Self], Tuple[Sequence[Self], int]]:
        """
        Вспомогательный метод для извлечения записей со всеми полями cls по фильтру
        """
        conditions = conditions or []
        total = 0

        stmt = sa.select(cls)
        if filters:
            if filters.get("search_like_string"):
                search_like_string = filters.pop("search_like_string", None)
                if search_like_string:
                    stmt = cls._build_search_like_string_stmt(stmt, search_like_string)
            conditions.extend(cls._build_conditions(filters))
        if sorting:
            stmt = stmt.order_by(*cls._build_sorting(sorting))

        query = stmt.where(and_(*conditions))

        if pagination:
            # pylint: disable=not-callable
            total = await db.scalar(sa.select(sa.func.count()).select_from(query.subquery())) or 0
            query = query.limit(pagination.limit).offset(pagination.offset)

        result = await db.execute(query)
        serialised_result = result.scalars().unique().all()

        if pagination:
            return serialised_result, total
        return serialised_result

    @classmethod
    def _build_sorting(cls, sorting: Union[BaseSorting, Dict[str, str]]) -> List[Any]:
        """Build list of ORDER_BY clauses

        :param sorting: dict like {"field_name": "asc|desc"}
        """
        if isinstance(sorting, BaseSorting):
            sort_fields = sorting.sort_dict()
        else:
            sort_fields = sorting
        result = []
        for field_name, direction in sort_fields.items():
            field = getattr(cls, field_name)
            result.append(getattr(field, direction)().nulls_last())
        return result

    @classmethod
    async def create(cls, db: AsyncSession, data: Dict[str, Any]) -> Any:
        new_item = cls(**{key: value for (key, value) in data.items() if key in cls.__dict__})
        await new_item.save(db)
        return new_item

    async def update(self, db: AsyncSession, data: Dict[str, Any]) -> None:
        self.update_attrs(**data)
        await self.save(db)

    @classmethod
    def _build_search_like_string_stmt(cls, stmt: sa.Select, search_like_string: str) -> sa.Select:
        fields = [getattr(cls, sls_field) for sls_field in cls.__search_like_fields__]

        sls_stmt = f"%{search_like_string}%".replace(" ", "%")
        ilikes = [field.ilike(sls_stmt) for field in fields]
        return stmt.where(sa.or_(*ilikes))

    @classmethod
    def _build_conditions(cls, filters: Dict[str, Any]) -> List[Any]:
        result = []

        for field_name, value in filters.items():
            field = getattr(cls, field_name)
            result.append(field == value)
        return result


class BaseModelWithName(BaseModel):
    __abstract__ = True

    name = sa.Column(sa.String, nullable=False, unique=True)

    @classmethod
    async def get_by_name(cls, db: AsyncSession, name: str) -> Optional[Self]:
        return await cls.get_one_by_filter(db, [cls.name == name])


class BaseModelSoftDelete(BaseModel):
    __abstract__ = True

    is_active = sa.Column(sa.Boolean, nullable=False, server_default="true", default=True)

    async def delete(self, db: AsyncSession) -> None:
        self.is_active = False
        await self.save(db)

    async def undelete(self, db: AsyncSession) -> None:
        self.is_active = True
        await self.save(db)
