# Copyright (C) 2020-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from typing import Any, Dict, List, Optional, Tuple, Union
from uuid import UUID

import attr
import attr.converters
from attrs_strict import type_validator


def check_timestamptz(value) -> None:
    """Checks the date has a timezone."""
    if value is not None and value.tzinfo is None:
        raise ValueError("date must be a timezone-aware datetime.")


@attr.s
class BaseSchedulerModel:
    """Base class for database-backed objects.

    These database-backed objects are defined through attrs-based attributes
    that match the columns of the database 1:1. This is a (very) lightweight
    ORM.

    These attrs-based attributes have metadata specific to the functionality
    expected from these fields in the database:

     - `primary_key`: the column is a primary key; it should be filtered out
       when doing an `update` of the object
     - `auto_primary_key`: the column is a primary key, which is automatically handled
       by the database. It will not be inserted to. This must be matched with a
       database-side default value.
     - `auto_now_add`: the column is a timestamp that is set to the current time when
       the object is inserted, and never updated afterwards. This must be matched with
       a database-side default value.
     - `auto_now`: the column is a timestamp that is set to the current time when
       the object is inserted or updated.

    """

    _pk_cols: Optional[Tuple[str, ...]] = None
    _select_cols: Optional[Tuple[str, ...]] = None
    _insert_cols_and_metavars: Optional[Tuple[Tuple[str, ...], Tuple[str, ...]]] = None

    @classmethod
    def primary_key_columns(cls) -> Tuple[str, ...]:
        """Get the primary key columns for this object type"""
        if cls._pk_cols is None:
            columns: List[str] = []
            for field in attr.fields(cls):
                if any(
                    field.metadata.get(flag)
                    for flag in ("auto_primary_key", "primary_key")
                ):
                    columns.append(field.name)
            cls._pk_cols = tuple(sorted(columns))

        return cls._pk_cols

    @classmethod
    def select_columns(cls) -> Tuple[str, ...]:
        """Get all the database columns needed for a `select` on this object type"""
        if cls._select_cols is None:
            columns: List[str] = []
            for field in attr.fields(cls):
                columns.append(field.name)
            cls._select_cols = tuple(sorted(columns))

        return cls._select_cols

    @classmethod
    def insert_columns_and_metavars(cls) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
        """Get the database columns and metavars needed for an `insert` or `update` on
           this object type.

        This implements support for the `auto_*` field metadata attributes.
        """
        if cls._insert_cols_and_metavars is None:
            zipped_cols_and_metavars: List[Tuple[str, str]] = []

            for field in attr.fields(cls):
                if any(
                    field.metadata.get(flag)
                    for flag in ("auto_now_add", "auto_primary_key")
                ):
                    continue
                elif field.metadata.get("auto_now"):
                    zipped_cols_and_metavars.append((field.name, "now()"))
                else:
                    zipped_cols_and_metavars.append((field.name, f"%({field.name})s"))

            zipped_cols_and_metavars.sort()

            cols, metavars = zip(*zipped_cols_and_metavars)
            cls._insert_cols_and_metavars = cols, metavars

        return cls._insert_cols_and_metavars


@attr.s
class Lister(BaseSchedulerModel):
    name = attr.ib(type=str, validator=[type_validator()])
    instance_name = attr.ib(type=str, validator=[type_validator()])

    # Populated by database
    id = attr.ib(
        type=Optional[UUID],
        validator=type_validator(),
        default=None,
        metadata={"auto_primary_key": True},
    )

    current_state = attr.ib(
        type=Dict[str, Any], validator=[type_validator()], factory=dict
    )
    created = attr.ib(
        type=Optional[datetime.datetime],
        validator=[type_validator()],
        default=None,
        metadata={"auto_now_add": True},
    )
    updated = attr.ib(
        type=Optional[datetime.datetime],
        validator=[type_validator()],
        default=None,
        metadata={"auto_now": True},
    )


@attr.s
class ListedOrigin(BaseSchedulerModel):
    """Basic information about a listed origin, output by a lister"""

    lister_id = attr.ib(
        type=UUID, validator=[type_validator()], metadata={"primary_key": True}
    )
    url = attr.ib(
        type=str, validator=[type_validator()], metadata={"primary_key": True}
    )
    visit_type = attr.ib(
        type=str, validator=[type_validator()], metadata={"primary_key": True}
    )
    extra_loader_arguments = attr.ib(
        type=Dict[str, str], validator=[type_validator()], factory=dict
    )

    last_update = attr.ib(
        type=Optional[datetime.datetime], validator=[type_validator()], default=None,
    )

    enabled = attr.ib(type=bool, validator=[type_validator()], default=True)

    first_seen = attr.ib(
        type=Optional[datetime.datetime],
        validator=[type_validator()],
        default=None,
        metadata={"auto_now_add": True},
    )
    last_seen = attr.ib(
        type=Optional[datetime.datetime],
        validator=[type_validator()],
        default=None,
        metadata={"auto_now": True},
    )

    def as_task_dict(self):
        return {
            "type": f"load-{self.visit_type}",
            "arguments": {
                "args": [],
                "kwargs": {"url": self.url, **self.extra_loader_arguments},
            },
        }


ListedOriginPageToken = Tuple[UUID, str]


def convert_listed_origin_page_token(
    input: Union[None, ListedOriginPageToken, List[Union[UUID, str]]]
) -> Optional[ListedOriginPageToken]:
    if input is None:
        return None

    if isinstance(input, tuple):
        return input

    x, y = input
    assert isinstance(x, UUID)
    assert isinstance(y, str)
    return (x, y)


@attr.s
class PaginatedListedOriginList(BaseSchedulerModel):
    """A list of listed origins, with a continuation token"""

    origins = attr.ib(type=List[ListedOrigin], validator=[type_validator()])
    next_page_token = attr.ib(
        type=Optional[ListedOriginPageToken],
        validator=[type_validator()],
        converter=convert_listed_origin_page_token,
        default=None,
    )


@attr.s(frozen=True, slots=True)
class OriginVisitStats(BaseSchedulerModel):
    """Represents an aggregated origin visits view.
    """

    url = attr.ib(
        type=str, validator=[type_validator()], metadata={"primary_key": True}
    )
    visit_type = attr.ib(
        type=str, validator=[type_validator()], metadata={"primary_key": True}
    )
    last_eventful = attr.ib(
        type=Optional[datetime.datetime], validator=type_validator()
    )
    last_uneventful = attr.ib(
        type=Optional[datetime.datetime], validator=type_validator()
    )
    last_failed = attr.ib(type=Optional[datetime.datetime], validator=type_validator())
    last_notfound = attr.ib(
        type=Optional[datetime.datetime], validator=type_validator()
    )
    last_scheduled = attr.ib(
        type=Optional[datetime.datetime], validator=[type_validator()], default=None,
    )
    last_snapshot = attr.ib(
        type=Optional[bytes], validator=type_validator(), default=None
    )

    @last_eventful.validator
    def check_last_eventful(self, attribute, value):
        check_timestamptz(value)

    @last_uneventful.validator
    def check_last_uneventful(self, attribute, value):
        check_timestamptz(value)

    @last_failed.validator
    def check_last_failed(self, attribute, value):
        check_timestamptz(value)

    @last_notfound.validator
    def check_last_notfound(self, attribute, value):
        check_timestamptz(value)


@attr.s(frozen=True, slots=True)
class SchedulerMetrics(BaseSchedulerModel):
    """Metrics for the scheduler, aggregated by (lister_id, visit_type)"""

    lister_id = attr.ib(
        type=UUID, validator=[type_validator()], metadata={"primary_key": True}
    )
    visit_type = attr.ib(
        type=str, validator=[type_validator()], metadata={"primary_key": True}
    )

    last_update = attr.ib(
        type=Optional[datetime.datetime], validator=[type_validator()], default=None,
    )

    origins_known = attr.ib(type=int, validator=[type_validator()], default=0)
    """Number of known (enabled or disabled) origins"""

    origins_enabled = attr.ib(type=int, validator=[type_validator()], default=0)
    """Number of origins that were present in the latest listings"""

    origins_never_visited = attr.ib(type=int, validator=[type_validator()], default=0)
    """Number of enabled origins that have never been visited
    (according to the visit cache)"""

    origins_with_pending_changes = attr.ib(
        type=int, validator=[type_validator()], default=0
    )
    """Number of enabled origins with known activity (recorded by a lister)
    since our last visit"""
