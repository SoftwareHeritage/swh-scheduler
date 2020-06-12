# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from uuid import UUID
from typing import Any, Dict, List, Optional, Tuple

import attr
import attr.converters
from attrs_strict import type_validator


@attr.s
class BaseSchedulerModel:
    _select_cols: Optional[Tuple[str, ...]] = None
    _insert_cols_and_metavars: Optional[Tuple[Tuple[str, ...], Tuple[str, ...]]] = None

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

        This supports the following attributes as booleans in the field's metadata:
          - primary_key: handled by the database; never inserted or updated
          - auto_now_add: handled by the database; set to now() on insertion, never
            updated
          - auto_now: handled by the client; set to now() on every insertion and update
        """
        if cls._insert_cols_and_metavars is None:
            zipped_cols_and_metavars: List[Tuple[str, str]] = []

            for field in attr.fields(cls):
                if any(
                    field.metadata.get(flag) for flag in ("auto_now_add", "primary_key")
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
    instance_name = attr.ib(type=str, validator=[type_validator()], factory=str)

    # Populated by database
    id = attr.ib(
        type=Optional[UUID],
        validator=type_validator(),
        default=None,
        metadata={"primary_key": True},
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
