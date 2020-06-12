# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import List, Optional, Tuple

import attr
import attr.converters


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
