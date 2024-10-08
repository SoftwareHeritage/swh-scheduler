# Copyright (C) 2016-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timezone
import os
from pathlib import Path
from typing import Dict, List, Optional
from unittest.mock import patch

import pytest

from swh.scheduler.model import ListedOrigin, Lister
from swh.scheduler.tests.common import LISTERS, TASK_TYPES

DATADIR = Path(__file__).parent.absolute() / "tests/data"


# make sure we are not fooled by CELERY_ config environment vars
for var in [x for x in os.environ.keys() if x.startswith("CELERY")]:
    os.environ.pop(var)


# test_cli tests depends on a en/C locale, so ensure it
os.environ["LC_ALL"] = "C.UTF-8"


@pytest.fixture
def stored_lister(swh_scheduler) -> Lister:
    """Store a lister in the scheduler and return its information"""
    return swh_scheduler.get_or_create_lister(**LISTERS[0])


@pytest.fixture
def visit_types() -> List[str]:
    """Possible visit types in `ListedOrigin`s"""
    return list(TASK_TYPES)


@pytest.fixture
def listed_origins_by_type(
    stored_lister: Lister, visit_types: List[str]
) -> Dict[str, List[ListedOrigin]]:
    """A fixed list of `ListedOrigin`s, for each `visit_type`."""

    def is_fork_field(i: int) -> Optional[bool]:
        return [True, False, None][i % 3]

    def forked_from_url_field(visit_type: str, i: int) -> Optional[str]:
        if is_fork_field(i) and i % 4 == 0:
            return f"https://{visit_type}.example.com/src_{i:04d}"
        return None

    count_per_type = 1000
    assert stored_lister.id
    return {
        visit_type: [
            ListedOrigin(
                lister_id=stored_lister.id,
                url=f"https://{visit_type}.example.com/{i:04d}",
                visit_type=visit_type,
                last_update=datetime(
                    2020, 6, 15, 16, 0, 0, j * count_per_type + i, tzinfo=timezone.utc
                ),
                is_fork=is_fork_field(i),
                forked_from_url=forked_from_url_field(visit_type, i),
            )
            for i in range(count_per_type)
        ]
        for j, visit_type in enumerate(visit_types)
    }


@pytest.fixture
def listed_origins(listed_origins_by_type) -> List[ListedOrigin]:
    """Return a (fixed) set of listed origins"""
    return sum(listed_origins_by_type.values(), [])


@pytest.fixture
def listed_origins_with_non_enabled(listed_origins) -> List[ListedOrigin]:
    """Return a (fixed) set of listed origins"""
    for i, origin in enumerate(listed_origins):
        origin.enabled = i % 2 == 0
    return listed_origins


@pytest.fixture
def storage(swh_storage):
    """An instance of in-memory storage that gets injected
    into the CLI functions."""
    with patch("swh.storage.get_storage") as get_storage_mock:
        get_storage_mock.return_value = swh_storage
        yield swh_storage


@pytest.fixture
def datadir():
    return DATADIR


@pytest.fixture
def task_types(swh_scheduler):
    all_task_types = {}
    for task_type in TASK_TYPES.values():
        swh_scheduler.create_task_type(task_type)
        all_task_types[task_type.type] = task_type
    return all_task_types
