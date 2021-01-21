# Copyright (C) 2016-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timezone
import os
from typing import Dict, List

import pytest

from swh.scheduler.model import ListedOrigin, Lister
from swh.scheduler.tests.common import LISTERS

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
    return ["git", "svn"]


@pytest.fixture
def listed_origins_by_type(
    stored_lister: Lister, visit_types: List[str]
) -> Dict[str, List[ListedOrigin]]:
    """A fixed list of `ListedOrigin`s, for each `visit_type`."""
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
            )
            for i in range(count_per_type)
        ]
        for j, visit_type in enumerate(visit_types)
    }


@pytest.fixture
def listed_origins(listed_origins_by_type) -> List[ListedOrigin]:
    """Return a (fixed) set of listed origins"""
    return sum(listed_origins_by_type.values(), [])
