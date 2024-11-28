# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from .test_scheduler import TestScheduler as TestMemoryScheduler  # noqa: F401


@pytest.fixture
def swh_scheduler_class():
    return "memory"


@pytest.fixture
def swh_scheduler_config():
    return {}
