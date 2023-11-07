# Copyright (C) 2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest
from testing.postgresql import find_program

from .test_scheduler import TestScheduler


def _has_initdb():
    try:
        find_program("initdb", ["bin"])
    except RuntimeError:
        return False
    else:
        return True


@pytest.fixture
def swh_scheduler_class():
    return "temporary"


@pytest.fixture
def swh_scheduler_config():
    return {}


pytestmark = pytest.mark.skipif(
    not _has_initdb(), reason="initdb executable is missing"
)


class TestTemporaryScheduler(TestScheduler):
    pass
