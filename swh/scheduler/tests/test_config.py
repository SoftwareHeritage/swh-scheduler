# Copyright (C) 2021 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.scheduler.celery_backend.config import route_for_task


@pytest.mark.parametrize("name", ["swh.something", "swh.anything"])
def test_route_for_task_routing(name):
    assert route_for_task(name, [], {}, {}) == {"queue": name}


@pytest.mark.parametrize("name", [None, "foobar"])
def test_route_for_task_no_routing(name):
    assert route_for_task(name, [], {}, {}) is None
