# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

import swh.scheduler.simulator as simulator
from swh.scheduler.tests.common import TASK_TYPES

NUM_ORIGINS = 42
TEST_RUNTIME = 1000


@pytest.fixture
def monkeypatch_get_scheduler(swh_scheduler, monkeypatch):
    def get_scheduler(*args, **kwargs):
        return swh_scheduler

    monkeypatch.setattr(simulator, "get_scheduler", get_scheduler)

    for task_type in TASK_TYPES.values():
        swh_scheduler.create_task_type(task_type)


def test_fill_test_data(swh_scheduler, monkeypatch_get_scheduler):
    simulator.fill_test_data(num_origins=NUM_ORIGINS)

    res = swh_scheduler.get_listed_origins()
    assert len(res.origins) == NUM_ORIGINS
    assert res.next_page_token is None

    res = swh_scheduler.search_tasks()
    assert len(res) == NUM_ORIGINS


@pytest.mark.parametrize("policy", ("oldest_scheduled_first",))
def test_run_origin_scheduler(swh_scheduler, monkeypatch_get_scheduler, policy):
    simulator.fill_test_data(num_origins=NUM_ORIGINS)
    simulator.run("origin_scheduler", policy=policy, runtime=TEST_RUNTIME)


def test_run_task_scheduler(swh_scheduler, monkeypatch_get_scheduler):
    simulator.fill_test_data(num_origins=NUM_ORIGINS)
    simulator.run("task_scheduler", policy=None, runtime=TEST_RUNTIME)
