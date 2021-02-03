# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import io

import pytest

from swh.core.api.classes import stream_results
import swh.scheduler.simulator as simulator
from swh.scheduler.tests.common import TASK_TYPES

NUM_ORIGINS = 42
TEST_RUNTIME = 1000


def test_fill_test_data(swh_scheduler):
    for task_type in TASK_TYPES.values():
        swh_scheduler.create_task_type(task_type)

    simulator.fill_test_data(swh_scheduler, num_origins=NUM_ORIGINS)

    origins = list(stream_results(swh_scheduler.get_listed_origins))
    assert len(origins) == NUM_ORIGINS

    res = swh_scheduler.search_tasks()
    assert len(res) == NUM_ORIGINS


@pytest.mark.parametrize(
    "policy",
    (
        "oldest_scheduled_first",
        "never_visited_oldest_update_first",
        "already_visited_order_by_lag",
    ),
)
def test_run_origin_scheduler(swh_scheduler, policy):
    for task_type in TASK_TYPES.values():
        swh_scheduler.create_task_type(task_type)

    simulator.fill_test_data(swh_scheduler, num_origins=NUM_ORIGINS)
    simulator.run(
        swh_scheduler,
        scheduler_type="origin_scheduler",
        policy=policy,
        runtime=TEST_RUNTIME,
    )


def test_run_task_scheduler(swh_scheduler):
    for task_type in TASK_TYPES.values():
        swh_scheduler.create_task_type(task_type)

    simulator.fill_test_data(swh_scheduler, num_origins=NUM_ORIGINS)
    report = simulator.run(
        swh_scheduler,
        scheduler_type="task_scheduler",
        policy=None,
        runtime=TEST_RUNTIME,
    )

    # just check these SimulationReport methods do not crash
    assert report.format(with_plots=True)
    assert report.format(with_plots=False)
    fobj = io.StringIO()
    report.metrics_csv(fobj=fobj)
    assert fobj.getvalue()
