# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Tuple

import attr
import pytest

from swh.scheduler.tests.common import TASK_TYPES
from swh.scheduler.tests.test_cli import invoke as basic_invoke


def invoke(scheduler, args: Tuple[str, ...] = (), catch_exceptions: bool = False):
    return basic_invoke(
        scheduler, args=["add-forge-now", *args], catch_exceptions=catch_exceptions
    )


def test_schedule_first_visits_cli_unknown_visit_type(
    swh_scheduler,
):
    "Calling cli without a known visit type should raise"
    with pytest.raises(ValueError, match="Unknown"):
        invoke(
            swh_scheduler,
            args=(
                "schedule-first-visits",
                "-t",
                "unknown-vt0",
                "--type-name",
                "unknown-visit-type1",
            ),
        )


@pytest.mark.parametrize(
    "extra_cmd_args",
    [
        [],
        ["--lister-name", "github", "--lister-instance-name", "github"],
        ["--staging"],
    ],
)
def test_schedule_first_visits_cli(
    mocker,
    swh_scheduler,
    swh_scheduler_celery_app,
    listed_origins_by_type,
    extra_cmd_args,
):
    for task_type in TASK_TYPES.values():
        swh_scheduler.create_task_type(task_type)

    visit_type = next(iter(listed_origins_by_type))

    # enabled origins by default except when --staging flag is provided
    enabled = "--staging" not in extra_cmd_args

    for origins in listed_origins_by_type.values():
        swh_scheduler.record_listed_origins(
            (attr.evolve(o, enabled=enabled) for o in origins)
        )

    get_queue_length = mocker.patch(
        "swh.scheduler.celery_backend.config.get_queue_length"
    )
    get_queue_length.return_value = None

    send_task = mocker.patch.object(swh_scheduler_celery_app, "send_task")
    send_task.return_value = None

    cmd_args = ["schedule-first-visits", "--type-name", visit_type] + extra_cmd_args

    result = invoke(swh_scheduler, args=tuple(cmd_args))
    assert result.exit_code == 0

    scheduled_tasks = {
        (call[0][0], call[1]["kwargs"]["url"]) for call in send_task.call_args_list
    }

    expected_tasks = {
        (TASK_TYPES[origin.visit_type]["backend_name"], origin.url)
        for origin in listed_origins_by_type[visit_type]
    }

    assert expected_tasks == scheduled_tasks
