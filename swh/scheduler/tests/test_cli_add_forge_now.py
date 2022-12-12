# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict, Tuple

import attr
import pytest

from swh.scheduler.cli.utils import lister_task_type
from swh.scheduler.interface import SchedulerInterface
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
    "cmd_args, subcmd_args",
    [
        ([], []),
        ([], ["--lister-name", "github", "--lister-instance-name", "github"]),
        (["--preset", "staging"], []),
    ],
)
def test_schedule_first_visits_cli(
    mocker,
    swh_scheduler,
    swh_scheduler_celery_app,
    listed_origins_by_type,
    cmd_args,
    subcmd_args,
):
    for task_type in TASK_TYPES.values():
        swh_scheduler.create_task_type(task_type)

    visit_type = next(iter(listed_origins_by_type))

    # enabled origins by default except when --staging flag is provided
    enabled = "staging" not in cmd_args

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

    command_args = (
        cmd_args + ["schedule-first-visits", "--type-name", visit_type] + subcmd_args
    )

    result = invoke(swh_scheduler, args=tuple(command_args))
    assert result.exit_code == 0

    scheduled_tasks = {
        (call[0][0], call[1]["kwargs"]["url"]) for call in send_task.call_args_list
    }

    expected_tasks = {
        (TASK_TYPES[origin.visit_type]["backend_name"], origin.url)
        for origin in listed_origins_by_type[visit_type]
    }

    assert scheduled_tasks == expected_tasks


def _create_task_type(
    swh_scheduler: SchedulerInterface, lister_name: str, listing_type: str = "full"
) -> Dict:
    task_type = {
        "type": lister_task_type(lister_name, listing_type),  # only relevant bit
        "description": f"{listing_type} listing",
        "backend_name": "swh.example.backend",
        "default_interval": "1 day",
        "min_interval": "1 day",
        "max_interval": "1 day",
        "backoff_factor": "1",
        "max_queue_length": "100",
        "num_retries": 3,
    }
    swh_scheduler.create_task_type(task_type)
    task_type = swh_scheduler.get_task_type(task_type["type"])
    assert task_type is not None
    return task_type


@pytest.mark.parametrize("preset", ["staging", "production"])
def test_schedule_register_lister(swh_scheduler, stored_lister, preset):
    # given
    assert stored_lister is not None
    lister_name = stored_lister.name
    # Let's create all possible associated lister task types
    full = _create_task_type(swh_scheduler, lister_name, "full")
    incremental = _create_task_type(swh_scheduler, lister_name, "incremental")

    # Let's trigger the registering of that lister
    result = invoke(
        swh_scheduler,
        [
            "--preset",
            preset,
            "register-lister",
            lister_name,
            "url=https://example.org",
        ],
    )

    output = result.output.lstrip()

    expected_msgs = []
    if preset == "production":
        # 2 tasks: 1 full + 1 incremental (tomorrow) with recurring policy
        expected_msgs = ["Policy: recurring", incremental["type"], "Next run: tomorrow"]
    else:
        # 1 task full with policy oneshot
        expected_msgs = ["Policy: oneshot"]

    # In any case, there is the full listing type too
    expected_msgs.append(full["type"])

    assert len(expected_msgs) > 0
    for msg in expected_msgs:
        assert msg in output


def test_register_lister_unknown_task_type(swh_scheduler):
    """When scheduling unknown task type, the cli should raise."""
    with pytest.raises(ValueError, match="Unknown"):
        invoke(
            swh_scheduler,
            [
                "register-lister",
                "unknown-lister-type-should-raise",
            ],
        )
