# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Tuple

import pytest

from swh.scheduler.cli.origin import format_origins
from swh.scheduler.tests.common import TASK_TYPES
from swh.scheduler.tests.test_cli import invoke as basic_invoke


def invoke(scheduler, args: Tuple[str, ...] = (), catch_exceptions: bool = False):
    return basic_invoke(
        scheduler, args=["origin", *args], catch_exceptions=catch_exceptions
    )


def test_cli_origin(swh_scheduler):
    """Check that swh scheduler origin returns its help text"""

    result = invoke(swh_scheduler)

    assert "Commands:" in result.stdout


def test_format_origins_basic(listed_origins):
    listed_origins = listed_origins[:100]

    basic_output = list(format_origins(listed_origins))
    # 1 header line + all origins
    assert len(basic_output) == len(listed_origins) + 1

    no_header_output = list(format_origins(listed_origins, with_header=False))
    assert basic_output[1:] == no_header_output


def test_format_origins_fields_unknown(listed_origins):
    listed_origins = listed_origins[:10]

    it = format_origins(listed_origins, fields=["unknown_field"])

    with pytest.raises(ValueError, match="unknown_field"):
        next(it)


def test_format_origins_fields(listed_origins):
    listed_origins = listed_origins[:10]
    fields = ["lister_id", "url", "visit_type"]

    output = list(format_origins(listed_origins, fields=fields))
    assert output[0] == ",".join(fields)
    for i, origin in enumerate(listed_origins):
        assert output[i + 1] == f"{origin.lister_id},{origin.url},{origin.visit_type}"


def test_grab_next(swh_scheduler, listed_origins_by_type):
    NUM_RESULTS = 10
    # Strict inequality to check that grab_next_visits doesn't return more
    # results than requested
    visit_type = next(iter(listed_origins_by_type))
    assert len(listed_origins_by_type[visit_type]) > NUM_RESULTS

    for origins in listed_origins_by_type.values():
        swh_scheduler.record_listed_origins(origins)

    result = invoke(swh_scheduler, args=("grab-next", visit_type, str(NUM_RESULTS)))
    assert result.exit_code == 0

    out_lines = result.stdout.splitlines()
    assert len(out_lines) == NUM_RESULTS + 1

    fields = out_lines[0].split(",")
    returned_origins = [dict(zip(fields, line.split(","))) for line in out_lines[1:]]

    # Check that we've received origins we had listed in the first place
    assert set(origin["url"] for origin in returned_origins) <= set(
        origin.url for origin in listed_origins_by_type[visit_type]
    )


def test_schedule_next(swh_scheduler, listed_origins_by_type):
    for task_type in TASK_TYPES.values():
        swh_scheduler.create_task_type(task_type)

    NUM_RESULTS = 10
    # Strict inequality to check that grab_next_visits doesn't return more
    # results than requested
    visit_type = next(iter(listed_origins_by_type))
    assert len(listed_origins_by_type[visit_type]) > NUM_RESULTS

    for origins in listed_origins_by_type.values():
        swh_scheduler.record_listed_origins(origins)

    result = invoke(swh_scheduler, args=("schedule-next", visit_type, str(NUM_RESULTS)))
    assert result.exit_code == 0

    # pull all tasks out of the scheduler
    tasks = swh_scheduler.search_tasks()
    assert len(tasks) == NUM_RESULTS

    scheduled_tasks = {
        (task["type"], task["arguments"]["kwargs"]["url"]) for task in tasks
    }
    all_possible_tasks = {
        (f"load-{origin.visit_type}", origin.url)
        for origin in listed_origins_by_type[visit_type]
    }

    assert scheduled_tasks <= all_possible_tasks


def test_send_to_celery(
    mocker, swh_scheduler, swh_scheduler_celery_app, listed_origins_by_type,
):
    for task_type in TASK_TYPES.values():
        swh_scheduler.create_task_type(task_type)

    visit_type = next(iter(listed_origins_by_type))

    for origins in listed_origins_by_type.values():
        swh_scheduler.record_listed_origins(origins)

    get_queue_length = mocker.patch(
        "swh.scheduler.celery_backend.config.get_queue_length"
    )
    get_queue_length.return_value = None

    send_task = mocker.patch.object(swh_scheduler_celery_app, "send_task")
    send_task.return_value = None

    result = invoke(swh_scheduler, args=("send-to-celery", visit_type))
    assert result.exit_code == 0

    scheduled_tasks = {
        (call[0][0], call[1]["kwargs"]["url"]) for call in send_task.call_args_list
    }

    expected_tasks = {
        (TASK_TYPES[origin.visit_type]["backend_name"], origin.url)
        for origin in listed_origins_by_type[visit_type]
    }

    assert expected_tasks == scheduled_tasks


def test_update_metrics(swh_scheduler, listed_origins):
    swh_scheduler.record_listed_origins(listed_origins)

    assert swh_scheduler.get_metrics() == []

    result = invoke(swh_scheduler, args=("update-metrics",))

    assert result.exit_code == 0
    assert swh_scheduler.get_metrics() != []
