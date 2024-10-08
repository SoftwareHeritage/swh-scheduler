# Copyright (C) 2021-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import timedelta
from typing import Tuple
from unittest.mock import MagicMock

import pytest

from swh.scheduler.cli.origin import format_origins
from swh.scheduler.model import ListedOrigin, OriginVisitStats
from swh.scheduler.tests.common import TASK_TYPES
from swh.scheduler.tests.test_cli import invoke as basic_invoke
from swh.scheduler.utils import utcnow


def invoke(
    scheduler, args: Tuple[str, ...] = (), catch_exceptions: bool = False, **kwargs
):
    return basic_invoke(
        scheduler, args=["origin", *args], catch_exceptions=catch_exceptions, **kwargs
    )


def test_cli_origin(swh_scheduler):
    """Check that swh scheduler origin returns its help text"""

    result = invoke(swh_scheduler)

    assert "Commands:" in result.stdout


def test_format_origins_basic(listed_origins):
    listed_origins = listed_origins[:100]

    ctx = MagicMock()

    basic_output = list(format_origins(ctx, listed_origins))
    assert ctx.method_calls == []
    # 1 header line + all origins
    assert len(basic_output) == len(listed_origins) + 1

    no_header_output = list(format_origins(ctx, listed_origins, with_header=False))
    assert ctx.method_calls == []
    assert basic_output[1:] == no_header_output


def test_format_origins_fields_unknown(listed_origins):
    listed_origins = listed_origins[:10]

    ctx = MagicMock()
    it = format_origins(ctx, listed_origins, fields=["unknown_field"])
    assert ctx.method_calls == []

    next(it)
    ctx.fail.assert_called()


def test_format_origins_fields(listed_origins):
    listed_origins = listed_origins[:10]
    fields = ["lister_id", "url", "visit_type"]

    ctx = MagicMock()
    output = list(format_origins(ctx, listed_origins, fields=fields))
    assert ctx.method_calls == []
    assert output[0] == ",".join(fields)
    for i, origin in enumerate(listed_origins):
        assert output[i + 1] == f"{origin.lister_id},{origin.url},{origin.visit_type}"


def test_grab_next(swh_scheduler, listed_origins_by_type):
    NUM_RESULTS = 10
    # Strict inequality to check that grab_next_visits doesn't return more
    # results than requested

    # XXX: should test all of 'listed_origins_by_type' here...
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

    scheduled_tasks = {(task.type, task.arguments.kwargs["url"]) for task in tasks}
    all_possible_tasks = {
        (f"load-{origin.visit_type}", origin.url)
        for origin in listed_origins_by_type[visit_type]
    }

    assert scheduled_tasks <= all_possible_tasks


def test_send_to_celery_unknown_visit_type(
    swh_scheduler,
):
    "Calling cli without a known visit type should raise"
    result = invoke(
        swh_scheduler,
        args=("send-origins-from-scheduler-to-celery", "unknown-visit-type"),
        catch_exceptions=True,
    )
    assert "Unknown" in result.output
    assert result.exit_code != 0


@pytest.mark.parametrize(
    "extra_cmd_args",
    [
        [],
        ["--lister-name", "github", "--lister-instance-name", "github"],
        [
            "--absolute-cooldown",
            "24 days",
            "--scheduled-cooldown",
            "24 hours",
            "--failed-cooldown",
            "1 day",
            "--not-found-cooldown",
            "60 days",
        ],
    ],
)
def test_send_origins_from_scheduler_to_celery(
    mocker,
    swh_scheduler,
    task_types,
    swh_scheduler_celery_app,
    listed_origins_by_type,
    extra_cmd_args,
):
    visit_type = next(iter(listed_origins_by_type))

    for origins in listed_origins_by_type.values():
        swh_scheduler.record_listed_origins(origins)

    get_queue_length = mocker.patch(
        "swh.scheduler.celery_backend.config.get_queue_length"
    )
    get_queue_length.return_value = None

    send_task = mocker.patch.object(swh_scheduler_celery_app, "send_task")
    send_task.return_value = None

    cmd_args = ["send-origins-from-scheduler-to-celery", visit_type] + extra_cmd_args

    result = invoke(swh_scheduler, args=tuple(cmd_args))
    assert result.exit_code == 0

    scheduled_tasks = {
        (call[0][0], call[1]["kwargs"]["url"]) for call in send_task.call_args_list
    }

    expected_tasks = {
        (TASK_TYPES[origin.visit_type].backend_name, origin.url)
        for origin in listed_origins_by_type[visit_type]
    }

    assert expected_tasks == scheduled_tasks


def test_update_metrics(swh_scheduler, listed_origins):
    swh_scheduler.record_listed_origins(listed_origins)

    assert swh_scheduler.get_metrics() == []

    result = invoke(swh_scheduler, args=("update-metrics",))

    assert result.exit_code == 0
    assert swh_scheduler.get_metrics() != []


@pytest.mark.parametrize(
    "limit,queue_name_prefix,dry_run,debug",
    [
        (None, "", False, False),
        (10, "", False, False),
        (None, "large-repository", False, False),
        (None, "", True, False),
        (None, "", False, True),
    ],
)
def test_send_origins_from_file_to_celery_cli(
    mocker,
    swh_scheduler,
    swh_scheduler_celery_app,
    task_types,
    listed_origins_by_type,
    limit,
    queue_name_prefix,
    dry_run,
    debug,
):
    visit_type = "test-git"
    origins_to_schedule = listed_origins_by_type[visit_type][:20]

    task_type_param = next(iter(task_types))
    origins_to_send = [o.url for o in origins_to_schedule]

    get_queue_length = mocker.patch(
        "swh.scheduler.celery_backend.config.get_queue_length"
    )
    get_queue_length.return_value = None

    send_task = mocker.patch.object(swh_scheduler_celery_app, "send_task")
    send_task.return_value = None

    extra_cmd_args = []
    if limit:
        extra_cmd_args += ["--limit", limit]
    if queue_name_prefix:
        extra_cmd_args += ["--queue-name-prefix", queue_name_prefix]
    if dry_run:
        extra_cmd_args += ["--dry-run"]
    if debug:
        extra_cmd_args += ["--debug"]

    cmd_args = ["send-origins-from-file-to-celery"] + extra_cmd_args + [task_type_param]

    input_data = ("\n").join(origins_to_send)
    result = invoke(swh_scheduler, args=tuple(cmd_args), input=input_data)
    assert result.exit_code == 0

    if dry_run:
        assert "** DRY-RUN **" in result.output

    if debug:
        assert "Destination queue" in result.output

    actual_scheduled_tasks = {
        (call[1]["name"], call[1]["kwargs"]["url"], call[1]["queue"])
        for call in send_task.call_args_list
    }

    backend_name = task_types[task_type_param].backend_name
    queue = f"{queue_name_prefix}:{backend_name}" if queue_name_prefix else backend_name
    expected_tasks = (
        {}
        if dry_run
        else {
            (backend_name, origin.url, queue)
            for origin in origins_to_schedule[
                : limit if limit else len(origins_to_schedule)
            ]
        }
    )

    assert set(actual_scheduled_tasks) == set(expected_tasks)


def test_schedule_high_priority_first_visits(
    swh_scheduler, swh_scheduler_celery_app, visit_types, task_types, mocker
):
    nb_origins_per_visit_type = 10
    # number of celery queue slots per visit type is lower than the
    # number of listed origins for that visit type
    nb_available_queue_slots = nb_origins_per_visit_type // 2

    mocker.patch.object(swh_scheduler_celery_app, "send_task")
    # mock number of available slots in queues to a fixed value
    mocker.patch(
        "swh.scheduler.celery_backend.config.get_available_slots"
    ).return_value = nb_available_queue_slots

    # create a lister with high priority first visits
    lister_name = "save-bulk"
    lister_instance_name = "foo"
    lister = swh_scheduler.get_or_create_lister(
        name=lister_name,
        instance_name=lister_instance_name,
        first_visits_queue_prefix="save_bulk",
    )
    # register origins for the lister
    for visit_type in visit_types:
        listed_origins = [
            ListedOrigin(
                lister_id=lister.id,
                url=f"https://{visit_type}.example.org/project{i}",
                visit_type=visit_type,
            )
            for i in range(nb_origins_per_visit_type)
        ]
        swh_scheduler.record_listed_origins(listed_origins)
        # mark some origins visits already scheduled in the past to
        # check they are scheduled again by the tested command
        swh_scheduler.origin_visit_stats_upsert(
            [
                OriginVisitStats(
                    url=listed_origin.url,
                    visit_type=listed_origin.visit_type,
                    last_scheduled=utcnow() - timedelta(days=180),
                )
                for i, listed_origin in enumerate(listed_origins)
                if i % 2 == 1
            ]
        )

    # mark listing as finished
    lister = lister.evolve(last_listing_finished_at=utcnow())
    lister = swh_scheduler.update_lister(lister)

    # start scheduling first visits
    result = invoke(swh_scheduler, args=("schedule-high-priority-first-visits",))
    assert result.exit_code == 0
    # check expected number of visits were scheduled
    for visit_type in visit_types:
        assert (
            f"{nb_available_queue_slots} visits of type {visit_type} to send to celery"
            in result.stdout
        )
    # check there is still origin first visits to schedule
    assert (
        f"All first visits of origins registered by lister with name '{lister_name}' "
        f"and instance '{lister_instance_name}' were scheduled.'"
    ) not in result.stdout
    lister = swh_scheduler.get_lister(
        name=lister_name, instance_name=lister_instance_name
    )
    assert lister.first_visits_scheduled_at is None

    # continue scheduling first visits
    result = invoke(swh_scheduler, args=("schedule-high-priority-first-visits",))
    assert result.exit_code == 0
    # check expected number of visits were scheduled
    for visit_type in visit_types:
        assert (
            f"{nb_available_queue_slots} visits of type {visit_type} to send to celery"
            in result.stdout
        )
    # check all listed origins first visits were scheduled
    assert (
        f"All first visits of origins registered by lister with name '{lister_name}' "
        f"and instance '{lister_instance_name}' were scheduled.'"
    ) in result.stdout
    lister = swh_scheduler.get_lister(
        name=lister_name, instance_name=lister_instance_name
    )
    assert lister.first_visits_scheduled_at is not None
