# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import timedelta
import logging
from queue import Queue
import time

import pytest
import yaml

from swh.scheduler.celery_backend.recurrent_visits import (
    DEFAULT_DVCS_POLICY,
    VisitSchedulerThreads,
    grab_next_visits_policy_weights,
    send_visits_for_visit_type,
    spawn_visit_scheduler_thread,
    terminate_visit_scheduler_threads,
    visit_scheduler_thread,
)

from .test_cli import invoke

TEST_MAX_QUEUE = 10000
MODULE_NAME = "swh.scheduler.celery_backend.recurrent_visits"


def _compute_backend_name(visit_type: str) -> str:
    "Build a dummy reproducible backend name"
    return f"swh.loader.{visit_type}.tasks"


@pytest.fixture
def swh_scheduler(swh_scheduler):
    """Override default fixture of the scheduler to install some more task types."""
    for visit_type in ["test-git", "test-hg", "test-svn"]:
        task_type = f"load-{visit_type}"
        swh_scheduler.create_task_type(
            {
                "type": task_type,
                "max_queue_length": TEST_MAX_QUEUE,
                "description": "The {} testing task".format(task_type),
                "backend_name": _compute_backend_name(visit_type),
                "default_interval": timedelta(days=1),
                "min_interval": timedelta(hours=6),
                "max_interval": timedelta(days=12),
            }
        )
    return swh_scheduler


@pytest.fixture
def all_task_types(swh_scheduler):
    return {
        task_type_d["type"]: task_type_d
        for task_type_d in swh_scheduler.get_task_types()
    }


def test_cli_schedule_recurrent_unknown_visit_type(swh_scheduler):
    """When passed an unknown visit type, the recurrent visit scheduler should refuse
    to start."""

    with pytest.raises(ValueError, match="Unknown"):
        invoke(
            swh_scheduler,
            False,
            [
                "schedule-recurrent",
                "--visit-type",
                "unknown",
                "--visit-type",
                "test-git",
            ],
        )


def test_cli_schedule_recurrent_noop(swh_scheduler, mocker):
    """When passing no visit types, the recurrent visit scheduler should start."""

    spawn_visit_scheduler_thread = mocker.patch(
        f"{MODULE_NAME}.spawn_visit_scheduler_thread"
    )
    spawn_visit_scheduler_thread.side_effect = SystemExit
    # The actual scheduling threads won't spawn, they'll immediately terminate. This
    # only exercises the logic to pull task types out of the database

    result = invoke(swh_scheduler, False, ["schedule-recurrent"])
    assert result.exit_code == 0, result.output


def test_send_visits_for_type_no_origin_scheduled_backoff(
    swh_scheduler, all_task_types, mocker
):
    visit_type = "test-git"
    task_type = f"load-{visit_type}"
    mocker.patch.object(time, "monotonic", lambda: 0)
    no_origins_scheduled_backoff = 60
    assert (
        send_visits_for_visit_type(
            swh_scheduler,
            mocker.MagicMock(),
            visit_type,
            all_task_types[task_type],
            DEFAULT_DVCS_POLICY,
            no_origins_scheduled_backoff,
        )
        == no_origins_scheduled_backoff
    )


def test_cli_schedule_recurrent_no_origins_scheduled_backoff_in_config(
    swh_scheduler, mocker
):
    """When passing no visit types, the recurrent visit scheduler should start."""

    config = """
    scheduler:
        cls: foo
        args: {}
    no_origins_scheduled_backoff: 60
    """

    spawn_visit_scheduler_thread = mocker.patch(
        f"{MODULE_NAME}.spawn_visit_scheduler_thread"
    )
    spawn_visit_scheduler_thread.side_effect = SystemExit
    # The actual scheduling threads won't spawn, they'll immediately terminate. This
    # only exercises the logic to pull task types out of the database

    result = invoke(
        swh_scheduler,
        False,
        ["schedule-recurrent", "--visit-type", "test-git"],
        config=config,
    )
    assert result.exit_code == 0, result.output

    assert yaml.safe_load(config) in spawn_visit_scheduler_thread.call_args[0]


def test_recurrent_visit_scheduling(
    swh_scheduler,
    caplog,
    listed_origins_by_type,
    all_task_types,
    mocker,
):
    """Scheduling known tasks is ok."""

    caplog.set_level(logging.DEBUG, MODULE_NAME)
    nb_origins = 1000

    mock_celery_app = mocker.MagicMock()
    mock_available_slots = mocker.patch(f"{MODULE_NAME}.get_available_slots")
    mock_available_slots.return_value = nb_origins  # Slots available in queue

    # Make sure the scheduler is properly configured in terms of visit/task types
    visit_types = list(listed_origins_by_type.keys())
    assert len(visit_types) > 0

    task_types = []
    origins = []
    for visit_type, _origins in listed_origins_by_type.items():
        origins.extend(swh_scheduler.record_listed_origins(_origins))
        task_type_name = f"load-{visit_type}"
        assert task_type_name in all_task_types.keys()
        task_type = all_task_types[task_type_name]
        task_type["visit_type"] = visit_type
        # we'll limit the orchestrator to the origins' type we know
        task_types.append(task_type)

    for visit_type in ["test-git", "test-svn"]:
        task_type = f"load-{visit_type}"
        send_visits_for_visit_type(
            swh_scheduler,
            mock_celery_app,
            visit_type,
            all_task_types[task_type],
            DEFAULT_DVCS_POLICY,
        )

    assert mock_available_slots.called, "The available slots functions should be called"

    records = [record.message for record in caplog.records]

    # Mapping over the dict ratio/policies entries can change overall order so let's
    # check the set of records
    expected_records = set()
    for task_type in task_types:
        visit_type = task_type["visit_type"]
        queue_name = task_type["backend_name"]
        msg = (
            f"{nb_origins} available slots for visit type {visit_type} "
            f"in queue {queue_name}"
        )
        expected_records.add(msg)

    for expected_record in expected_records:
        assert expected_record in set(records)


@pytest.mark.parametrize(
    "visit_type, extras",
    [("test-hg", {}), ("test-git", {"tablesample": 0.1})],
)
def test_recurrent_visit_additional_parameters(
    swh_scheduler, mocker, visit_type, extras
):
    """Testing additional policy parameters"""

    mock_grab_next_visits = mocker.patch.object(swh_scheduler, "grab_next_visits")
    mock_grab_next_visits.return_value = []
    policy_cfg = DEFAULT_DVCS_POLICY[:]
    for policy in policy_cfg:
        policy.update(extras)
    grab_next_visits_policy_weights(swh_scheduler, visit_type, 10, policy_cfg)

    for call in mock_grab_next_visits.call_args_list:
        assert call[1].get("tablesample") == extras.get("tablesample")


@pytest.fixture
def scheduler_config(swh_scheduler_config):
    return {"scheduler": {"cls": "postgresql", **swh_scheduler_config}, "celery": {}}


def test_visit_scheduler_thread_unknown_task(
    swh_scheduler,
    scheduler_config,
):
    """Starting a thread with unknown task type reports the error"""

    unknown_visit_type = "unknown"
    command_queue = Queue()
    exc_queue = Queue()

    visit_scheduler_thread(
        scheduler_config, unknown_visit_type, command_queue, exc_queue
    )

    assert command_queue.empty() is True
    assert exc_queue.empty() is False
    assert len(exc_queue.queue) == 1
    result = exc_queue.queue.pop()
    assert result[0] == unknown_visit_type
    assert isinstance(result[1], ValueError)


def test_spawn_visit_scheduler_thread_noop(scheduler_config, visit_types, mocker):
    """Spawning and terminating threads runs smoothly"""

    threads: VisitSchedulerThreads = {}
    exc_queue = Queue()
    mock_build_app = mocker.patch("swh.scheduler.celery_backend.config.build_app")
    mock_build_app.return_value = mocker.MagicMock()

    assert len(threads) == 0
    for visit_type in visit_types:
        spawn_visit_scheduler_thread(threads, exc_queue, scheduler_config, visit_type)

    # This actually only checks the spawning and terminating logic is sound

    assert len(threads) == len(visit_types)

    actual_threads = terminate_visit_scheduler_threads(threads)
    assert not len(actual_threads)

    assert mock_build_app.called
