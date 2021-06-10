# Copyright (C) 2019-2021 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Module in charge of testing the scheduler runner module"""

from itertools import count
from time import sleep

from celery.result import AsyncResult, GroupResult
from kombu import Exchange, Queue
import pytest

from swh.scheduler.celery_backend.runner import run_ready_tasks
from swh.scheduler.tests.tasks import (
    TASK_ADD,
    TASK_ECHO,
    TASK_ERROR,
    TASK_MULTIPING,
    TASK_PING,
)
from swh.scheduler.utils import create_task_dict

# Queues to subscribe. Due to the rerouting of high priority tasks, this module requires
# to declare all queues/task names
TEST_QUEUES = [
    "celery",
    TASK_ECHO,
    TASK_ERROR,
    TASK_PING,
    TASK_ADD,
    TASK_MULTIPING,
    # and the high priority queue
    f"save_code_now:{TASK_ADD}",
]


@pytest.fixture(scope="session")
def swh_scheduler_celery_app(swh_scheduler_celery_app):
    swh_scheduler_celery_app.add_defaults(
        {
            "task_queues": [
                Queue(queue, Exchange(queue), routing_key=queue)
                for queue in TEST_QUEUES
            ],
        }
    )
    return swh_scheduler_celery_app


def test_ping(swh_scheduler_celery_app, swh_scheduler_celery_worker):
    res = swh_scheduler_celery_app.send_task(TASK_PING)
    assert res
    res.wait()
    assert res.successful()
    assert res.result == "OK"


def test_ping_with_kw(swh_scheduler_celery_app, swh_scheduler_celery_worker):
    res = swh_scheduler_celery_app.send_task(TASK_PING, kwargs={"a": 1})
    assert res
    res.wait()
    assert res.successful()
    assert res.result == "OK (kw={'a': 1})"


def test_multiping(swh_scheduler_celery_app, swh_scheduler_celery_worker):
    "Test that a task that spawns subtasks (group) works"
    res = swh_scheduler_celery_app.send_task(TASK_MULTIPING, kwargs={"n": 5})
    assert res

    res.wait()
    assert res.successful()

    # retrieve the GroupResult for this task and wait for all the subtasks
    # to complete
    promise_id = res.result
    assert promise_id
    promise = GroupResult.restore(promise_id, app=swh_scheduler_celery_app)
    for i in range(5):
        if promise.ready():
            break
        sleep(1)

    results = [x.get() for x in promise.results]
    assert len(results) == 5
    for i in range(5):
        assert ("OK (kw={'i': %s})" % i) in results


def test_scheduler_fixture(
    swh_scheduler_celery_app, swh_scheduler_celery_worker, swh_scheduler
):
    "Test that the scheduler fixture works properly"
    task_type = swh_scheduler.get_task_type("swh-test-ping")

    assert task_type
    assert task_type["backend_name"] == TASK_PING

    swh_scheduler.create_tasks([create_task_dict("swh-test-ping", "oneshot")])

    backend_tasks = run_ready_tasks(swh_scheduler, swh_scheduler_celery_app)
    assert backend_tasks
    for task in backend_tasks:
        # Make sure the task completed
        AsyncResult(id=task["backend_id"]).get()


def test_run_ready_task_standard(
    swh_scheduler_celery_app, swh_scheduler_celery_worker, swh_scheduler
):
    """Ensure scheduler runner schedules tasks ready for scheduling"""
    task_type_name, backend_name = "swh-test-add", TASK_ADD
    task_type = swh_scheduler.get_task_type(task_type_name)
    assert task_type
    assert task_type["backend_name"] == backend_name

    task_inputs = [
        ("oneshot", (12, 30)),
        ("oneshot", (20, 10)),
        ("recurring", (30, 10)),
    ]

    tasks = swh_scheduler.create_tasks(
        create_task_dict(task_type_name, policy, *args)
        for (policy, args) in task_inputs
    )

    assert len(tasks) == len(task_inputs)

    task_ids = set()
    for task in tasks:
        assert task["status"] == "next_run_not_scheduled"
        assert task["priority"] is None
        task_ids.add(task["id"])

    backend_tasks = run_ready_tasks(swh_scheduler, swh_scheduler_celery_app)
    assert len(backend_tasks) == len(tasks)

    scheduled_tasks = swh_scheduler.search_tasks(task_type=task_type_name)
    assert len(scheduled_tasks) == len(tasks)
    for task in scheduled_tasks:
        assert task["status"] == "next_run_scheduled"
        assert task["id"] in task_ids

    # Ensure each task is indeed scheduled to the queue backend
    for i, (_, args) in enumerate(task_inputs):
        task = backend_tasks[i]
        value = AsyncResult(id=task["backend_id"]).get()
        assert value == sum(args)


def test_run_ready_task_with_priority(
    swh_scheduler_celery_app, swh_scheduler_celery_worker, swh_scheduler
):
    """Ensure scheduler runner schedules priority tasks ready for scheduling"""
    task_type_name, backend_name = "swh-test-add", TASK_ADD
    task_type = swh_scheduler.get_task_type(task_type_name)
    assert task_type
    assert task_type["backend_name"] == backend_name

    task_inputs = [
        ("oneshot", (10, 22), "low"),
        ("oneshot", (20, 10), "normal"),
        ("recurring", (30, 10), "high"),
    ]

    tasks = swh_scheduler.create_tasks(
        create_task_dict(task_type_name, policy, *args, priority=priority)
        for (policy, args, priority) in task_inputs
    )

    assert len(tasks) == len(task_inputs)

    task_ids = set()
    for task in tasks:
        assert task["status"] == "next_run_not_scheduled"
        assert task["priority"] is not None
        task_ids.add(task["id"])

    backend_tasks = run_ready_tasks(
        swh_scheduler, swh_scheduler_celery_app, task_types=[], with_priority=True
    )
    assert len(backend_tasks) == len(tasks)

    scheduled_tasks = swh_scheduler.search_tasks(task_type=task_type_name)
    assert len(scheduled_tasks) == len(tasks)
    for task in scheduled_tasks:
        assert task["status"] == "next_run_scheduled"
        assert task["id"] in task_ids

    # Ensure each priority task is indeed scheduled to the queue backend
    for i, (_, args, _) in enumerate(task_inputs):
        task = backend_tasks[i]
        value = AsyncResult(id=task["backend_id"]).get()
        assert value == sum(args)


def test_task_exception(
    swh_scheduler_celery_app, swh_scheduler_celery_worker, swh_scheduler
):
    task_type = swh_scheduler.get_task_type("swh-test-error")
    assert task_type
    assert task_type["backend_name"] == TASK_ERROR

    swh_scheduler.create_tasks([create_task_dict("swh-test-error", "oneshot")])

    backend_tasks = run_ready_tasks(swh_scheduler, swh_scheduler_celery_app)
    assert len(backend_tasks) == 1

    task = backend_tasks[0]
    result = AsyncResult(id=task["backend_id"])
    with pytest.raises(NotImplementedError):
        result.get()


def test_statsd(swh_scheduler_celery_app, swh_scheduler_celery_worker, mocker):
    m = mocker.patch("swh.scheduler.task.Statsd._send_to_server")
    mocker.patch("swh.scheduler.task.ts", side_effect=count())
    mocker.patch("swh.core.statsd.monotonic", side_effect=count())
    res = swh_scheduler_celery_app.send_task(TASK_ECHO)
    assert res
    res.wait()
    assert res.successful()
    assert res.result == {}

    m.assert_any_call(
        "swh_task_called_count:1|c|"
        "#task:swh.scheduler.tests.tasks.echo,worker:unknown worker"
    )
    m.assert_any_call(
        "swh_task_start_ts:0|g|"
        "#task:swh.scheduler.tests.tasks.echo,worker:unknown worker"
    )
    m.assert_any_call(
        "swh_task_end_ts:1|g|"
        "#status:uneventful,task:swh.scheduler.tests.tasks.echo,"
        "worker:unknown worker"
    )
    m.assert_any_call(
        "swh_task_duration_seconds:1000|ms|"
        "#task:swh.scheduler.tests.tasks.echo,worker:unknown worker"
    )
    m.assert_any_call(
        "swh_task_success_count:1|c|"
        "#task:swh.scheduler.tests.tasks.echo,worker:unknown worker"
    )


def test_statsd_with_status(
    swh_scheduler_celery_app, swh_scheduler_celery_worker, mocker
):
    m = mocker.patch("swh.scheduler.task.Statsd._send_to_server")
    mocker.patch("swh.scheduler.task.ts", side_effect=count())
    mocker.patch("swh.core.statsd.monotonic", side_effect=count())
    res = swh_scheduler_celery_app.send_task(TASK_ECHO, kwargs={"status": "eventful"})
    assert res
    res.wait()
    assert res.successful()
    assert res.result == {"status": "eventful"}

    m.assert_any_call(
        "swh_task_called_count:1|c|"
        "#task:swh.scheduler.tests.tasks.echo,worker:unknown worker"
    )
    m.assert_any_call(
        "swh_task_start_ts:0|g|"
        "#task:swh.scheduler.tests.tasks.echo,worker:unknown worker"
    )
    m.assert_any_call(
        "swh_task_end_ts:1|g|"
        "#status:eventful,task:swh.scheduler.tests.tasks.echo,"
        "worker:unknown worker"
    )
    m.assert_any_call(
        "swh_task_duration_seconds:1000|ms|"
        "#task:swh.scheduler.tests.tasks.echo,worker:unknown worker"
    )
    m.assert_any_call(
        "swh_task_success_count:1|c|"
        "#task:swh.scheduler.tests.tasks.echo,worker:unknown worker"
    )
