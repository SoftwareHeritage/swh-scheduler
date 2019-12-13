from time import sleep
from itertools import count

from celery.result import GroupResult
from celery.result import AsyncResult

import pytest

from swh.scheduler.utils import create_task_dict
from swh.scheduler.celery_backend.runner import run_ready_tasks


def test_ping(swh_app, celery_session_worker):
    res = swh_app.send_task(
        'swh.scheduler.tests.tasks.ping')
    assert res
    res.wait()
    assert res.successful()
    assert res.result == 'OK'


def test_ping_with_kw(swh_app, celery_session_worker):
    res = swh_app.send_task(
        'swh.scheduler.tests.tasks.ping', kwargs={'a': 1})
    assert res
    res.wait()
    assert res.successful()
    assert res.result == "OK (kw={'a': 1})"


def test_multiping(swh_app, celery_session_worker):
    "Test that a task that spawns subtasks (group) works"
    res = swh_app.send_task(
        'swh.scheduler.tests.tasks.multiping', kwargs={'n': 5})
    assert res

    res.wait()
    assert res.successful()

    # retrieve the GroupResult for this task and wait for all the subtasks
    # to complete
    promise_id = res.result
    assert promise_id
    promise = GroupResult.restore(promise_id, app=swh_app)
    for i in range(5):
        if promise.ready():
            break
        sleep(1)

    results = [x.get() for x in promise.results]
    assert len(results) == 5
    for i in range(5):
        assert ("OK (kw={'i': %s})" % i) in results


@pytest.mark.db
def test_scheduler_fixture(swh_app, celery_session_worker, swh_scheduler):
    "Test that the scheduler fixture works properly"
    task_type = swh_scheduler.get_task_type('swh-test-ping')

    assert task_type
    assert task_type['backend_name'] == 'swh.scheduler.tests.tasks.ping'

    swh_scheduler.create_tasks([create_task_dict(
        'swh-test-ping', 'oneshot')])

    backend_tasks = run_ready_tasks(swh_scheduler, swh_app)
    assert backend_tasks
    for task in backend_tasks:
        # Make sure the task completed
        AsyncResult(id=task['backend_id']).get()


@pytest.mark.db
def test_task_return_value(swh_app, celery_session_worker, swh_scheduler):
    task_type = swh_scheduler.get_task_type('swh-test-add')
    assert task_type
    assert task_type['backend_name'] == 'swh.scheduler.tests.tasks.add'

    swh_scheduler.create_tasks([create_task_dict(
        'swh-test-add', 'oneshot', 12, 30)])

    backend_tasks = run_ready_tasks(swh_scheduler, swh_app)
    assert len(backend_tasks) == 1
    task = backend_tasks[0]
    value = AsyncResult(id=task['backend_id']).get()
    assert value == 42


@pytest.mark.db
def test_task_exception(swh_app, celery_session_worker, swh_scheduler):
    task_type = swh_scheduler.get_task_type('swh-test-error')
    assert task_type
    assert task_type['backend_name'] == 'swh.scheduler.tests.tasks.error'

    swh_scheduler.create_tasks([create_task_dict(
        'swh-test-error', 'oneshot')])

    backend_tasks = run_ready_tasks(swh_scheduler, swh_app)
    assert len(backend_tasks) == 1

    task = backend_tasks[0]
    result = AsyncResult(id=task['backend_id'])
    with pytest.raises(NotImplementedError):
        result.get()


def test_statsd(swh_app, celery_session_worker, mocker):
    m = mocker.patch('swh.scheduler.task.Statsd._send_to_server')
    mocker.patch('swh.scheduler.task.ts', side_effect=count())
    mocker.patch('swh.core.statsd.monotonic', side_effect=count())
    res = swh_app.send_task(
        'swh.scheduler.tests.tasks.echo')
    assert res
    res.wait()
    assert res.successful()
    assert res.result == {}

    m.assert_any_call(
        'swh_task_called_count:1|c|'
        '#task:swh.scheduler.tests.tasks.echo,worker:unknown worker')
    m.assert_any_call(
        'swh_task_start_ts:0|g|'
        '#task:swh.scheduler.tests.tasks.echo,worker:unknown worker')
    m.assert_any_call(
        'swh_task_end_ts:1|g|'
        '#status:uneventful,task:swh.scheduler.tests.tasks.echo,'
        'worker:unknown worker')
    m.assert_any_call(
        'swh_task_duration_seconds:1000|ms|'
        '#task:swh.scheduler.tests.tasks.echo,worker:unknown worker')
    m.assert_any_call(
        'swh_task_success_count:1|c|'
        '#task:swh.scheduler.tests.tasks.echo,worker:unknown worker')


def test_statsd_with_status(swh_app, celery_session_worker, mocker):
    m = mocker.patch('swh.scheduler.task.Statsd._send_to_server')
    mocker.patch('swh.scheduler.task.ts', side_effect=count())
    mocker.patch('swh.core.statsd.monotonic', side_effect=count())
    res = swh_app.send_task(
        'swh.scheduler.tests.tasks.echo', kwargs={'status': 'eventful'})
    assert res
    res.wait()
    assert res.successful()
    assert res.result == {'status': 'eventful'}

    m.assert_any_call(
        'swh_task_called_count:1|c|'
        '#task:swh.scheduler.tests.tasks.echo,worker:unknown worker')
    m.assert_any_call(
        'swh_task_start_ts:0|g|'
        '#task:swh.scheduler.tests.tasks.echo,worker:unknown worker')
    m.assert_any_call(
        'swh_task_end_ts:1|g|'
        '#status:eventful,task:swh.scheduler.tests.tasks.echo,'
        'worker:unknown worker')
    m.assert_any_call(
        'swh_task_duration_seconds:1000|ms|'
        '#task:swh.scheduler.tests.tasks.echo,worker:unknown worker')
    m.assert_any_call(
        'swh_task_success_count:1|c|'
        '#task:swh.scheduler.tests.tasks.echo,worker:unknown worker')
