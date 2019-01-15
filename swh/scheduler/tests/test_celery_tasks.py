from time import sleep
from celery.result import GroupResult
from celery.result import AsyncResult

from swh.scheduler.utils import create_task_dict
from swh.scheduler.celery_backend.runner import run_ready_tasks


def test_ping(swh_app, celery_session_worker):
    res = swh_app.send_task(
        'swh.scheduler.tests.tasks.ping')
    assert res
    res.wait()
    assert res.successful()
    assert res.result == 'OK'


def test_multiping(swh_app, celery_session_worker):
    "Test that a task that spawns subtasks (group) works"
    res = swh_app.send_task(
        'swh.scheduler.tests.tasks.multiping', n=5)
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
    for i in range(5):
        assert ("OK (kw={'i': %s})" % i) in results


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
