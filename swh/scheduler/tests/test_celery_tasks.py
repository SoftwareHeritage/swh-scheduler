from time import sleep
from celery.result import GroupResult


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
