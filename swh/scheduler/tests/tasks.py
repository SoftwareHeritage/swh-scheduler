# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from celery import group


def register_test_tasks(app):
    """Register test tasks for the specific app passed as parameter.

    In the test context, app is the swh_app and not the runtime one.

    Args:
        app: Celery app. Expects the tests application
            (swh.scheduler.tests.conftest.swh_app)

    """
    @app.task(name='swh.scheduler.tests.tasks.ping', bind=True)
    def ping(self, **kw):
        # check this is a SWHTask
        assert hasattr(self, 'log')
        assert not hasattr(self, 'run_task')
        assert 'SWHTask' in [x.__name__ for x in self.__class__.__mro__]
        self.log.debug(self.name)
        if kw:
            return 'OK (kw=%s)' % kw
        return 'OK'

    @app.task(name='swh.scheduler.tests.tasks.multiping', bind=True)
    def multiping(self, n=10):
        promise = group(ping.s(i=i) for i in range(n))()
        self.log.debug('%s OK (spawned %s subtasks)' % (self.name, n))
        promise.save()
        return promise.id

    @app.task(name='swh.scheduler.tests.tasks.error')
    def not_implemented():
        raise NotImplementedError('Nope')

    @app.task(name='swh.scheduler.tests.tasks.add')
    def add(x, y):
        return x + y
