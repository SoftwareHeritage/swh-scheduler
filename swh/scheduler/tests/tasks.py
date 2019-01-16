from celery import group

from swh.scheduler.celery_backend.config import app


@app.task(name='swh.scheduler.tests.tasks.ping',
          bind=True)
def ping(self, **kw):
    # check this is a SWHTask
    assert hasattr(self, 'log')
    assert not hasattr(self, 'run_task')
    assert 'SWHTask' in [x.__name__ for x in self.__class__.__mro__]
    self.log.debug(self.name)
    if kw:
        return 'OK (kw=%s)' % kw
    return 'OK'


@app.task(name='swh.scheduler.tests.tasks.multiping',
          bind=True)
def multiping(self, n=10):
    self.log.debug(self.name)

    promise = group(ping.s(i=i) for i in range(n))()
    self.log.debug('%s OK (spawned %s subtasks)' % (self.name, n))
    promise.save()
    return promise.id


@app.task(name='swh.scheduler.tests.tasks.error',
          bind=True)
def not_implemented(self):
    self.log.debug(self.name)
    raise NotImplementedError('Nope')


@app.task(name='swh.scheduler.tests.tasks.add',
          bind=True)
def add(self, x, y):
    self.log.debug(self.name)
    return x + y
