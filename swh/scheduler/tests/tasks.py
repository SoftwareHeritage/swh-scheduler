from celery import group, current_app as app


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
