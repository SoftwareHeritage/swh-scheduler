# Copyright (C) 2015-2017 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import celery.app.task
from celery.utils.log import get_task_logger

from celery.app.task import TaskType
if TaskType is type:
    # From Celery 3.1.25, celery/celery/app/task.py
    # Copyright (c) 2015 Ask Solem & contributors.  All rights reserved.
    # Copyright (c) 2012-2014 GoPivotal, Inc.  All rights reserved.
    # Copyright (c) 2009, 2010, 2011, 2012 Ask Solem, and individual
    # contributors. All rights reserved.
    #
    # Redistribution and use in source and binary forms, with or without
    # modification, are permitted provided that the following conditions are
    # met:
    #     * Redistributions of source code must retain the above copyright
    #       notice, this list of conditions and the following disclaimer.
    #     * Redistributions in binary form must reproduce the above copyright
    #       notice, this list of conditions and the following disclaimer in the
    #       documentation and/or other materials provided with the
    #       distribution.
    #     * Neither the name of Ask Solem, nor the names of its contributors
    #       may be used to endorse or promote products derived from this
    #       software without specific prior written permission.
    #
    # THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
    # IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
    # THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
    # PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL Ask Solem OR CONTRIBUTORS BE
    # LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
    # CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
    # SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
    # INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
    # CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
    # ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
    # THE POSSIBILITY OF SUCH DAMAGE.

    from celery import current_app
    from celery.local import Proxy
    from celery.utils import gen_task_name

    class _CompatShared(object):

        def __init__(self, name, cons):
            self.name = name
            self.cons = cons

        def __hash__(self):
            return hash(self.name)

        def __repr__(self):
            return '<OldTask: %r>' % (self.name, )

        def __call__(self, app):
            return self.cons(app)

    class TaskType(type):
        """Meta class for tasks.

        Automatically registers the task in the task registry (except if the
        :attr:`Task.abstract`` attribute is set).

        If no :attr:`Task.name` attribute is provided, then the name is
        generated from the module and class name.

        """
        _creation_count = {}  # used by old non-abstract task classes

        def __new__(cls, name, bases, attrs):
            new = super(TaskType, cls).__new__
            task_module = attrs.get('__module__') or '__main__'

            # - Abstract class: abstract attribute should not be inherited.
            abstract = attrs.pop('abstract', None)
            if abstract or not attrs.get('autoregister', True):
                return new(cls, name, bases, attrs)

            # The 'app' attribute is now a property, with the real app located
            # in the '_app' attribute. Previously this was a regular attribute,
            # so we should support classes defining it.
            app = attrs.pop('_app', None) or attrs.pop('app', None)

            # Attempt to inherit app from one the bases
            if not isinstance(app, Proxy) and app is None:
                for base in bases:
                    if getattr(base, '_app', None):
                        app = base._app
                        break
                else:
                    app = current_app._get_current_object()
                    attrs['_app'] = app

            # - Automatically generate missing/empty name.
            task_name = attrs.get('name')
            if not task_name:
                attrs['name'] = task_name = gen_task_name(app, name,
                                                          task_module)

            if not attrs.get('_decorated'):
                # non decorated tasks must also be shared in case
                # an app is created multiple times due to modules
                # imported under multiple names.
                # Hairy stuff,  here to be compatible with 2.x.
                # People should not use non-abstract task classes anymore,
                # use the task decorator.
                from celery._state import connect_on_app_finalize
                unique_name = '.'.join([task_module, name])
                if unique_name not in cls._creation_count:
                    # the creation count is used as a safety
                    # so that the same task is not added recursively
                    # to the set of constructors.
                    cls._creation_count[unique_name] = 1
                    connect_on_app_finalize(_CompatShared(
                        unique_name,
                        lambda app: TaskType.__new__(cls, name, bases,
                                                     dict(attrs, _app=app)),
                    ))

            # - Create and register class.
            # Because of the way import happens (recursively)
            # we may or may not be the first time the task tries to register
            # with the framework.  There should only be one class for each task
            # name, so we always return the registered version.
            tasks = app._tasks
            if task_name not in tasks:
                tasks.register(new(cls, name, bases, attrs))
            instance = tasks[task_name]
            instance.bind(app)
            return instance.__class__


class Task(celery.app.task.Task, metaclass=TaskType):
    """a schedulable task (abstract class)

    Sub-classes must implement the run() method.  Sub-classes that
    want their tasks to get routed to a non-default task queue must
    override the task_queue attribute.

    Current implementation is based on Celery. See
    http://docs.celeryproject.org/en/latest/reference/celery.app.task.html for
    how to use tasks once instantiated

    """

    abstract = True
    task_queue = 'celery'

    def run(self, *args, **kwargs):
        """This method is called by the celery worker when a task is received.

        Should not be overridden as we need our special events to be sent for
        the reccurrent scheduler. Override run_task instead."""
        try:
            result = self.run_task(*args, **kwargs)
        except Exception as e:
            self.send_event('task-result-exception')
            raise e from None
        else:
            self.send_event('task-result', result=result)
            return result

    def run_task(self, *args, **kwargs):
        """Perform the task.

        Must return a json-serializable value as it is passed back to the task
        scheduler using a celery event.
        """
        raise NotImplementedError('tasks must implement the run_task() method')

    @property
    def log(self):
        if not hasattr(self, '__log'):
            self.__log = get_task_logger('%s.%s' %
                                         (__name__, self.__class__.__name__))
        return self.__log
