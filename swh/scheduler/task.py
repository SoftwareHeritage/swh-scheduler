# Copyright (C) 2015-2017 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import celery
from celery.utils.log import get_task_logger


class Task(celery.Task):
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
