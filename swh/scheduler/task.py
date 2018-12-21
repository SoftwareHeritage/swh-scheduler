# Copyright (C) 2015-2017 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import celery.app.task
from celery.utils.log import get_task_logger


class SWHTask(celery.app.task.Task):
    """a schedulable task (abstract class)

    Current implementation is based on Celery. See
    http://docs.celeryproject.org/en/latest/reference/celery.app.task.html for
    how to use tasks once instantiated

    """

    _log = None

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        self.send_event('task-result-exception')

    def on_success(self, retval, task_id, args, kwargs):
        self.send_event('task-result', result=retval)

    @property
    def log(self):
        if self._log is None:
            self._log = get_task_logger(self.name)
        return self._log


class Task(SWHTask):
    """a schedulable task (abstract class)

    DEPRECATED! Please use SWHTask as base for decorated functions instead.

    Sub-classes must implement the run_task() method.

    Current implementation is based on Celery. See
    http://docs.celeryproject.org/en/latest/reference/celery.app.task.html for
    how to use tasks once instantiated

    """

    abstract = True

    def run(self, *args, **kwargs):
        """This method is called by the celery worker when a task is received.

        Should not be overridden as we need our special events to be sent for
        the reccurrent scheduler. Override run_task instead."""
        return self.run_task(*args, **kwargs)

    def run_task(self, *args, **kwargs):
        """Perform the task.

        Must return a json-serializable value as it is passed back to the task
        scheduler using a celery event.
        """
        raise NotImplementedError('tasks must implement the run_task() method')
