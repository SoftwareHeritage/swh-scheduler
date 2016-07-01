# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from celery import group

from ..backend import OneShotSchedulerBackend
from .config import app as main_app


def run_ready_tasks(backend, app):
    """Run all oneshot tasks that are ready.

    Args:
        - backend: the backend abstraction that permits to list the
        actual tasks to run
        - app: The application to ask for running tasks
    """

    backend_names = {}

    cursor = backend.cursor()
    pending_tasks = backend.grab_ready_tasks(num_tasks=10000,
                                             cursor=cursor)
    # if not pending_tasks:
    #     break

    celery_tasks = []
    task_ids = []
    for task in pending_tasks:
        task_ids.append(task['id'])

        backend_name = backend_names.get(task['type'])
        if not backend_name:
            task_type = backend.get_task_type(task['type'], cursor=cursor)
            backend_names[task['type']] = task_type['backend_name']
            backend_name = task_type['backend_name']

        arguments = task['arguments']
        args = arguments['args']
        if not args:
            args = []

        kwargs = arguments['kwargs']
        if not kwargs:
            kwargs = {}

        celery_task = app.tasks[backend_name].s(*args, **kwargs)
        celery_tasks.append(celery_task)

    group(celery_tasks).delay()

    # Now clean up entries
    for task_id in task_ids:
        backend.delete_task(task_id)

if __name__ == '__main__':
    for module in main_app.conf.CELERY_IMPORTS:
        __import__(module)

    main_backend = OneShotSchedulerBackend()
    try:
        run_ready_tasks(main_backend, main_app)
    except:
        main_backend.rollback()
        raise
