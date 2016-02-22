# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from ..backend import SchedulerBackend
from .config import app as main_app


def run_ready_tasks(backend, app):
    """Run all tasks that are ready"""

    backend_names = {}

    while True:
        cursor = backend.cursor()
        pending_tasks = backend.grab_ready_tasks(num_tasks=100, cursor=cursor)
        if not pending_tasks:
            break

        for task in pending_tasks:
            backend_name = backend_names.get(task['type'])
            if not backend_name:
                task_type = backend.get_task_type(task['type'], cursor=cursor)
                backend_names[task['type']] = task_type['backend_name']
                backend_name = task_type['backend_name']

            args = task['arguments']['args']
            kwargs = task['arguments']['kwargs']

            celery_task = app.tasks[backend_name].delay(*args, **kwargs)
            backend.schedule_task_run(task['id'], celery_task.id,
                                      cursor=cursor)

        backend.commit()

if __name__ == '__main__':
    for module in main_app.conf.CELERY_IMPORTS:
        __import__(module)

    main_backend = SchedulerBackend()
    try:
        run_ready_tasks(main_backend, main_app)
    except:
        main_backend.rollback()
        raise
