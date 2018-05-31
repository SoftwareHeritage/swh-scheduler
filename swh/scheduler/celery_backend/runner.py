# Copyright (C) 2015-2018 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import time

import arrow
from celery import group

from swh.scheduler import get_scheduler, compute_nb_tasks_from
from .config import app as main_app


# Max batch size for tasks
MAX_NUM_TASKS = 10000


def run_ready_tasks(backend, app):
    """Run tasks that are ready

    Args
        backend (Scheduler): backend to read tasks to schedule
        app (App): Celery application to send tasks to

    """
    while True:
        throttled = False
        cursor = backend.cursor()
        task_types = {}
        pending_tasks = []
        for task_type in backend.get_task_types(cursor=cursor):
            task_type_name = task_type['type']
            task_types[task_type_name] = task_type
            max_queue_length = task_type['max_queue_length']
            if max_queue_length:
                backend_name = task_type['backend_name']
                queue_name = app.tasks[backend_name].task_queue
                queue_length = app.get_queue_length(queue_name)
                num_tasks = min(max_queue_length - queue_length,
                                MAX_NUM_TASKS)
            else:
                num_tasks = MAX_NUM_TASKS
            if num_tasks > 0:
                num_tasks, num_tasks_priority = compute_nb_tasks_from(
                    num_tasks)

                pending_tasks.extend(
                    backend.grab_ready_tasks(
                        task_type_name,
                        num_tasks=num_tasks,
                        num_tasks_priority=num_tasks_priority,
                        cursor=cursor))

        if not pending_tasks:
            break

        celery_tasks = []
        for task in pending_tasks:
            args = task['arguments']['args']
            kwargs = task['arguments']['kwargs']

            celery_task = app.tasks[
                task_types[task['type']]['backend_name']
            ].s(*args, **kwargs)

            celery_tasks.append(celery_task)

        group_result = group(celery_tasks).delay()

        backend_tasks = [{
            'task': task['id'],
            'backend_id': group_result.results[i].id,
            'scheduled': arrow.utcnow(),
        } for i, task in enumerate(pending_tasks)]

        backend.mass_schedule_task_runs(backend_tasks, cursor=cursor)

        backend.commit()

        if throttled:
            time.sleep(10)


if __name__ == '__main__':
    for module in main_app.conf.CELERY_IMPORTS:
        __import__(module)

    main_backend = get_scheduler('local')
    try:
        run_ready_tasks(main_backend, main_app)
    except Exception:
        main_backend.rollback()
        raise
