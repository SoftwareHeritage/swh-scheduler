# Copyright (C) 2015-2018 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import arrow
import logging
from kombu.utils.uuid import uuid

from swh.scheduler import get_scheduler, compute_nb_tasks_from


logger = logging.getLogger(__name__)
# Max batch size for tasks
MAX_NUM_TASKS = 10000


def run_ready_tasks(backend, app):
    """Run tasks that are ready

    Args:
        backend (Scheduler): backend to read tasks to schedule
        app (App): Celery application to send tasks to

    Returns:
        A list of dictionaries::

          {
            'task': the scheduler's task id,
            'backend_id': Celery's task id,
            'scheduler': arrow.utcnow()
          }

        The result can be used to block-wait for the tasks' results::

          backend_tasks = run_ready_tasks(self.scheduler, app)
          for task in backend_tasks:
              AsyncResult(id=task['backend_id']).get()

    """
    all_backend_tasks = []
    while True:
        task_types = {}
        pending_tasks = []
        for task_type in backend.get_task_types():
            task_type_name = task_type['type']
            task_types[task_type_name] = task_type
            max_queue_length = task_type['max_queue_length']
            backend_name = task_type['backend_name']
            if max_queue_length:
                try:
                    queue_length = app.get_queue_length(backend_name)
                except ValueError:
                    queue_length = None

                if queue_length is None:
                    # Running without RabbitMQ (probably a test env).
                    num_tasks = MAX_NUM_TASKS
                else:
                    num_tasks = min(max_queue_length - queue_length,
                                    MAX_NUM_TASKS)
            else:
                num_tasks = MAX_NUM_TASKS
            if num_tasks > 0:
                num_tasks, num_tasks_priority = compute_nb_tasks_from(
                    num_tasks)

                grabbed_tasks = backend.grab_ready_tasks(
                        task_type_name,
                        num_tasks=num_tasks,
                        num_tasks_priority=num_tasks_priority)
                if grabbed_tasks:
                    pending_tasks.extend(grabbed_tasks)
                    logger.info('Grabbed %s tasks %s',
                                len(grabbed_tasks), task_type_name)

        if not pending_tasks:
            return all_backend_tasks

        backend_tasks = []
        celery_tasks = []
        for task in pending_tasks:
            args = task['arguments']['args']
            kwargs = task['arguments']['kwargs']

            backend_name = task_types[task['type']]['backend_name']
            backend_id = uuid()
            celery_tasks.append((backend_name, backend_id, args, kwargs))
            data = {
                'task': task['id'],
                'backend_id': backend_id,
                'scheduled': arrow.utcnow(),
            }

            backend_tasks.append(data)
        logger.debug('Sent %s celery tasks', len(backend_tasks))

        backend.mass_schedule_task_runs(backend_tasks)
        for backend_name, backend_id, args, kwargs in celery_tasks:
            app.send_task(
                backend_name, task_id=backend_id, args=args, kwargs=kwargs,
            )

        all_backend_tasks.extend(backend_tasks)


def main():
    from .config import app as main_app
    for module in main_app.conf.CELERY_IMPORTS:
        __import__(module)

    main_backend = get_scheduler('local')
    try:
        run_ready_tasks(main_backend, main_app)
    except Exception:
        main_backend.rollback()
        raise


if __name__ == '__main__':
    main()
