# Copyright (C) 2015-2021 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from typing import Dict, List, Tuple

from kombu.utils.uuid import uuid

from swh.core.statsd import statsd
from swh.scheduler import get_scheduler
from swh.scheduler.celery_backend.config import get_available_slots
from swh.scheduler.interface import SchedulerInterface
from swh.scheduler.utils import utcnow

logger = logging.getLogger(__name__)

# Max batch size for tasks
MAX_NUM_TASKS = 10000


def run_ready_tasks(
    backend: SchedulerInterface,
    app,
    task_types: List[Dict] = [],
    with_priority: bool = False,
) -> List[Dict]:
    """Schedule tasks ready to be scheduled.

    This lookups any tasks per task type and mass schedules those accordingly (send
    messages to rabbitmq and mark as scheduled equivalent tasks in the scheduler
    backend).

    If tasks (per task type) with priority exist, they will get redirected to dedicated
    high priority queue (standard queue name prefixed with `save_code_now:`).

    Args:
        backend: scheduler backend to interact with (read/update tasks)
        app (App): Celery application to send tasks to
        task_types: The list of task types dict to iterate over. By default, empty.
          When empty, the full list of task types referenced in the scheduler will be
          used.
        with_priority: If True, only tasks with priority set will be fetched and
          scheduled. By default, False.

    Returns:
        A list of dictionaries::

          {
            'task': the scheduler's task id,
            'backend_id': Celery's task id,
            'scheduler': utcnow()
          }

        The result can be used to block-wait for the tasks' results::

          backend_tasks = run_ready_tasks(self.scheduler, app)
          for task in backend_tasks:
              AsyncResult(id=task['backend_id']).get()

    """
    all_backend_tasks: List[Dict] = []
    while True:
        if not task_types:
            task_types = backend.get_task_types()
        task_types_d = {}
        pending_tasks = []
        for task_type in task_types:
            task_type_name = task_type["type"]
            task_types_d[task_type_name] = task_type
            max_queue_length = task_type["max_queue_length"]
            if max_queue_length is None:
                max_queue_length = 0
            backend_name = task_type["backend_name"]

            if with_priority:
                # grab max_queue_length (or 10) potential tasks with any priority for
                # the same type (limit the result to avoid too long running queries)
                grabbed_priority_tasks = backend.grab_ready_priority_tasks(
                    task_type_name, num_tasks=max_queue_length or 10
                )
                if grabbed_priority_tasks:
                    pending_tasks.extend(grabbed_priority_tasks)
                    logger.info(
                        "Grabbed %s tasks %s (priority)",
                        len(grabbed_priority_tasks),
                        task_type_name,
                    )
                    statsd.increment(
                        "swh_scheduler_runner_scheduled_task_total",
                        len(grabbed_priority_tasks),
                        tags={"task_type": task_type_name},
                    )
            else:
                num_tasks = get_available_slots(app, backend_name, max_queue_length)
                # only pull tasks if the buffer is at least 1/5th empty (= 80%
                # full), to help postgresql use properly indexed queries.
                if num_tasks > min(MAX_NUM_TASKS, max_queue_length) // 5:
                    # Only grab num_tasks tasks with no priority
                    grabbed_tasks = backend.grab_ready_tasks(
                        task_type_name, num_tasks=num_tasks
                    )
                    if grabbed_tasks:
                        pending_tasks.extend(grabbed_tasks)
                        logger.info(
                            "Grabbed %s tasks %s", len(grabbed_tasks), task_type_name
                        )
                        statsd.increment(
                            "swh_scheduler_runner_scheduled_task_total",
                            len(grabbed_tasks),
                            tags={"task_type": task_type_name},
                        )

        if not pending_tasks:
            return all_backend_tasks

        backend_tasks = []
        celery_tasks: List[Tuple[bool, str, str, List, Dict]] = []
        for task in pending_tasks:
            args = task["arguments"]["args"]
            kwargs = task["arguments"]["kwargs"]

            backend_name = task_types_d[task["type"]]["backend_name"]
            backend_id = uuid()
            celery_tasks.append(
                (
                    task.get("priority") is not None,
                    backend_name,
                    backend_id,
                    args,
                    kwargs,
                )
            )
            data = {
                "task": task["id"],
                "backend_id": backend_id,
                "scheduled": utcnow(),
            }

            backend_tasks.append(data)
        logger.debug("Sent %s celery tasks", len(backend_tasks))

        backend.mass_schedule_task_runs(backend_tasks)
        for with_priority, backend_name, backend_id, args, kwargs in celery_tasks:
            kw = dict(task_id=backend_id, args=args, kwargs=kwargs,)
            if with_priority:
                kw["queue"] = f"save_code_now:{backend_name}"
            app.send_task(backend_name, **kw)

        all_backend_tasks.extend(backend_tasks)


def main():
    from .config import app as main_app

    for module in main_app.conf.CELERY_IMPORTS:
        __import__(module)

    main_backend = get_scheduler("local")
    try:
        run_ready_tasks(main_backend, main_app)
    except Exception:
        main_backend.rollback()
        raise


if __name__ == "__main__":
    main()
