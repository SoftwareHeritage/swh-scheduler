# Copyright (C) 2015-2025 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""This is the first scheduler runner. It is in charge of scheduling "oneshot" tasks
(e.g save code now, indexer, vault, deposit, ...). To do this, it reads tasks ouf of the
scheduler backend and pushes those to their associated rabbitmq queues.

The scheduler listener :mod:`swh.scheduler.celery_backend.pika_listener` is the module
in charge of finalizing the task results.

"""

import logging
from typing import Dict, List, Tuple

from deprecated import deprecated
from kombu.utils.uuid import uuid

from swh.core.statsd import statsd
from swh.scheduler import get_scheduler
from swh.scheduler.celery_backend.config import get_available_slots
from swh.scheduler.interface import SchedulerInterface
from swh.scheduler.model import TaskRun, TaskType
from swh.scheduler.utils import utcnow

logger = logging.getLogger(__name__)

# Max batch size for tasks
MAX_NUM_TASKS = 10000


def write_to_backends(
    backend: SchedulerInterface, app, backend_tasks: List, celery_tasks: List
):
    """Utility function to unify the writing to rabbitmq and the scheduler backends in a
    consistent way (towards atomicity).

    Messages are first sent to rabbitmq then postgresql.

    In the nominal case where all writes are ok, that changes nothing vs the previous
    implementation (postgresql first then rabbitmq).

    In degraded performance though, that's supposedly better.

    1. If we cannot write to rabbitmq, then we won't write to postgresql either, that
    function will raise and stop.

    2. If we can write to rabbitmq first, then the messages will be consumed
    independently from this. And then, if we cannot write to postgresql (for some
    reason), then we just lose the information we sent the task already. This means the
    same task will be rescheduled and we'll have a go at it again. As those kind of
    tasks are supposed to be idempotent, that should not a major issue for their
    upstream.

    Also, those tasks are mostly listers now and they have a state management of their
    own, so that should definitely mostly noops (if the ingestion from the previous run
    went fine). Edge cases scenario like down site will behave as before.

    """
    for with_priority, backend_name, backend_id, args, kwargs in celery_tasks:
        kw = dict(
            task_id=backend_id,
            args=args,
            kwargs=kwargs,
        )
        if with_priority:
            kw["queue"] = f"save_code_now:{backend_name}"
        app.send_task(backend_name, **kw)
    logger.debug("Sent %s celery tasks", len(backend_tasks))
    backend.mass_schedule_task_runs(backend_tasks)
    logger.debug("Written %s celery tasks", len(backend_tasks))


def run_ready_tasks(
    backend: SchedulerInterface,
    app,
    task_types: List[TaskType] = [],
    task_type_patterns: List[str] = [],
    with_priority: bool = False,
) -> List[TaskRun]:
    """Schedule tasks ready to be scheduled.

    This lookups any tasks per task type and mass schedules those accordingly (send
    messages to rabbitmq and mark as scheduled equivalent tasks in the scheduler
    backend).

    If tasks (per task type) with priority exist, they will get redirected to dedicated
    high priority queue (standard queue name prefixed with `save_code_now:`).

    Args:
        backend: scheduler backend to interact with (read/update tasks)
        app (App): Celery application to send tasks to
        task_types: The list of task types dict to iterate over. When empty (the
          default), the full list of task types referenced in the scheduler backend will
          be used.
        task_type_patterns: List of task type patterns allowed to be scheduled. If task
          type does not match, they are skipped from the scheduling. When empty (the
          default), there is no filtering on the task types.
        with_priority: If True, only tasks with priority set will be fetched and
          scheduled. By default, False.

    Returns:
        A list of TaskRun scheduled

    """
    all_backend_tasks: List[TaskRun] = []
    while True:
        task_types_to_schedule = []
        # Initialize task types from scheduler backend when none is provided
        if not task_types:
            task_types = backend.get_task_types()

        # Let's filter task types by patterns if any is provided
        if task_type_patterns:
            for task_type in task_types:
                task_type_name = task_type.type
                for pattern in task_type_patterns:
                    if task_type_name.startswith(pattern):
                        task_types_to_schedule.append(task_type)
        else:
            # Otherwise, no filter, let's keep all task types
            task_types_to_schedule = task_types

        # Finally, let's schedule the tasks matching the task types that are ready
        task_types_d = {}
        pending_tasks = []
        for task_type in task_types_to_schedule:
            task_type_name = task_type.type
            task_types_d[task_type_name] = task_type
            max_queue_length = task_type.max_queue_length
            if max_queue_length is None:
                max_queue_length = 0
            backend_name = task_type.backend_name

            if with_priority:
                # grab max_queue_length (or 10) potential tasks with any priority for
                # the same type (limit the result to avoid too long running queries)
                grabbed_priority_tasks = backend.peek_ready_priority_tasks(
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
                    grabbed_tasks = backend.peek_ready_tasks(
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
            args = task.arguments.args
            kwargs = task.arguments.kwargs

            backend_name = task_types_d[task.type].backend_name
            backend_id = uuid()
            celery_tasks.append(
                (
                    task.priority is not None,
                    backend_name,
                    backend_id,
                    args,
                    kwargs,
                )
            )
            data = TaskRun(
                task=task.id,
                backend_id=backend_id,
                scheduled=utcnow(),
            )

            backend_tasks.append(data)
        write_to_backends(backend, app, backend_tasks, celery_tasks)
        all_backend_tasks.extend(backend_tasks)


@deprecated(version="0.18", reason="Use `swh scheduler start-runner` instead")
def main():
    from .config import app as main_app

    for module in main_app.conf.CELERY_IMPORTS:
        __import__(module)

    main_backend = get_scheduler("postgresql")
    try:
        run_ready_tasks(main_backend, main_app)
    except Exception:
        main_backend.rollback()
        raise


if __name__ == "__main__":
    main()
