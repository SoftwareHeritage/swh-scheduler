# Copyright (C) 2021-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Agents using the "old" task-based scheduler."""

import logging
from typing import Dict, Generator, Iterator

from simpy import Event

from swh.scheduler.model import TaskRun, TaskRunStatus

from .common import Environment, Queue, Task, TaskEvent

logger = logging.getLogger(__name__)


def scheduler_runner_process(
    env: Environment,
    task_queues: Dict[str, Queue],
    min_batch_size: int,
) -> Iterator[Event]:
    """Simulate a scheduler runner process. This schedules tasks from the database
    according to the scheduling policy, that means filling the task_queues first, then
    update their status in the scheduler backend.

    """

    while True:
        for visit_type, queue in task_queues.items():
            remaining = queue.slots_remaining()
            if remaining < min_batch_size:
                continue
            next_tasks = env.scheduler.peek_ready_tasks(
                f"load-{visit_type}", num_tasks=remaining, timestamp=env.time
            )
            logger.debug(
                "%s runner: running %s %s tasks",
                env.time,
                visit_type,
                len(next_tasks),
            )

            sim_tasks = [
                Task(visit_type=visit_type, origin=task.arguments.kwargs["url"])
                for task in next_tasks
            ]

            # Send to the queuing messaging system first
            for sim_task in sim_tasks:
                yield queue.put(sim_task)

            # Then mark the sent tasks as scheduled
            env.scheduler.mass_schedule_task_runs(
                [
                    TaskRun(
                        task=task.id,
                        scheduled=env.time,
                        backend_id=str(sim_task.backend_id),
                    )
                    for task, sim_task in zip(next_tasks, sim_tasks)
                ]
            )

        yield env.timeout(10.0)


def scheduler_listener_process(
    env: Environment, status_queue: Queue
) -> Generator[Event, TaskEvent, None]:
    """Scheduler listener. In the real world this would listen to celery
    events, but we listen to the status_queue and simulate celery events from
    that."""
    while True:
        event = yield status_queue.get()
        if event.status.status == "ongoing":
            env.scheduler.start_task_run(str(event.task.backend_id), timestamp=env.time)
        else:
            status: TaskRunStatus = "failed"
            if event.status.status == "full":
                status = "eventful" if event.eventful else "uneventful"

            env.scheduler.end_task_run(
                str(event.task.backend_id), status=status, timestamp=env.time
            )
