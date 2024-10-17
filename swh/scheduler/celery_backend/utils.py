# Copyright (C) 2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from datetime import timedelta
import logging
from typing import TYPE_CHECKING, Dict, Optional

if TYPE_CHECKING:
    from swh.scheduler.interface import SchedulerInterface
    from swh.scheduler.model import TaskType


logger = logging.getLogger(__name__)


def get_loader_task_type(
    scheduler: SchedulerInterface, visit_type: str
) -> Optional[TaskType]:
    "Given a visit type, return its associated task type."
    return scheduler.get_task_type(f"load-{visit_type}")


def send_to_celery(
    scheduler: SchedulerInterface,
    visit_type_to_queue: Dict[str, str],
    enabled: bool = True,
    lister_name: Optional[str] = None,
    lister_instance_name: Optional[str] = None,
    policy: str = "oldest_scheduled_first",
    tablesample: Optional[float] = None,
    absolute_cooldown: Optional[timedelta] = None,
    scheduled_cooldown: Optional[timedelta] = None,
    failed_cooldown: Optional[timedelta] = None,
    not_found_cooldown: Optional[timedelta] = None,
):
    """Utility function to read tasks from the scheduler and send those directly to
    celery.

    Args:
        visit_type_to_queue: Optional mapping of visit/loader type (e.g git, svn, ...)
          to queue to send task to.
        enabled: Determine whether we want to list enabled or disabled origins. As
          default, we want reasonably enabled origins. For some edge case, we might
          want the others.
        lister_name: Determine the list of origins listed from the lister with name
        lister_instance_name: Determine the list of origins listed from the lister
          with instance name
        policy: the scheduling policy used to select which visits to schedule
        tablesample: the percentage of the table on which we run the query
          (None: no sampling)
        absolute_cooldown: the minimal interval between two visits of the same origin
        scheduled_cooldown: the minimal interval before which we can schedule
          the same origin again if it's not been visited
        failed_cooldown: the minimal interval before which we can reschedule a
          failed origin
        not_found_cooldown: the minimal interval before which we can reschedule a
          not_found origin

    Returns:
        The number of tasks sent to celery

    """

    from kombu.utils.uuid import uuid

    from swh.scheduler.celery_backend.config import app, get_available_slots

    from ..utils import create_origin_tasks

    nb_scheduled_tasks = 0
    for visit_type_name, queue_name in visit_type_to_queue.items():
        task_type = get_loader_task_type(scheduler, visit_type_name)
        assert task_type is not None
        task_name = task_type.backend_name
        num_tasks = get_available_slots(app, queue_name, task_type.max_queue_length)

        logger.info("%s slots available in celery queue %s", num_tasks, queue_name)

        origins = scheduler.grab_next_visits(
            visit_type_name,
            num_tasks,
            policy=policy,
            tablesample=tablesample,
            enabled=enabled,
            lister_name=lister_name,
            lister_instance_name=lister_instance_name,
            absolute_cooldown=absolute_cooldown,
            scheduled_cooldown=scheduled_cooldown,
            failed_cooldown=failed_cooldown,
            not_found_cooldown=not_found_cooldown,
        )

        logger.info(
            "%s visits of type %s to send to celery", len(origins), visit_type_name
        )
        for task in create_origin_tasks(origins, scheduler):
            app.send_task(
                task_name,
                task_id=uuid(),
                args=task.arguments.args,
                kwargs=task.arguments.kwargs,
                queue=queue_name,
            )
            nb_scheduled_tasks += 1

    return nb_scheduled_tasks
