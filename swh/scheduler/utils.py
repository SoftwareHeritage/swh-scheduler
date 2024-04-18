# Copyright (C) 2017-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from datetime import datetime, timezone
from typing import List, Optional

from swh.scheduler.interface import SchedulerInterface
from swh.scheduler.model import (
    ListedOrigin,
    Lister,
    Task,
    TaskArguments,
    TaskPolicy,
    TaskPriority,
)


def utcnow():
    return datetime.now(tz=timezone.utc)


def get_task(task_name):
    """Retrieve task object in our application instance by its fully
    qualified python name.

    Args:
        task_name (str): task's name (e.g
                         swh.loader.git.tasks.LoadDiskGitRepository)

    Returns:
        Instance of task

    """
    from swh.scheduler.celery_backend.config import app

    for module in app.conf.CELERY_IMPORTS:
        __import__(module)
    return app.tasks[task_name]


def create_task(
    type: str,
    policy: TaskPolicy,
    *args,
    next_run: Optional[datetime] = None,
    priority: Optional[TaskPriority] = None,
    retries_left: Optional[int] = None,
    **kwargs,
) -> Task:
    """Create a task with type and policy, scheduled for as soon as
       possible.

    Args:
        type: Type of oneshot task as per swh-scheduler's db
                    table task_type's column (Ex: load-git,
                    check-deposit)
        policy: oneshot or recurring policy
        next_run: optional date and time from which the task can be executed,
            use current time otherwise

    Returns:
        Expected dictionary for the one-shot task scheduling api
        (swh.scheduler.backend.create_tasks)

    """
    task = Task(
        policy=policy,
        type=type,
        next_run=next_run or utcnow(),
        arguments=TaskArguments(
            args=list(args) if args else [],
            kwargs=kwargs if kwargs else {},
        ),
        priority=priority,
        retries_left=retries_left,
    )
    return task


def create_origin_task(origin: ListedOrigin, lister: Lister) -> Task:
    if origin.lister_id != lister.id:
        raise ValueError(
            "origin.lister_id and lister.id differ", origin.lister_id, lister.id
        )
    return Task(
        type=f"load-{origin.visit_type}",
        arguments=TaskArguments(
            args=[],
            kwargs={
                "url": origin.url,
                "lister_name": lister.name,
                "lister_instance_name": lister.instance_name or None,
                **origin.extra_loader_arguments,
            },
        ),
        next_run=utcnow(),
    )


def create_origin_tasks(
    origins: List[ListedOrigin], scheduler: SchedulerInterface
) -> List[Task]:
    """Returns a task dict for each origin, in the same order."""

    lister_ids = {o.lister_id for o in origins}
    listers = {
        lister.id: lister
        for lister in scheduler.get_listers_by_id(list(map(str, lister_ids)))
    }

    missing_lister_ids = lister_ids - set(listers)
    assert not missing_lister_ids, f"Missing listers: {missing_lister_ids}"

    return [create_origin_task(o, listers[o.lister_id]) for o in origins]


def create_oneshot_task(
    type: str, *args, next_run: Optional[datetime] = None, **kwargs
) -> Task:
    """Create a oneshot task scheduled for as soon as possible.

    Args:
        type: Type of oneshot task as per swh-scheduler's db
            table task_type's column (Ex: load-git, check-deposit)
        next_run: optional date and time from which the task can be executed,
            use current time otherwise

    Returns:
        Expected dictionary for the one-shot task scheduling api
        (:func:`swh.scheduler.backend.create_tasks`)

    """
    return create_task(type, "oneshot", *args, next_run=next_run, **kwargs)
