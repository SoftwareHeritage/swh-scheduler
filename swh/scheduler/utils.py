# Copyright (C) 2017-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from datetime import datetime, timezone
from typing import Any, Dict, List

from .interface import SchedulerInterface
from .model import ListedOrigin, Lister


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


def create_task_dict(type, policy, *args, **kwargs):
    """Create a task with type and policy, scheduled for as soon as
       possible.

    Args:
        type (str): Type of oneshot task as per swh-scheduler's db
                    table task_type's column (Ex: load-git,
                    check-deposit)
        policy (str): oneshot or recurring policy

    Returns:
        Expected dictionary for the one-shot task scheduling api
        (swh.scheduler.backend.create_tasks)

    """
    task_extra = {}
    for extra_key in ["priority", "retries_left"]:
        if extra_key in kwargs:
            extra_val = kwargs.pop(extra_key)
            task_extra[extra_key] = extra_val

    task = {
        "policy": policy,
        "type": type,
        "next_run": utcnow(),
        "arguments": {
            "args": args if args else [],
            "kwargs": kwargs if kwargs else {},
        },
    }
    task.update(task_extra)
    return task


def create_origin_task_dict(origin: ListedOrigin, lister: Lister) -> Dict[str, Any]:
    if origin.lister_id != lister.id:
        raise ValueError(
            "origin.lister_id and lister.id differ", origin.lister_id, lister.id
        )
    return {
        "type": f"load-{origin.visit_type}",
        "arguments": {
            "args": [],
            "kwargs": {
                "url": origin.url,
                "lister_name": lister.name,
                "lister_instance_name": lister.instance_name or None,
                **origin.extra_loader_arguments,
            },
        },
    }


def create_origin_task_dicts(
    origins: List[ListedOrigin], scheduler: SchedulerInterface
) -> List[Dict[str, Any]]:
    """Returns a task dict for each origin, in the same order."""

    lister_ids = {o.lister_id for o in origins}
    listers = {
        lister.id: lister
        for lister in scheduler.get_listers_by_id(list(map(str, lister_ids)))
    }

    missing_lister_ids = lister_ids - set(listers)
    assert not missing_lister_ids, f"Missing listers: {missing_lister_ids}"

    return [create_origin_task_dict(o, listers[o.lister_id]) for o in origins]


def create_oneshot_task_dict(type, *args, **kwargs):
    """Create a oneshot task scheduled for as soon as possible.

    Args:
        type (str): Type of oneshot task as per swh-scheduler's db
                    table task_type's column (Ex: load-git,
                    check-deposit)

    Returns:
        Expected dictionary for the one-shot task scheduling api
        (swh.scheduler.backend.create_tasks)

    """
    return create_task_dict(type, "oneshot", *args, **kwargs)
