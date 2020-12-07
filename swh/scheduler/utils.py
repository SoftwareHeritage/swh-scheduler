# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from datetime import datetime, timezone


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
