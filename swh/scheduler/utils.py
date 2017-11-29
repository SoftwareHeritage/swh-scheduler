# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import datetime


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


def create_oneshot_task_dict(type, *args, **kwargs):
    """Create a oneshot task scheduled for as soon as possible.

    Args:
        type (str): Type of oneshot task as per swh-scheduler's db
                    table task_type's column (Ex: origin-update-git,
                    swh-deposit-archive-checks)

    Returns:
        Expected dictionary for the one-shot task scheduling api
        (swh.scheduler.backend.create_tasks)

    """
    return {
        'policy': 'oneshot',
        'type': type,
        'next_run': datetime.datetime.now(tz=datetime.timezone.utc),
        'arguments': {
            'args': args if args else [],
            'kwargs': kwargs if kwargs else {},
        }
    }
