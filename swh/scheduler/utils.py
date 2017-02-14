# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


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
