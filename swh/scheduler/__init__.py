# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


# Percentage of tasks with priority to schedule
PRIORITY_SLOT = 0.6

DEFAULT_CONFIG = {
    'scheduler': ('dict', {
        'cls': 'local',
        'args': {
            'db': 'dbname=softwareheritage-scheduler-dev',
        },
    })
}
# current configuration. To be set by the config loading mechanism
CONFIG = {}


def compute_nb_tasks_from(num_tasks):
    """Compute and returns the tuple, number of tasks without priority,
       number of tasks with priority.

    Args:
        num_tasks (int):

    Returns:
        tuple number of tasks without priority (int), number of tasks with
        priority (int)

    """
    if not num_tasks:
        return None, None
    return (int((1 - PRIORITY_SLOT) * num_tasks),
            int(PRIORITY_SLOT * num_tasks))


def get_scheduler(cls, args={}):
    """
    Get a scheduler object of class `scheduler_class` with arguments
    `scheduler_args`.

    Args:
        scheduler (dict): dictionary with keys:

        cls (str): scheduler's class, either 'local' or 'remote'
        args (dict): dictionary with keys, default to empty.

    Returns:
        an instance of swh.scheduler, either local or remote:

        local: swh.scheduler.backend.SchedulerBackend
        remote: swh.scheduler.api.client.RemoteScheduler

    Raises:
        ValueError if passed an unknown storage class.

    """

    if cls == 'remote':
        from .api.client import RemoteScheduler as SchedulerBackend
    elif cls == 'local':
        from .backend import SchedulerBackend
    else:
        raise ValueError('Unknown swh.scheduler class `%s`' % cls)

    return SchedulerBackend(**args)
