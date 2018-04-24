# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


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
