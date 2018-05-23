# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import logging
import time

from arrow import utcnow

from swh.core.config import SWHConfig
from swh.core import utils
from swh.scheduler import get_scheduler
from swh.scheduler.utils import create_oneshot_task_dict
from swh.scheduler.updater.backend import SchedulerUpdaterBackend


class UpdaterWriter(SWHConfig):
    """Updater writer in charge of updating the scheduler db with latest
       prioritized oneshot tasks

       In effect, this:
       - reads the events from scheduler updater's db
       - converts those events into priority oneshot tasks
       - dumps them into the scheduler db

    """
    CONFIG_BASE_FILENAME = 'backend/scheduler-updater-writer'
    DEFAULT_CONFIG = {
        # access to the scheduler backend
        'scheduler': ('dict', {
            'cls': 'local',
            'args': {
                'scheduling_db': 'dbname=softwareheritage-scheduler-dev',
            },
        }),
        # access to the scheduler updater cache
        'scheduler_updater': ('dict', {
            'scheduling_updater_db': 'dbname=softwareheritage-scheduler-updater-dev',  # noqa
            'cache_read_limit': 1000,
        }),
        # waiting time between db read when no more data exists
        'pause': ('int', 10),
        # verbose or not
        'verbose': ('bool', False),
    }

    def __init__(self, **config):
        if config:
            self.config = config
        else:
            self.config = self.parse_config_file()

        self.scheduler_updater_backend = SchedulerUpdaterBackend(
            **self.config['scheduler_updater'])
        self.scheduler_backend = get_scheduler(**self.config['scheduler'])
        self.pause = self.config['pause']
        self.log = logging.getLogger(
            'swh.scheduler.updater.writer.UpdaterWriter')
        self.log.setLevel(
            logging.DEBUG if self.config['verbose'] else logging.INFO)

    def convert_to_oneshot_task(self, event):
        """Given an event, convert it into oneshot task with priority

        Args:
            event (dict): The event to convert to task

        """
        if event['origin_type'] == 'git':
            return create_oneshot_task_dict(
                'origin-update-git',
                event['url'],
                priority='normal')
        self.log.warn('Type %s is not supported for now, only git' % (
            event['type'], ))
        return None

    def write_event_to_scheduler(self, events):
        """Write events to the scheduler and yield ids when done"""
        for event in events:
            # convert event to oneshot task
            oneshot_task = self.convert_to_oneshot_task(event)
            if not oneshot_task:
                continue

            # write event to scheduler
            # FIXME: deal with this in batch
            r = self.scheduler_backend.create_tasks([oneshot_task])
            if r:
                yield event['url']

    def run(self):
        """First retrieve events from cache (including origin_type, cnt),
           then convert them into oneshot tasks with priority, then
           write them to the scheduler db, at last remove them from
           cache.

        """
        while True:
            timestamp = utcnow()
            events = self.scheduler_updater_backend.cache_read(timestamp)
            if not events:
                time.sleep(self.pause)
                continue

            for urls in utils.grouper(self.write_event_to_scheduler(events),
                                      n=100):
                self.scheduler_updater_backend.cache_remove(urls)


@click.command()
@click.option('--verbose/--no-verbose', '-v', default=False,
              help='Verbose mode')
def main(verbose):
    log = logging.getLogger('swh.scheduler.updater.writer')
    log.addHandler(logging.StreamHandler())
    _loglevel = logging.DEBUG if verbose else logging.INFO
    log.setLevel(_loglevel)

    UpdaterWriter().run()


if __name__ == '__main__':
    main()
