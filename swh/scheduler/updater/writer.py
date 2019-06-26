# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import logging
import time

from arrow import utcnow

from swh.core import utils
from swh.scheduler import get_scheduler
from swh.scheduler.utils import create_oneshot_task_dict
from swh.scheduler.updater.backend import SchedulerUpdaterBackend


class UpdaterWriter:
    """Updater writer in charge of updating the scheduler db with latest
       prioritized oneshot tasks

       In effect, this:
       - reads the events from scheduler updater's db
       - converts those events into priority oneshot tasks
       - dumps them into the scheduler db

    """

    def __init__(self, **config):
        self.config = config
        if self.config['scheduler_updater']['cls'] != 'local':
            raise ValueError(
                'The scheduler_updater can only be a cls=local for now')
        self.scheduler_updater_backend = SchedulerUpdaterBackend(
            **self.config['scheduler_updater']['args'])
        self.scheduler_backend = get_scheduler(**self.config['scheduler'])
        self.pause = self.config.get('updater_writer', {}).get('pause', 10)
        self.log = logging.getLogger(
            'swh.scheduler.updater.writer.UpdaterWriter')

    def convert_to_oneshot_task(self, event):
        """Given an event, convert it into oneshot task with priority

        Args:
            event (dict): The event to convert to task

        """
        if event['origin_type'] == 'git':
            return create_oneshot_task_dict(
                'load-git',
                event['url'],
                priority='normal')
        self.log.warning('Type %s is not supported for now, only git' % (
            event['origin_type'], ))
        return None

    def write_event_to_scheduler(self, events):
        """Write events to the scheduler and yield ids when done"""
        # convert events to oneshot tasks
        oneshot_tasks = filter(lambda e: e is not None,
                               map(self.convert_to_oneshot_task, events))
        # write event to scheduler
        self.scheduler_backend.create_tasks(list(oneshot_tasks))
        for e in events:
            yield e['url']

    def run(self):
        """First retrieve events from cache (including origin_type, cnt),
           then convert them into oneshot tasks with priority, then
           write them to the scheduler db, at last remove them from
           cache.

        """
        while True:
            timestamp = utcnow()
            events = list(self.scheduler_updater_backend.cache_read(timestamp))
            if not events:
                break
            for urls in utils.grouper(self.write_event_to_scheduler(events),
                                      n=100):
                self.scheduler_updater_backend.cache_remove(urls)
            time.sleep(self.pause)


@click.command()
@click.option('--verbose/--no-verbose', '-v', default=False,
              help='Verbose mode')
@click.pass_context
def main(ctx, verbose):
    click.echo("Deprecated! Use 'swh-scheduler updater' instead.",
               err=True)
    ctx.exit(1)


if __name__ == '__main__':
    main()
