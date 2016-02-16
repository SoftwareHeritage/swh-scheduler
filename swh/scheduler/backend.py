# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

import psycopg2
from psycopg2.extras import RealDictCursor

from swh.core.config import SWHConfig


class SchedulerBackend(SWHConfig):
    """
    Backend for the Software Heritage scheduling database.
    """

    CONFIG_BASE_FILENAME = 'scheduler.ini'
    DEFAULT_CONFIG = {
        'scheduling_db': ('str', 'dbname=swh-scheduler'),
    }

    def __init__(self):
        self.config = self.parse_config_file(global_config=False)

        self.db = None

        self.reconnect()

    def reconnect(self):
        if not self.db or self.db.closed:
            self.db = psycopg2.connect(self.config['scheduling_db'])

    def cursor(self):
        """Return a fresh cursor on the database, with auto-reconnection in case of
        failure"""
        cur = None

        # Get a fresh cursor and reconnect at most three times
        tries = 0
        while True:
            tries += 1
            try:
                cur = self.db.cursor(cursor_factory=RealDictCursor)
                cur.execute('select 1')
                break
            except psycopg2.OperationalError:
                if tries < 3:
                    self.reconnect()
                else:
                    raise

        return cur

    def commit(self):
        """Commit a transaction"""
        self.db.commit()

    def rollback(self):
        """Rollback a transaction"""
        self.db.rollback()

    task_type_keys = [
        'type', 'description', 'backend_name', 'default_interval',
        'min_interval', 'max_interval', 'backoff_factor',
    ]

    def _format_query(self, query, keys):
        """Format a query with the given keys"""

        query_keys = ', '.join(keys)
        placeholders = ', '.join(['%s'] * len(keys))

        return query.format(keys=query_keys, placeholders=placeholders)

    def create_task_type(self, task_type, cursor=None):
        """Create a new task type ready for scheduling.

        A task type is a dictionary with the following keys:
            type (str): an identifier for the task type
            description (str): a human-readable description of what the task
                does
            backend_name (str): the name of the task in the job-scheduling
                backend
            default_interval (datetime.timedelta): the default interval
                between two task runs
            min_interval (datetime.timedelta): the minimum interval between
                two task runs
            max_interval (datetime.timedelta): the maximum interval between
                two task runs
            backoff_factor (float): the factor by which the interval changes
                at each run
        """
        cur = cursor if cursor else self.cursor()

        query = self._format_query(
            """insert into task_type ({keys}) values ({placeholders})""",
            self.task_type_keys,
        )
        cur.execute(query, [task_type[key] for key in self.task_type_keys])

        if not cursor:
            self.commit()

    def get_task_type(self, task_type_name, cursor=None):
        """Retrieve the task type with id task_type_name"""
        cur = cursor if cursor else self.cursor()

        query = self._format_query(
            "select {keys} from task_type where type=%s",
            self.task_type_keys,
        )
        cur.execute(query, (task_type_name,))

        ret = cur.fetchone()

        if not cursor:
            self.commit()

        return ret

if __name__ == '__main__':
    backend = SchedulerBackend()
    backend.create_task_type({
        'type': "origin-update-git",
        'description': 'Update an origin git repository',
        'backend_name': 'swh.loader.git.tasks.UpdateGitRepository',
        'default_interval': datetime.timedelta(days=8),
        'min_interval': datetime.timedelta(hours=12),
        'max_interval': datetime.timedelta(days=32),
        'backoff_factor': 2,
    })

    print(backend.get_task_type('origin-update-git'))
