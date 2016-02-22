# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from functools import wraps

import psycopg2
import psycopg2.extras

from swh.core.config import SWHConfig


psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)


def autocommit(fn):
    @wraps(fn)
    def wrapped(self, *args, **kwargs):
        autocommit = False
        if 'cursor' not in kwargs or not kwargs['cursor']:
            autocommit = True
            kwargs['cursor'] = self.cursor()

        try:
            ret = fn(self, *args, **kwargs)
        except:
            if autocommit:
                self.rollback()
            raise

        if autocommit:
            self.commit()

        return ret

    return wrapped


def utcnow():
    return datetime.datetime.now(tz=datetime.timezone.utc)


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
            self.db = psycopg2.connect(
                dsn=self.config['scheduling_db'],
                cursor_factory=psycopg2.extras.RealDictCursor,
            )

    def cursor(self):
        """Return a fresh cursor on the database, with auto-reconnection in case of
        failure"""
        cur = None

        # Get a fresh cursor and reconnect at most three times
        tries = 0
        while True:
            tries += 1
            try:
                cur = self.db.cursor()
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

    def _format_multiquery(self, query, keys, values):
        """Format a query with placeholders generated for multiple values"""
        query_keys = ', '.join(keys)
        placeholders = '), ('.join(
            [', '.join(['%s'] * len(keys))] * len(values)
        )
        ret_values = sum([[value[key] for key in keys]
                          for value in values], [])

        return (
            query.format(keys=query_keys, placeholders=placeholders),
            ret_values,
        )

    @autocommit
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
        query = self._format_query(
            """insert into task_type ({keys}) values ({placeholders})""",
            self.task_type_keys,
        )
        cursor.execute(query, [task_type[key] for key in self.task_type_keys])

    @autocommit
    def get_task_type(self, task_type_name, cursor=None):
        """Retrieve the task type with id task_type_name"""
        query = self._format_query(
            "select {keys} from task_type where type=%s",
            self.task_type_keys,
        )
        cursor.execute(query, (task_type_name,))

        ret = cursor.fetchone()

        return ret

    create_task_keys = ['type', 'arguments', 'next_run']

    task_keys = ['id', 'type', 'arguments', 'next_run', 'current_interval',
                 'status']

    @autocommit
    def create_tasks(self, tasks, cursor=None):
        """Create new tasks.

        A task is a dictionary with the following keys:
            type (str): the task type
            arguments (dict): the arguments for the task runner
                args (list of str): arguments
                kwargs (dict str -> str): keyword arguments
            next_run (datetime.datetime): the next scheduled run for the task
        This returns a list of created task ids.
        """
        cursor.execute('select swh_scheduler_mktemp_task()')
        query, data = self._format_multiquery(
            """insert into tmp_task ({keys}) values ({placeholders})""",
            self.create_task_keys,
            tasks,
        )
        cursor.execute(query, data)
        query = self._format_query(
            'select {keys} from swh_scheduler_create_tasks_from_temp()',
            self.task_keys,
        )
        cursor.execute(query)
        return cursor.fetchall()

    @autocommit
    def peek_ready_tasks(self, timestamp=None, num_tasks=None, cursor=None):
        """Fetch the list of ready tasks

        Args:
            timestamp (datetime.datetime): peek tasks that need to be executed
                before that timestamp
            num_tasks (int): only peek at num_tasks tasks

        Returns:
            a list of tasks
        """

        if timestamp is None:
            timestamp = utcnow()

        cursor.execute('select * from swh_scheduler_peek_ready_tasks(%s, %s)',
                       (timestamp, num_tasks))

        return cursor.fetchall()

    @autocommit
    def grab_ready_tasks(self, timestamp=None, num_tasks=None, cursor=None):
        """Fetch the list of ready tasks, and mark them as scheduled

        Args:
            timestamp (datetime.datetime): grab tasks that need to be executed
                before that timestamp
            num_tasks (int): only grab num_tasks tasks

        Returns:
            a list of tasks
        """

        if timestamp is None:
            timestamp = utcnow()

        cursor.execute('select * from swh_scheduler_grab_ready_tasks(%s, %s)',
                       (timestamp, num_tasks))

        return cursor.fetchall()

    @autocommit
    def schedule_task_run(self, task_id, backend_id, metadata=None,
                          timestamp=None, cursor=None):
        """Mark a given task as scheduled, adding a task_run entry in the database.

        Args:
            task_id (int): the identifier for the task being scheduled
            backend_id (str): the identifier of the job in the backend
            metadata (dict): metadata to add to the task_run entry
            timestamp (datetime.datetime): the instant the event occurred
        Returns:
            a fresh task_run entry
        """

        if metadata is None:
            metadata = {}

        if timestamp is None:
            timestamp = utcnow()

        cursor.execute(
            'select * from swh_scheduler_schedule_task_run(%s, %s, %s, %s)',
            (task_id, backend_id, metadata, timestamp)
        )

        return cursor.fetchone()

    @autocommit
    def start_task_run(self, backend_id, metadata=None, timestamp=None,
                       cursor=None):
        """Mark a given task as started, updating the corresponding task_run
           entry in the database.

        Args:
            backend_id (str): the identifier of the job in the backend
            metadata (dict): metadata to add to the task_run entry
            timestamp (datetime.datetime): the instant the event occurred
        Returns:
            the updated task_run entry
        """

        if metadata is None:
            metadata = {}

        if timestamp is None:
            timestamp = utcnow()

        cursor.execute(
            'select * from swh_scheduler_start_task_run(%s, %s, %s)',
            (backend_id, metadata, timestamp)
        )

        return cursor.fetchone()

    @autocommit
    def end_task_run(self, backend_id, status, metadata=None, timestamp=None,
                     cursor=None):
        """Mark a given task as ended, updating the corresponding task_run
           entry in the database.

        Args:
            backend_id (str): the identifier of the job in the backend
            status ('eventful', 'uneventful', 'failed'): how the task ended
            metadata (dict): metadata to add to the task_run entry
            timestamp (datetime.datetime): the instant the event occurred
        Returns:
            the updated task_run entry
        """

        if metadata is None:
            metadata = {}

        if timestamp is None:
            timestamp = utcnow()

        cursor.execute(
            'select * from swh_scheduler_end_task_run(%s, %s, %s, %s)',
            (backend_id, status, metadata, timestamp)
        )

        return cursor.fetchone()


if __name__ == '__main__':
    backend = SchedulerBackend()
    if not backend.get_task_type('origin-update-git'):
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
    args = '''
    https://github.com/hylang/hy
    https://github.com/torvalds/linux
    '''.strip().split()
    args = [arg.strip() for arg in args]

    tasks = [
        {
            'type': 'origin-update-git',
            'next_run': datetime.datetime.now(tz=datetime.timezone.utc),
            'arguments': {
                'args': [arg],
                'kwargs': {},
            }
        }
        for arg in args
    ]
    print(backend.create_tasks(tasks))
    print(backend.peek_ready_tasks())

    # cur = backend.cursor()
    # ready_tasks = backend.grab_ready_tasks(cursor=cur)
    # print(ready_tasks)

    # for task in ready_tasks:
    #     backend.schedule_task_run(task['id'], 'task-%s' % task['id'],
    #                               {'foo': 'bar'}, cursor=cur)

    # backend.commit()

    # for task in ready_tasks:
    #     backend.start_task_run('task-%s' % task['id'],
    #                            {'worker': 'the-worker'})

    # eventful = True
    # for task in ready_tasks:
    #     eventful = not eventful
    #     backend.end_task_run('task-%s' % task['id'], eventful,
    #                          {'ended': 'ack'})
