# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import binascii
import datetime
from functools import wraps
import json
import tempfile

from arrow import Arrow, utcnow
import psycopg2
import psycopg2.extras
from psycopg2.extensions import AsIs

from swh.core.config import SWHConfig


def adapt_arrow(arrow):
    return AsIs("'%s'::timestamptz" % arrow.isoformat())


psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
psycopg2.extensions.register_adapter(Arrow, adapt_arrow)


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


class SchedulerBackend(SWHConfig):
    """
    Backend for the Software Heritage scheduling database.
    """

    CONFIG_BASE_FILENAME = 'scheduler.ini'
    DEFAULT_CONFIG = {
        'scheduling_db': ('str', 'dbname=softwareheritage-scheduler-dev'),
    }

    def __init__(self, **override_config):
        self.config = self.parse_config_file(global_config=False)
        self.config.update(override_config)

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

    def copy_to(self, items, tblname, columns, cursor=None, item_cb=None):
        def escape(data):
            if data is None:
                return ''
            if isinstance(data, bytes):
                return '\\x%s' % binascii.hexlify(data).decode('ascii')
            elif isinstance(data, str):
                return '"%s"' % data.replace('"', '""')
            elif isinstance(data, (datetime.datetime, Arrow)):
                # We escape twice to make sure the string generated by
                # isoformat gets escaped
                return escape(data.isoformat())
            elif isinstance(data, dict):
                return escape(json.dumps(data))
            elif isinstance(data, list):
                return escape("{%s}" % ','.join(escape(d) for d in data))
            elif isinstance(data, psycopg2.extras.Range):
                # We escape twice here too, so that we make sure
                # everything gets passed to copy properly
                return escape(
                    '%s%s,%s%s' % (
                        '[' if data.lower_inc else '(',
                        '-infinity' if data.lower_inf else escape(data.lower),
                        'infinity' if data.upper_inf else escape(data.upper),
                        ']' if data.upper_inc else ')',
                    )
                )
            else:
                # We don't escape here to make sure we pass literals properly
                return str(data)
        with tempfile.TemporaryFile('w+') as f:
            for d in items:
                if item_cb is not None:
                    item_cb(d)
                line = [escape(d.get(k)) for k in columns]
                f.write(','.join(line))
                f.write('\n')
            f.seek(0)
            cursor.copy_expert('COPY %s (%s) FROM STDIN CSV' % (
                tblname, ', '.join(columns)), f)

    task_type_keys = [
        'type', 'description', 'backend_name', 'default_interval',
        'min_interval', 'max_interval', 'backoff_factor', 'max_queue_length',
        'num_retries', 'retry_delay',
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

        Args:
            task_type (dict): a dictionary with the following keys:

                - type (str): an identifier for the task type
                - description (str): a human-readable description of what the
                  task does
                - backend_name (str): the name of the task in the
                  job-scheduling backend
                - default_interval (datetime.timedelta): the default interval
                  between two task runs
                - min_interval (datetime.timedelta): the minimum interval
                  between two task runs
                - max_interval (datetime.timedelta): the maximum interval
                  between two task runs
                - backoff_factor (float): the factor by which the interval
                  changes at each run
                - max_queue_length (int): the maximum length of the task queue
                  for this task type

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

    @autocommit
    def get_task_types(self, cursor=None):
        query = self._format_query(
            "select {keys} from task_type",
            self.task_type_keys,
        )
        cursor.execute(query)
        ret = cursor.fetchall()
        return ret

    task_create_keys = [
        'type', 'arguments', 'next_run', 'policy', 'retries_left',
    ]
    task_keys = task_create_keys + ['id', 'current_interval', 'status']

    @autocommit
    def create_tasks(self, tasks, cursor=None):
        """Create new tasks.

        Args:
            tasks (list): each task is a dictionary with the following keys:

                - type (str): the task type
                - arguments (dict): the arguments for the task runner, keys:

                      - args (list of str): arguments
                      - kwargs (dict str -> str): keyword arguments

                - next_run (datetime.datetime): the next scheduled run for the
                  task

        Returns:
            a list of created tasks.

        """
        cursor.execute('select swh_scheduler_mktemp_task()')
        self.copy_to(tasks, 'tmp_task', self.task_create_keys, cursor)
        query = self._format_query(
            'select {keys} from swh_scheduler_create_tasks_from_temp()',
            self.task_keys,
        )
        cursor.execute(query)
        return cursor.fetchall()

    @autocommit
    def disable_tasks(self, task_ids, cursor=None):
        """Disable the tasks whose ids are listed."""
        query = "UPDATE task SET status = 'disabled' WHERE id IN %s"
        cursor.execute(query, (tuple(task_ids),))
        return None

    @autocommit
    def get_tasks(self, task_ids, cursor=None):
        """Retrieve the info of tasks whose ids are listed."""
        query = self._format_query('select {keys} from task where id in %s',
                                   self.task_keys)
        cursor.execute(query, (tuple(task_ids),))
        return cursor.fetchall()

    @autocommit
    def peek_ready_tasks(self, task_type, timestamp=None, num_tasks=None,
                         cursor=None):
        """Fetch the list of ready tasks

        Args:
            task_type (str): filtering task per their type
            timestamp (datetime.datetime): peek tasks that need to be executed
                before that timestamp
            num_tasks (int): only peek at num_tasks tasks

        Returns:
            a list of tasks
        """

        if timestamp is None:
            timestamp = utcnow()

        cursor.execute(
            'select * from swh_scheduler_peek_ready_tasks(%s, %s, %s)',
            (task_type, timestamp, num_tasks)
        )

        return cursor.fetchall()

    @autocommit
    def grab_ready_tasks(self, task_type, timestamp=None, num_tasks=None,
                         cursor=None):
        """Fetch the list of ready tasks, and mark them as scheduled

        Args:
            task_type (str): filtering task per their type
            timestamp (datetime.datetime): grab tasks that need to be executed
                before that timestamp
            num_tasks (int): only grab num_tasks tasks

        Returns:
            a list of tasks
        """

        if timestamp is None:
            timestamp = utcnow()

        cursor.execute(
            'select * from swh_scheduler_grab_ready_tasks(%s, %s, %s)',
            (task_type, timestamp, num_tasks)
        )

        return cursor.fetchall()

    task_run_create_keys = ['task', 'backend_id', 'scheduled', 'metadata']

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
    def mass_schedule_task_runs(self, task_runs, cursor=None):
        """Schedule a bunch of task runs.

        Args:
            task_runs (list): a list of dicts with keys:

                - task (int): the identifier for the task being scheduled
                - backend_id (str): the identifier of the job in the backend
                - metadata (dict): metadata to add to the task_run entry
                - scheduled (datetime.datetime): the instant the event occurred

        Returns:
            None
        """
        cursor.execute('select swh_scheduler_mktemp_task_run()')
        self.copy_to(task_runs, 'tmp_task_run', self.task_run_create_keys,
                     cursor)
        cursor.execute('select swh_scheduler_schedule_task_run_from_temp()')

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
                     result=None, cursor=None):
        """Mark a given task as ended, updating the corresponding task_run entry in the
        database.

        Args:
            backend_id (str): the identifier of the job in the backend
            status (str): how the task ended; one of: 'eventful', 'uneventful',
                'failed'
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
