# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import logging

from arrow import Arrow, utcnow
import psycopg2.pool
import psycopg2.extras
from psycopg2.extensions import AsIs

from swh.core.db import BaseDb
from swh.core.db.common import db_transaction, db_transaction_generator


logger = logging.getLogger(__name__)


def adapt_arrow(arrow):
    return AsIs("'%s'::timestamptz" % arrow.isoformat())


psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
psycopg2.extensions.register_adapter(Arrow, adapt_arrow)


def format_query(query, keys):
    """Format a query with the given keys"""

    query_keys = ', '.join(keys)
    placeholders = ', '.join(['%s'] * len(keys))

    return query.format(keys=query_keys, placeholders=placeholders)


class SchedulerBackend:
    """Backend for the Software Heritage scheduling database.

    """

    def __init__(self, db, min_pool_conns=1, max_pool_conns=10):
        """
        Args:
            db_conn: either a libpq connection string, or a psycopg2 connection

        """
        if isinstance(db, psycopg2.extensions.connection):
            self._pool = None
            self._db = BaseDb(db)
        else:
            self._pool = psycopg2.pool.ThreadedConnectionPool(
                min_pool_conns, max_pool_conns, db,
                cursor_factory=psycopg2.extras.RealDictCursor,
            )
            self._db = None

    def get_db(self):
        if self._db:
            return self._db
        return BaseDb.from_pool(self._pool)

    def put_db(self, db):
        if db is not self._db:
            db.put_conn()

    task_type_keys = [
        'type', 'description', 'backend_name', 'default_interval',
        'min_interval', 'max_interval', 'backoff_factor', 'max_queue_length',
        'num_retries', 'retry_delay',
    ]

    @db_transaction()
    def create_task_type(self, task_type, db=None, cur=None):
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
        keys = [key for key in self.task_type_keys if key in task_type]
        query = format_query(
            """insert into task_type ({keys}) values ({placeholders})""",
            keys)
        cur.execute(query, [task_type[key] for key in keys])

    @db_transaction()
    def get_task_type(self, task_type_name, db=None, cur=None):
        """Retrieve the task type with id task_type_name"""
        query = format_query(
            "select {keys} from task_type where type=%s",
            self.task_type_keys,
        )
        cur.execute(query, (task_type_name,))
        return cur.fetchone()

    @db_transaction()
    def get_task_types(self, db=None, cur=None):
        """Retrieve all registered task types"""
        query = format_query(
            "select {keys} from task_type",
            self.task_type_keys,
        )
        cur.execute(query)
        return cur.fetchall()

    task_create_keys = [
        'type', 'arguments', 'next_run', 'policy', 'status', 'retries_left',
        'priority'
    ]
    task_keys = task_create_keys + ['id', 'current_interval']

    @db_transaction()
    def create_tasks(self, tasks, policy='recurring', db=None, cur=None):
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
        cur.execute('select swh_scheduler_mktemp_task()')
        db.copy_to(tasks, 'tmp_task', self.task_create_keys,
                   default_values={
                       'policy': policy,
                       'status': 'next_run_not_scheduled'
                   },
                   cur=cur)
        query = format_query(
            'select {keys} from swh_scheduler_create_tasks_from_temp()',
            self.task_keys,
        )
        cur.execute(query)
        return cur.fetchall()

    @db_transaction()
    def set_status_tasks(self, task_ids, status='disabled', next_run=None,
                         db=None, cur=None):
        """Set the tasks' status whose ids are listed.

        If given, also set the next_run date.
        """
        if not task_ids:
            return
        query = ["UPDATE task SET status = %s"]
        args = [status]
        if next_run:
            query.append(', next_run = %s')
            args.append(next_run)
        query.append(" WHERE id IN %s")
        args.append(tuple(task_ids))

        cur.execute(''.join(query), args)

    @db_transaction()
    def disable_tasks(self, task_ids, db=None, cur=None):
        """Disable the tasks whose ids are listed."""
        return self.set_status_tasks(task_ids, db=db, cur=cur)

    @db_transaction()
    def search_tasks(self, task_id=None, task_type=None, status=None,
                     priority=None, policy=None, before=None, after=None,
                     limit=None, db=None, cur=None):
        """Search tasks from selected criterions"""
        where = []
        args = []

        if task_id:
            if isinstance(task_id, (str, int)):
                where.append('id = %s')
            else:
                where.append('id in %s')
                task_id = tuple(task_id)
            args.append(task_id)
        if task_type:
            if isinstance(task_type, str):
                where.append('type = %s')
            else:
                where.append('type in %s')
                task_type = tuple(task_type)
            args.append(task_type)
        if status:
            if isinstance(status, str):
                where.append('status = %s')
            else:
                where.append('status in %s')
                status = tuple(status)
            args.append(status)
        if priority:
            if isinstance(priority, str):
                where.append('priority = %s')
            else:
                priority = tuple(priority)
                where.append('priority in %s')
            args.append(priority)
        if policy:
            where.append('policy = %s')
            args.append(policy)
        if before:
            where.append('next_run <= %s')
            args.append(before)
        if after:
            where.append('next_run >= %s')
            args.append(after)

        query = 'select * from task'
        if where:
            query += ' where ' + ' and '.join(where)
        if limit:
            query += ' limit %s :: bigint'
            args.append(limit)
        cur.execute(query, args)
        return cur.fetchall()

    @db_transaction()
    def get_tasks(self, task_ids, db=None, cur=None):
        """Retrieve the info of tasks whose ids are listed."""
        query = format_query('select {keys} from task where id in %s',
                             self.task_keys)
        cur.execute(query, (tuple(task_ids),))
        return cur.fetchall()

    @db_transaction()
    def peek_ready_tasks(self, task_type, timestamp=None, num_tasks=None,
                         num_tasks_priority=None,
                         db=None, cur=None):
        """Fetch the list of ready tasks

        Args:
            task_type (str): filtering task per their type
            timestamp (datetime.datetime): peek tasks that need to be executed
                before that timestamp
            num_tasks (int): only peek at num_tasks tasks (with no priority)
            num_tasks_priority (int): only peek at num_tasks_priority
                                      tasks (with priority)

        Returns:
            a list of tasks

        """
        if timestamp is None:
            timestamp = utcnow()

        cur.execute(
            '''select * from swh_scheduler_peek_ready_tasks(
                %s, %s, %s :: bigint, %s :: bigint)''',
            (task_type, timestamp, num_tasks, num_tasks_priority)
        )
        logger.debug('PEEK %s => %s' % (task_type, cur.rowcount))
        return cur.fetchall()

    @db_transaction()
    def grab_ready_tasks(self, task_type, timestamp=None, num_tasks=None,
                         num_tasks_priority=None, db=None, cur=None):
        """Fetch the list of ready tasks, and mark them as scheduled

        Args:
            task_type (str): filtering task per their type
            timestamp (datetime.datetime): grab tasks that need to be executed
                before that timestamp
            num_tasks (int): only grab num_tasks tasks (with no priority)
            num_tasks_priority (int): only grab oneshot num_tasks tasks (with
                                      priorities)

        Returns:
            a list of tasks

        """
        if timestamp is None:
            timestamp = utcnow()
        cur.execute(
            '''select * from swh_scheduler_grab_ready_tasks(
                 %s, %s, %s :: bigint, %s :: bigint)''',
            (task_type, timestamp, num_tasks, num_tasks_priority)
        )
        logger.debug('GRAB %s => %s' % (task_type, cur.rowcount))
        return cur.fetchall()

    task_run_create_keys = ['task', 'backend_id', 'scheduled', 'metadata']

    @db_transaction()
    def schedule_task_run(self, task_id, backend_id, metadata=None,
                          timestamp=None, db=None, cur=None):
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

        cur.execute(
            'select * from swh_scheduler_schedule_task_run(%s, %s, %s, %s)',
            (task_id, backend_id, metadata, timestamp)
        )

        return cur.fetchone()

    @db_transaction()
    def mass_schedule_task_runs(self, task_runs, db=None, cur=None):
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
        cur.execute('select swh_scheduler_mktemp_task_run()')
        db.copy_to(task_runs, 'tmp_task_run', self.task_run_create_keys,
                   cur=cur)
        cur.execute('select swh_scheduler_schedule_task_run_from_temp()')

    @db_transaction()
    def start_task_run(self, backend_id, metadata=None, timestamp=None,
                       db=None, cur=None):
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

        cur.execute(
            'select * from swh_scheduler_start_task_run(%s, %s, %s)',
            (backend_id, metadata, timestamp)
        )

        return cur.fetchone()

    @db_transaction()
    def end_task_run(self, backend_id, status, metadata=None, timestamp=None,
                     result=None, db=None, cur=None):
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

        cur.execute(
            'select * from swh_scheduler_end_task_run(%s, %s, %s, %s)',
            (backend_id, status, metadata, timestamp)
        )
        return cur.fetchone()

    @db_transaction_generator()
    def filter_task_to_archive(self, after_ts, before_ts, limit=10, last_id=-1,
                               db=None, cur=None):
        """Returns the list of task/task_run prior to a given date to archive.

        """
        last_task_run_id = None
        while True:
            row = None
            cur.execute(
                "select * from swh_scheduler_task_to_archive(%s, %s, %s, %s)",
                (after_ts, before_ts, last_id, limit)
            )
            for row in cur:
                # nested type index does not accept bare values
                # transform it as a dict to comply with this
                row['arguments']['args'] = {
                    i: v for i, v in enumerate(row['arguments']['args'])
                }
                kwargs = row['arguments']['kwargs']
                row['arguments']['kwargs'] = json.dumps(kwargs)
                yield row

            if not row:
                break
            _id = row.get('task_id')
            _task_run_id = row.get('task_run_id')
            if last_id == _id and last_task_run_id == _task_run_id:
                break
            last_id = _id
            last_task_run_id = _task_run_id

    @db_transaction()
    def delete_archived_tasks(self, task_ids, db=None, cur=None):
        """Delete archived tasks as much as possible. Only the task_ids whose
           complete associated task_run have been cleaned up will be.

        """
        _task_ids = _task_run_ids = []
        for task_id in task_ids:
            _task_ids.append(task_id['task_id'])
            _task_run_ids.append(task_id['task_run_id'])

        cur.execute(
            "select * from swh_scheduler_delete_archived_tasks(%s, %s)",
            (_task_ids, _task_run_ids))

    task_run_keys = ['id', 'task', 'backend_id', 'scheduled',
                     'started', 'ended', 'metadata', 'status', ]

    @db_transaction()
    def get_task_runs(self, task_ids, limit=None, db=None, cur=None):
        """Search task run for a task id"""
        where = []
        args = []

        if task_ids:
            if isinstance(task_ids, (str, int)):
                where.append('task = %s')
            else:
                where.append('task in %s')
                task_ids = tuple(task_ids)
            args.append(task_ids)
        else:
            return ()

        query = 'select * from task_run where ' + ' and '.join(where)
        if limit:
            query += ' limit %s :: bigint'
            args.append(limit)
        cur.execute(query, args)
        return cur.fetchall()
