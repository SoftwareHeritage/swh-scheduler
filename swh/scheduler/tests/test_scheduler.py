# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import datetime
import os
import random
import unittest

from arrow import utcnow
from nose.plugins.attrib import attr
from nose.tools import istest
import psycopg2

from swh.core.tests.db_testing import SingleDbTestFixture
from swh.scheduler.backend import SchedulerBackend


TEST_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_DATA_DIR = os.path.join(TEST_DIR, '../../../../swh-storage-testdata')


@attr('db')
class Scheduler(SingleDbTestFixture, unittest.TestCase):
    TEST_DB_NAME = 'softwareheritage-scheduler-test'
    TEST_DB_DUMP = os.path.join(TEST_DATA_DIR, 'dumps/swh-scheduler.dump')

    def setUp(self):
        super().setUp()
        self.config = {'scheduling_db': 'dbname=' + self.TEST_DB_NAME}
        self.backend = SchedulerBackend(**self.config)

        tt = {
            'type': 'update-git',
            'description': 'Update a git repository',
            'backend_name': 'swh.loader.git.tasks.UpdateGitRepository',
            'default_interval': datetime.timedelta(days=64),
            'min_interval': datetime.timedelta(hours=12),
            'max_interval': datetime.timedelta(days=64),
            'backoff_factor': 2,
            'max_queue_length': None,
            'num_retries': 7,
            'retry_delay': datetime.timedelta(hours=2),
        }
        tt2 = tt.copy()
        tt2['type'] = 'update-hg'
        tt2['description'] = 'Update a mercurial repository'
        tt2['backend_name'] = 'swh.loader.mercurial.tasks.UpdateHgRepository'
        tt2['max_queue_length'] = 42
        tt2['num_retries'] = None
        tt2['retry_delay'] = None

        self.task_types = {
            tt['type']: tt,
            tt2['type']: tt2,
        }

        self.task1_template = t1_template = {
            'type': tt['type'],
            'arguments': {
                'args': [],
                'kwargs': {},
            },
            'next_run': None,
        }
        self.task2_template = t2_template = copy.deepcopy(t1_template)
        t2_template['type'] = tt2['type']
        t2_template['policy'] = 'oneshot'

    def tearDown(self):
        self.backend.db.close()
        self.empty_tables()
        super().tearDown()

    def empty_tables(self):
        self.cursor.execute("""SELECT table_name FROM information_schema.tables
                               WHERE table_schema = %s""", ('public',))

        tables = set(table for (table,) in self.cursor.fetchall())

        for table in tables:
            self.cursor.execute('truncate table %s cascade' % table)
        self.conn.commit()

    @istest
    def add_task_type(self):
        tt, tt2 = self.task_types.values()
        self.backend.create_task_type(tt)
        self.assertEqual(tt, self.backend.get_task_type(tt['type']))
        with self.assertRaisesRegex(psycopg2.IntegrityError,
                                    '\(type\)=\(%s\)' % tt['type']):
            self.backend.create_task_type(tt)
        self.backend.create_task_type(tt2)
        self.assertEqual(tt, self.backend.get_task_type(tt['type']))
        self.assertEqual(tt2, self.backend.get_task_type(tt2['type']))

    @istest
    def get_task_types(self):
        tt, tt2 = self.task_types.values()
        self.backend.create_task_type(tt)
        self.backend.create_task_type(tt2)
        self.assertCountEqual([tt2, tt], self.backend.get_task_types())

    @staticmethod
    def _task_from_template(template, next_run, *args, **kwargs):
        ret = copy.deepcopy(template)
        ret['next_run'] = next_run
        if args:
            ret['arguments']['args'] = list(args)
        if kwargs:
            ret['arguments']['kwargs'] = kwargs
        return ret

    def _tasks_from_template(self, template, max_timestamp, num):
        return [
            self._task_from_template(
                template,
                max_timestamp - datetime.timedelta(microseconds=i),
                'argument-%03d' % i,
                **{'kwarg%03d' % i: 'bogus-kwarg'}
            )
            for i in range(num)
        ]

    def _create_task_types(self):
        for tt in self.task_types.values():
            self.backend.create_task_type(tt)

    @istest
    def create_tasks(self):
        self._create_task_types()
        tasks = (
            self._tasks_from_template(self.task1_template, utcnow(), 100)
            + self._tasks_from_template(self.task2_template, utcnow(), 100)
        )
        ret = self.backend.create_tasks(tasks)
        ids = set()
        for task, orig_task in zip(ret, tasks):
            task = copy.deepcopy(task)
            task_type = self.task_types[orig_task['type']]
            self.assertNotIn(task['id'], ids)
            self.assertEqual(task['status'], 'next_run_not_scheduled')
            self.assertEqual(task['current_interval'],
                             task_type['default_interval'])
            self.assertEqual(task['policy'], orig_task.get('policy',
                                                           'recurring'))
            self.assertEqual(task['retries_left'],
                             task_type['num_retries'] or 0)
            ids.add(task['id'])
            del task['id']
            del task['status']
            del task['current_interval']
            del task['retries_left']
            if 'policy' not in orig_task:
                del task['policy']
            self.assertEqual(task, orig_task)

    @istest
    def peek_ready_tasks(self):
        self._create_task_types()
        t = utcnow()
        task_type = self.task1_template['type']
        tasks = self._tasks_from_template(self.task1_template, t, 100)
        random.shuffle(tasks)
        self.backend.create_tasks(tasks)

        ready_tasks = self.backend.peek_ready_tasks(task_type)
        self.assertEqual(len(ready_tasks), len(tasks))
        for i in range(len(ready_tasks) - 1):
            self.assertLessEqual(ready_tasks[i]['next_run'],
                                 ready_tasks[i+1]['next_run'])

        # Only get the first few ready tasks
        limit = random.randrange(5, 5 + len(tasks)//2)
        ready_tasks_limited = self.backend.peek_ready_tasks(
            task_type, num_tasks=limit)

        self.assertEqual(len(ready_tasks_limited), limit)
        self.assertCountEqual(ready_tasks_limited, ready_tasks[:limit])

        # Limit by timestamp
        max_ts = tasks[limit-1]['next_run']
        ready_tasks_timestamped = self.backend.peek_ready_tasks(
            task_type, timestamp=max_ts)

        for ready_task in ready_tasks_timestamped:
            self.assertLessEqual(ready_task['next_run'], max_ts)

        # Make sure we get proper behavior for the first ready tasks
        self.assertCountEqual(
            ready_tasks[:len(ready_tasks_timestamped)],
            ready_tasks_timestamped,
        )

        # Limit by both
        ready_tasks_both = self.backend.peek_ready_tasks(
            task_type, timestamp=max_ts, num_tasks=limit//3)
        self.assertLessEqual(len(ready_tasks_both), limit//3)
        for ready_task in ready_tasks_both:
            self.assertLessEqual(ready_task['next_run'], max_ts)
            self.assertIn(ready_task, ready_tasks[:limit//3])

    @istest
    def grab_ready_tasks(self):
        self._create_task_types()
        t = utcnow()
        task_type = self.task1_template['type']
        tasks = self._tasks_from_template(self.task1_template, t, 100)
        random.shuffle(tasks)
        self.backend.create_tasks(tasks)

        first_ready_tasks = self.backend.peek_ready_tasks(
            task_type, num_tasks=10)
        grabbed_tasks = self.backend.grab_ready_tasks(task_type, num_tasks=10)

        for peeked, grabbed in zip(first_ready_tasks, grabbed_tasks):
            self.assertEqual(peeked['status'], 'next_run_not_scheduled')
            del peeked['status']
            self.assertEqual(grabbed['status'], 'next_run_scheduled')
            del grabbed['status']
            self.assertEqual(peeked, grabbed)

    @istest
    def get_tasks(self):
        self._create_task_types()
        t = utcnow()
        tasks = self._tasks_from_template(self.task1_template, t, 100)
        tasks = self.backend.create_tasks(tasks)
        random.shuffle(tasks)
        while len(tasks) > 1:
            length = random.randrange(1, len(tasks))
            cur_tasks = tasks[:length]
            tasks[:length] = []

            ret = self.backend.get_tasks(task['id'] for task in cur_tasks)
            self.assertCountEqual(ret, cur_tasks)
