# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import datetime
import os
import random
import unittest
import uuid
from collections import defaultdict

import psycopg2
from arrow import utcnow
import pytest

from swh.core.db.tests.db_testing import SingleDbTestFixture
from swh.scheduler import get_scheduler

from . import SQL_DIR


TASK_TYPES = {
    'git': {
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
    },
    'hg': {
        'type': 'update-hg',
        'description': 'Update a mercurial repository',
        'backend_name': 'swh.loader.mercurial.tasks.UpdateHgRepository',
        'default_interval': datetime.timedelta(days=64),
        'min_interval': datetime.timedelta(hours=12),
        'max_interval': datetime.timedelta(days=64),
        'backoff_factor': 2,
        'max_queue_length': None,
        'num_retries': 7,
        'retry_delay': datetime.timedelta(hours=2),
    },
}

TEMPLATES = {
    'git': {
        'type': 'update-git',
        'arguments': {
            'args': [],
            'kwargs': {},
        },
        'next_run': None,
    },
    'hg': {
        'type': 'update-hg',
        'arguments': {
            'args': [],
            'kwargs': {},
        },
        'next_run': None,
        'policy': 'oneshot',
    }
}


def subdict(d, keys=None, excl=()):
    if keys is None:
        keys = [k for k in d.keys()]
    return {k: d[k] for k in keys if k not in excl}


@pytest.mark.db
class CommonSchedulerTest(SingleDbTestFixture):
    TEST_DB_NAME = 'softwareheritage-scheduler-test'
    TEST_DB_DUMP = os.path.join(SQL_DIR, '*.sql')

    def tearDown(self):
        self.empty_tables()
        super().tearDown()

    def empty_tables(self, whitelist=["priority_ratio"]):
        query = """SELECT table_name FROM information_schema.tables
                   WHERE table_schema = %%s and
                         table_name not in (%s)
                """ % ','.join(map(lambda t: "'%s'" % t, whitelist))
        self.cursor.execute(query, ('public', ))

        tables = set(table for (table,) in self.cursor.fetchall())

        for table in tables:
            self.cursor.execute('truncate table %s cascade' % table)
        self.conn.commit()

    def test_add_task_type(self):
        tt = TASK_TYPES['git']
        self.backend.create_task_type(tt)
        self.assertEqual(tt, self.backend.get_task_type(tt['type']))
        with self.assertRaisesRegex(psycopg2.IntegrityError,
                                    r'\(type\)=\(%s\)' % tt['type']):
            self.backend.create_task_type(tt)

        tt2 = TASK_TYPES['hg']
        self.backend.create_task_type(tt2)
        self.assertEqual(tt, self.backend.get_task_type(tt['type']))
        self.assertEqual(tt2, self.backend.get_task_type(tt2['type']))

    def test_get_task_types(self):
        tt, tt2 = TASK_TYPES['git'], TASK_TYPES['hg']
        self.backend.create_task_type(tt)
        self.backend.create_task_type(tt2)
        self.assertCountEqual([tt2, tt], self.backend.get_task_types())

    @staticmethod
    def _task_from_template(template, next_run, priority, *args, **kwargs):
        ret = copy.deepcopy(template)
        ret['next_run'] = next_run
        if priority:
            ret['priority'] = priority
        if args:
            ret['arguments']['args'] = list(args)
        if kwargs:
            ret['arguments']['kwargs'] = kwargs
        return ret

    def _pop_priority(self, priorities):
        if not priorities:
            return None
        for priority, remains in priorities.items():
            if remains > 0:
                priorities[priority] = remains - 1
                return priority
        return None

    def _tasks_from_template(self, template, max_timestamp, num,
                             num_priority=0, priorities=None):
        if num_priority and priorities:
            priorities = {
                priority: ratio * num_priority
                for priority, ratio in priorities.items()
            }

        tasks = []
        for i in range(num + num_priority):
            priority = self._pop_priority(priorities)
            tasks.append(self._task_from_template(
                template,
                max_timestamp - datetime.timedelta(microseconds=i),
                priority,
                'argument-%03d' % i,
                **{'kwarg%03d' % i: 'bogus-kwarg'}
            ))
        return tasks

    def _create_task_types(self):
        for tt in TASK_TYPES.values():
            self.backend.create_task_type(tt)

    def test_create_tasks(self):
        priority_ratio = self._priority_ratio()
        self._create_task_types()
        num_tasks_priority = 100
        tasks_1 = self._tasks_from_template(
            TEMPLATES['git'], utcnow(), 100)
        tasks_2 = self._tasks_from_template(
            TEMPLATES['hg'], utcnow(), 100,
            num_tasks_priority, priorities=priority_ratio)
        tasks = tasks_1 + tasks_2

        # tasks are returned only once with their ids
        ret1 = self.backend.create_tasks(tasks + tasks_1 + tasks_2)
        set_ret1 = set([t['id'] for t in ret1])

        # creating the same set result in the same ids
        ret = self.backend.create_tasks(tasks)
        set_ret = set([t['id'] for t in ret])

        # Idempotence results
        self.assertEqual(set_ret, set_ret1)
        self.assertEqual(len(ret), len(ret1))

        ids = set()
        actual_priorities = defaultdict(int)

        for task, orig_task in zip(ret, tasks):
            task = copy.deepcopy(task)
            task_type = TASK_TYPES[orig_task['type'].split('-')[-1]]
            self.assertNotIn(task['id'], ids)
            self.assertEqual(task['status'], 'next_run_not_scheduled')
            self.assertEqual(task['current_interval'],
                             task_type['default_interval'])
            self.assertEqual(task['policy'], orig_task.get('policy',
                                                           'recurring'))
            priority = task.get('priority')
            if priority:
                actual_priorities[priority] += 1

            self.assertEqual(task['retries_left'],
                             task_type['num_retries'] or 0)
            ids.add(task['id'])
            del task['id']
            del task['status']
            del task['current_interval']
            del task['retries_left']
            if 'policy' not in orig_task:
                del task['policy']
            if 'priority' not in orig_task:
                del task['priority']
                self.assertEqual(task, orig_task)

        self.assertEqual(dict(actual_priorities), {
            priority: int(ratio * num_tasks_priority)
            for priority, ratio in priority_ratio.items()
        })

    def test_peek_ready_tasks_no_priority(self):
        self._create_task_types()
        t = utcnow()
        task_type = TEMPLATES['git']['type']
        tasks = self._tasks_from_template(TEMPLATES['git'], t, 100)
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

    def _priority_ratio(self):
        self.cursor.execute('select id, ratio from priority_ratio')
        priority_ratio = {}
        for row in self.cursor.fetchall():
            priority_ratio[row[0]] = row[1]
        return priority_ratio

    def test_peek_ready_tasks_mixed_priorities(self):
        priority_ratio = self._priority_ratio()
        self._create_task_types()
        t = utcnow()
        task_type = TEMPLATES['git']['type']
        num_tasks_priority = 100
        num_tasks_no_priority = 100
        # Create tasks with and without priorities
        tasks = self._tasks_from_template(
            TEMPLATES['git'], t,
            num=num_tasks_no_priority,
            num_priority=num_tasks_priority,
            priorities=priority_ratio)

        random.shuffle(tasks)
        self.backend.create_tasks(tasks)

        # take all available tasks
        ready_tasks = self.backend.peek_ready_tasks(
            task_type)

        self.assertEqual(len(ready_tasks), len(tasks))
        self.assertEqual(num_tasks_priority + num_tasks_no_priority,
                         len(ready_tasks))

        count_tasks_per_priority = defaultdict(int)
        for task in ready_tasks:
            priority = task.get('priority')
            if priority:
                count_tasks_per_priority[priority] += 1

        self.assertEqual(dict(count_tasks_per_priority), {
            priority: int(ratio * num_tasks_priority)
            for priority, ratio in priority_ratio.items()
        })

        # Only get some ready tasks
        num_tasks = random.randrange(5, 5 + num_tasks_no_priority//2)
        num_tasks_priority = random.randrange(5, num_tasks_priority//2)
        ready_tasks_limited = self.backend.peek_ready_tasks(
            task_type, num_tasks=num_tasks,
            num_tasks_priority=num_tasks_priority)

        count_tasks_per_priority = defaultdict(int)
        for task in ready_tasks_limited:
            priority = task.get('priority')
            count_tasks_per_priority[priority] += 1

        import math
        for priority, ratio in priority_ratio.items():
            expected_count = math.ceil(ratio * num_tasks_priority)
            actual_prio = count_tasks_per_priority[priority]
            self.assertTrue(
                actual_prio == expected_count or
                actual_prio == expected_count + 1)

        self.assertEqual(count_tasks_per_priority[None], num_tasks)

    def test_grab_ready_tasks(self):
        priority_ratio = self._priority_ratio()
        self._create_task_types()
        t = utcnow()
        task_type = TEMPLATES['git']['type']
        num_tasks_priority = 100
        num_tasks_no_priority = 100
        # Create tasks with and without priorities
        tasks = self._tasks_from_template(
            TEMPLATES['git'], t,
            num=num_tasks_no_priority,
            num_priority=num_tasks_priority,
            priorities=priority_ratio)
        random.shuffle(tasks)
        self.backend.create_tasks(tasks)

        first_ready_tasks = self.backend.peek_ready_tasks(
            task_type, num_tasks=10, num_tasks_priority=10)
        grabbed_tasks = self.backend.grab_ready_tasks(
            task_type, num_tasks=10, num_tasks_priority=10)

        for peeked, grabbed in zip(first_ready_tasks, grabbed_tasks):
            self.assertEqual(peeked['status'], 'next_run_not_scheduled')
            del peeked['status']
            self.assertEqual(grabbed['status'], 'next_run_scheduled')
            del grabbed['status']
            self.assertEqual(peeked, grabbed)
            self.assertEqual(peeked['priority'], grabbed['priority'])

    def test_get_tasks(self):
        self._create_task_types()
        t = utcnow()
        tasks = self._tasks_from_template(TEMPLATES['git'], t, 100)
        tasks = self.backend.create_tasks(tasks)
        random.shuffle(tasks)
        while len(tasks) > 1:
            length = random.randrange(1, len(tasks))
            cur_tasks = tasks[:length]
            tasks[:length] = []

            ret = self.backend.get_tasks(task['id'] for task in cur_tasks)
            self.assertCountEqual(ret, cur_tasks)

    def test_search_tasks(self):
        def make_real_dicts(l):
            """RealDictRow is not a real dict."""
            return [dict(d.items()) for d in l]
        self._create_task_types()
        t = utcnow()
        tasks = self._tasks_from_template(TEMPLATES['git'], t, 100)
        tasks = self.backend.create_tasks(tasks)
        self.assertCountEqual(
            make_real_dicts(self.backend.search_tasks()),
            make_real_dicts(tasks))

    def test_filter_task_to_archive(self):
        """Filtering only list disabled recurring or completed oneshot tasks

        """
        self._create_task_types()
        _time = utcnow()
        recurring = self._tasks_from_template(TEMPLATES['git'], _time, 12)
        oneshots = self._tasks_from_template(TEMPLATES['hg'], _time, 12)
        total_tasks = len(recurring) + len(oneshots)

        # simulate scheduling tasks
        pending_tasks = self.backend.create_tasks(recurring + oneshots)
        backend_tasks = [{
            'task': task['id'],
            'backend_id': str(uuid.uuid4()),
            'scheduled': utcnow(),
        } for task in pending_tasks]
        self.backend.mass_schedule_task_runs(backend_tasks)

        # we simulate the task are being done
        _tasks = []
        for task in backend_tasks:
            t = self.backend.end_task_run(
                task['backend_id'], status='eventful')
            _tasks.append(t)

        # Randomly update task's status per policy
        status_per_policy = {'recurring': 0, 'oneshot': 0}
        status_choice = {
            # policy: [tuple (1-for-filtering, 'associated-status')]
            'recurring': [(1, 'disabled'),
                          (0, 'completed'),
                          (0, 'next_run_not_scheduled')],
            'oneshot': [(0, 'next_run_not_scheduled'),
                        (1, 'disabled'),
                        (1, 'completed')]
        }

        tasks_to_update = defaultdict(list)
        _task_ids = defaultdict(list)
        # randomize 'disabling' recurring task or 'complete' oneshot task
        for task in pending_tasks:
            policy = task['policy']
            _task_ids[policy].append(task['id'])
            status = random.choice(status_choice[policy])
            if status[0] != 1:
                continue
            # elected for filtering
            status_per_policy[policy] += status[0]
            tasks_to_update[policy].append(task['id'])

        self.backend.disable_tasks(tasks_to_update['recurring'])
        # hack: change the status to something else than completed/disabled
        self.backend.set_status_tasks(
            _task_ids['oneshot'], status='next_run_not_scheduled')
        # complete the tasks to update
        self.backend.set_status_tasks(
            tasks_to_update['oneshot'], status='completed')

        total_tasks_filtered = (status_per_policy['recurring'] +
                                status_per_policy['oneshot'])

        # retrieve tasks to archive
        after = _time.shift(days=-1).format('YYYY-MM-DD')
        before = utcnow().shift(days=1).format('YYYY-MM-DD')
        tasks_to_archive = list(self.backend.filter_task_to_archive(
            after_ts=after, before_ts=before, limit=total_tasks))

        self.assertEqual(len(tasks_to_archive), total_tasks_filtered)

        actual_filtered_per_status = {'recurring': 0, 'oneshot': 0}
        for task in tasks_to_archive:
            actual_filtered_per_status[task['task_policy']] += 1

        self.assertEqual(actual_filtered_per_status, status_per_policy)

    def test_delete_archived_tasks(self):
        self._create_task_types()
        _time = utcnow()
        recurring = self._tasks_from_template(
            TEMPLATES['git'], _time, 12)
        oneshots = self._tasks_from_template(
            TEMPLATES['hg'], _time, 12)
        total_tasks = len(recurring) + len(oneshots)
        pending_tasks = self.backend.create_tasks(recurring + oneshots)
        backend_tasks = [{
            'task': task['id'],
            'backend_id': str(uuid.uuid4()),
            'scheduled': utcnow(),
        } for task in pending_tasks]
        self.backend.mass_schedule_task_runs(backend_tasks)

        _tasks = []
        percent = random.randint(0, 100)  # random election removal boundary
        for task in backend_tasks:
            t = self.backend.end_task_run(
                task['backend_id'], status='eventful')
            c = random.randint(0, 100)
            if c <= percent:
                _tasks.append({'task_id': t['task'], 'task_run_id': t['id']})

        self.backend.delete_archived_tasks(_tasks)

        self.cursor.execute('select count(*) from task')
        tasks_count = self.cursor.fetchone()

        self.cursor.execute('select count(*) from task_run')
        tasks_run_count = self.cursor.fetchone()

        self.assertEqual(tasks_count[0], total_tasks - len(_tasks))
        self.assertEqual(tasks_run_count[0], total_tasks - len(_tasks))

    def test_get_task_runs_no_task(self):
        '''No task exist in the scheduler's db, get_task_runs() should always return an
        empty list.

        '''
        self.assertFalse(self.backend.get_task_runs(task_ids=()))
        self.assertFalse(self.backend.get_task_runs(task_ids=(1, 2, 3)))
        self.assertFalse(self.backend.get_task_runs(task_ids=(1, 2, 3),
                                                    limit=10))

    def test_get_task_runs_no_task_executed(self):
        '''No task has been executed yet, get_task_runs() should always return an empty
        list.

        '''
        self._create_task_types()
        _time = utcnow()
        recurring = self._tasks_from_template(
            TEMPLATES['git'], _time, 12)
        oneshots = self._tasks_from_template(
            TEMPLATES['hg'], _time, 12)
        self.backend.create_tasks(recurring + oneshots)

        self.assertFalse(self.backend.get_task_runs(
            task_ids=()))
        self.assertFalse(self.backend.get_task_runs(
            task_ids=(1, 2, 3)))
        self.assertFalse(self.backend.get_task_runs(
            task_ids=(1, 2, 3), limit=10))

    def test_get_task_runs_with_scheduled(self):
        '''Some tasks have been scheduled but not executed yet, get_task_runs() should
        not return an empty list. limit should behave as expected.

        '''
        self._create_task_types()
        _time = utcnow()
        recurring = self._tasks_from_template(
            TEMPLATES['git'], _time, 12)
        oneshots = self._tasks_from_template(
            TEMPLATES['hg'], _time, 12)
        total_tasks = len(recurring) + len(oneshots)
        pending_tasks = self.backend.create_tasks(recurring + oneshots)
        backend_tasks = [{
            'task': task['id'],
            'backend_id': str(uuid.uuid4()),
            'scheduled': utcnow(),
        } for task in pending_tasks]
        self.backend.mass_schedule_task_runs(backend_tasks)

        self.assertFalse(self.backend.get_task_runs(
            task_ids=[total_tasks + 1]))

        btask = backend_tasks[0]
        runs = self.backend.get_task_runs(
            task_ids=[btask['task']])
        self.assertEqual(len(runs), 1)
        run = runs[0]

        self.assertEqual(subdict(run, excl=('id',)),
                         {'task': btask['task'],
                          'backend_id': btask['backend_id'],
                          'scheduled': btask['scheduled'],
                          'started': None,
                          'ended': None,
                          'metadata': None,
                          'status': 'scheduled',
                          })

        runs = self.backend.get_task_runs(
            task_ids=[bt['task'] for bt in backend_tasks], limit=2)
        self.assertEqual(len(runs), 2)

        runs = self.backend.get_task_runs(
            task_ids=[bt['task'] for bt in backend_tasks])
        self.assertEqual(len(runs), total_tasks)

        keys = ('task', 'backend_id', 'scheduled')
        self.assertEqual(sorted([subdict(x, keys) for x in runs],
                                key=lambda x: x['task']),
                         backend_tasks)

    def test_get_task_runs_with_executed(self):
        '''Some tasks have been executed, get_task_runs() should
        not return an empty list. limit should behave as expected.

        '''
        self._create_task_types()
        _time = utcnow()
        recurring = self._tasks_from_template(
            TEMPLATES['git'], _time, 12)
        oneshots = self._tasks_from_template(
            TEMPLATES['hg'], _time, 12)
        pending_tasks = self.backend.create_tasks(recurring + oneshots)
        backend_tasks = [{
            'task': task['id'],
            'backend_id': str(uuid.uuid4()),
            'scheduled': utcnow(),
        } for task in pending_tasks]
        self.backend.mass_schedule_task_runs(backend_tasks)

        btask = backend_tasks[0]
        ts = utcnow()
        self.backend.start_task_run(btask['backend_id'],
                                    metadata={'something': 'stupid'},
                                    timestamp=ts)
        runs = self.backend.get_task_runs(task_ids=[btask['task']])
        self.assertEqual(len(runs), 1)
        self.assertEqual(subdict(runs[0], excl=('id')), {
            'task': btask['task'],
            'backend_id': btask['backend_id'],
            'scheduled': btask['scheduled'],
            'started': ts,
            'ended': None,
            'metadata': {'something': 'stupid'},
            'status': 'started',
            })

        ts2 = utcnow()
        self.backend.end_task_run(btask['backend_id'],
                                  metadata={'other': 'stuff'},
                                  timestamp=ts2,
                                  status='eventful')
        runs = self.backend.get_task_runs(task_ids=[btask['task']])
        self.assertEqual(len(runs), 1)
        self.assertEqual(subdict(runs[0], excl=('id')), {
            'task': btask['task'],
            'backend_id': btask['backend_id'],
            'scheduled': btask['scheduled'],
            'started': ts,
            'ended': ts2,
            'metadata': {'something': 'stupid', 'other': 'stuff'},
            'status': 'eventful',
            })


class LocalSchedulerTest(CommonSchedulerTest, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.config = {'db': 'dbname=' + self.TEST_DB_NAME}
        self.backend = get_scheduler('local', self.config)
