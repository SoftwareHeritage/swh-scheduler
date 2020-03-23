# Copyright (C) 2017-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import datetime
import random
import uuid

from collections import defaultdict
from typing import Any, Dict

from arrow import utcnow

import pytest

from .common import tasks_from_template, TEMPLATES, TASK_TYPES


def subdict(d, keys=None, excl=()):
    if keys is None:
        keys = [k for k in d.keys()]
    return {k: d[k] for k in keys if k not in excl}


@pytest.mark.db
class TestScheduler:
    def test_get_priority_ratios(self, swh_scheduler):
        assert swh_scheduler.get_priority_ratios() == {
            'high': 0.5,
            'normal': 0.3,
            'low': 0.2,
            }

    def test_add_task_type(self, swh_scheduler):
        tt = TASK_TYPES['git']
        swh_scheduler.create_task_type(tt)
        assert tt == swh_scheduler.get_task_type(tt['type'])
        tt2 = TASK_TYPES['hg']
        swh_scheduler.create_task_type(tt2)
        assert tt == swh_scheduler.get_task_type(tt['type'])
        assert tt2 == swh_scheduler.get_task_type(tt2['type'])

    def test_create_task_type_idempotence(self, swh_scheduler):
        tt = TASK_TYPES['git']
        swh_scheduler.create_task_type(tt)
        swh_scheduler.create_task_type(tt)
        assert tt == swh_scheduler.get_task_type(tt['type'])

    def test_get_task_types(self, swh_scheduler):
        tt, tt2 = TASK_TYPES['git'], TASK_TYPES['hg']
        swh_scheduler.create_task_type(tt)
        swh_scheduler.create_task_type(tt2)
        actual_task_types = swh_scheduler.get_task_types()
        assert tt in actual_task_types
        assert tt2 in actual_task_types

    def test_create_tasks(self, swh_scheduler):
        priority_ratio = self._priority_ratio(swh_scheduler)
        self._create_task_types(swh_scheduler)
        num_tasks_priority = 100
        tasks_1 = tasks_from_template(
            TEMPLATES['git'], utcnow(), 100)
        tasks_2 = tasks_from_template(
            TEMPLATES['hg'], utcnow(), 100,
            num_tasks_priority, priorities=priority_ratio)
        tasks = tasks_1 + tasks_2

        # tasks are returned only once with their ids
        ret1 = swh_scheduler.create_tasks(tasks + tasks_1 + tasks_2)
        set_ret1 = set([t['id'] for t in ret1])

        # creating the same set result in the same ids
        ret = swh_scheduler.create_tasks(tasks)
        set_ret = set([t['id'] for t in ret])

        # Idempotence results
        assert set_ret == set_ret1
        assert len(ret) == len(ret1)

        ids = set()
        actual_priorities = defaultdict(int)

        for task, orig_task in zip(ret, tasks):
            task = copy.deepcopy(task)
            task_type = TASK_TYPES[orig_task['type'].split('-')[-1]]
            assert task['id'] not in ids
            assert task['status'] == 'next_run_not_scheduled'
            assert task['current_interval'] == task_type['default_interval']
            assert task['policy'] == orig_task.get('policy', 'recurring')
            priority = task.get('priority')
            if priority:
                actual_priorities[priority] += 1

            assert task['retries_left'] == (task_type['num_retries'] or 0)
            ids.add(task['id'])
            del task['id']
            del task['status']
            del task['current_interval']
            del task['retries_left']
            if 'policy' not in orig_task:
                del task['policy']
            if 'priority' not in orig_task:
                del task['priority']
                assert task == orig_task

        assert dict(actual_priorities) == {
            priority: int(ratio * num_tasks_priority)
            for priority, ratio in priority_ratio.items()
        }

    def test_peek_ready_tasks_no_priority(self, swh_scheduler):
        self._create_task_types(swh_scheduler)
        t = utcnow()
        task_type = TEMPLATES['git']['type']
        tasks = tasks_from_template(TEMPLATES['git'], t, 100)
        random.shuffle(tasks)
        swh_scheduler.create_tasks(tasks)

        ready_tasks = swh_scheduler.peek_ready_tasks(task_type)
        assert len(ready_tasks) == len(tasks)
        for i in range(len(ready_tasks) - 1):
            assert ready_tasks[i]['next_run'] <= ready_tasks[i+1]['next_run']

        # Only get the first few ready tasks
        limit = random.randrange(5, 5 + len(tasks)//2)
        ready_tasks_limited = swh_scheduler.peek_ready_tasks(
            task_type, num_tasks=limit)

        assert len(ready_tasks_limited) == limit
        assert ready_tasks_limited == ready_tasks[:limit]

        # Limit by timestamp
        max_ts = tasks[limit-1]['next_run']
        ready_tasks_timestamped = swh_scheduler.peek_ready_tasks(
            task_type, timestamp=max_ts)

        for ready_task in ready_tasks_timestamped:
            assert ready_task['next_run'] <= max_ts

        # Make sure we get proper behavior for the first ready tasks
        assert ready_tasks[:len(ready_tasks_timestamped)] \
            == ready_tasks_timestamped

        # Limit by both
        ready_tasks_both = swh_scheduler.peek_ready_tasks(
            task_type, timestamp=max_ts, num_tasks=limit//3)
        assert len(ready_tasks_both) <= limit//3
        for ready_task in ready_tasks_both:
            assert ready_task['next_run'] <= max_ts
            assert ready_task in ready_tasks[:limit//3]

    def _priority_ratio(self, swh_scheduler):
        return swh_scheduler.get_priority_ratios()

    def test_peek_ready_tasks_mixed_priorities(self, swh_scheduler):
        priority_ratio = self._priority_ratio(swh_scheduler)
        self._create_task_types(swh_scheduler)
        t = utcnow()
        task_type = TEMPLATES['git']['type']
        num_tasks_priority = 100
        num_tasks_no_priority = 100
        # Create tasks with and without priorities
        tasks = tasks_from_template(
            TEMPLATES['git'], t,
            num=num_tasks_no_priority,
            num_priority=num_tasks_priority,
            priorities=priority_ratio)

        random.shuffle(tasks)
        swh_scheduler.create_tasks(tasks)

        # take all available tasks
        ready_tasks = swh_scheduler.peek_ready_tasks(
            task_type)

        assert len(ready_tasks) == len(tasks)
        assert num_tasks_priority + num_tasks_no_priority \
            == len(ready_tasks)

        count_tasks_per_priority = defaultdict(int)
        for task in ready_tasks:
            priority = task.get('priority')
            if priority:
                count_tasks_per_priority[priority] += 1

        assert dict(count_tasks_per_priority) == {
            priority: int(ratio * num_tasks_priority)
            for priority, ratio in priority_ratio.items()
        }

        # Only get some ready tasks
        num_tasks = random.randrange(5, 5 + num_tasks_no_priority//2)
        num_tasks_priority = random.randrange(5, num_tasks_priority//2)
        ready_tasks_limited = swh_scheduler.peek_ready_tasks(
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
            assert (actual_prio == expected_count or
                    actual_prio == expected_count + 1)

        assert count_tasks_per_priority[None] == num_tasks

    def test_grab_ready_tasks(self, swh_scheduler):
        priority_ratio = self._priority_ratio(swh_scheduler)
        self._create_task_types(swh_scheduler)
        t = utcnow()
        task_type = TEMPLATES['git']['type']
        num_tasks_priority = 100
        num_tasks_no_priority = 100
        # Create tasks with and without priorities
        tasks = tasks_from_template(
            TEMPLATES['git'], t,
            num=num_tasks_no_priority,
            num_priority=num_tasks_priority,
            priorities=priority_ratio)
        random.shuffle(tasks)
        swh_scheduler.create_tasks(tasks)

        first_ready_tasks = swh_scheduler.peek_ready_tasks(
            task_type, num_tasks=10, num_tasks_priority=10)
        grabbed_tasks = swh_scheduler.grab_ready_tasks(
            task_type, num_tasks=10, num_tasks_priority=10)

        for peeked, grabbed in zip(first_ready_tasks, grabbed_tasks):
            assert peeked['status'] == 'next_run_not_scheduled'
            del peeked['status']
            assert grabbed['status'] == 'next_run_scheduled'
            del grabbed['status']
            assert peeked == grabbed
            assert peeked['priority'] == grabbed['priority']

    def test_get_tasks(self, swh_scheduler):
        self._create_task_types(swh_scheduler)
        t = utcnow()
        tasks = tasks_from_template(TEMPLATES['git'], t, 100)
        tasks = swh_scheduler.create_tasks(tasks)
        random.shuffle(tasks)
        while len(tasks) > 1:
            length = random.randrange(1, len(tasks))
            cur_tasks = sorted(tasks[:length], key=lambda x: x['id'])
            tasks[:length] = []

            ret = swh_scheduler.get_tasks(task['id'] for task in cur_tasks)
            # result is not guaranteed to be sorted
            ret.sort(key=lambda x: x['id'])
            assert ret == cur_tasks

    def test_search_tasks(self, swh_scheduler):
        def make_real_dicts(l):
            """RealDictRow is not a real dict."""
            return [dict(d.items()) for d in l]
        self._create_task_types(swh_scheduler)
        t = utcnow()
        tasks = tasks_from_template(TEMPLATES['git'], t, 100)
        tasks = swh_scheduler.create_tasks(tasks)
        assert make_real_dicts(swh_scheduler.search_tasks()) \
            == make_real_dicts(tasks)

    def assert_filtered_task_ok(
            self, task: Dict[str, Any],
            after: datetime.datetime,
            before: datetime.datetime) -> None:
        """Ensure filtered tasks have the right expected properties
           (within the range, recurring disabled, etc..)

        """
        started = task['started']
        date = started if started is not None else task['scheduled']
        assert after <= date and date <= before
        if task['task_policy'] == 'oneshot':
            assert task['task_status'] in ['completed', 'disabled']
        if task['task_policy'] == 'recurring':
            assert task['task_status'] in ['disabled']

    def test_filter_task_to_archive(self, swh_scheduler):
        """Filtering only list disabled recurring or completed oneshot tasks

        """
        self._create_task_types(swh_scheduler)
        _time = utcnow()
        recurring = tasks_from_template(TEMPLATES['git'], _time, 12)
        oneshots = tasks_from_template(TEMPLATES['hg'], _time, 12)
        total_tasks = len(recurring) + len(oneshots)

        # simulate scheduling tasks
        pending_tasks = swh_scheduler.create_tasks(recurring + oneshots)
        backend_tasks = [{
            'task': task['id'],
            'backend_id': str(uuid.uuid4()),
            'scheduled': utcnow(),
        } for task in pending_tasks]
        swh_scheduler.mass_schedule_task_runs(backend_tasks)

        # we simulate the task are being done
        _tasks = []
        for task in backend_tasks:
            t = swh_scheduler.end_task_run(
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

        swh_scheduler.disable_tasks(tasks_to_update['recurring'])
        # hack: change the status to something else than completed/disabled
        swh_scheduler.set_status_tasks(
            _task_ids['oneshot'], status='next_run_not_scheduled')
        # complete the tasks to update
        swh_scheduler.set_status_tasks(
            tasks_to_update['oneshot'], status='completed')

        total_tasks_filtered = (status_per_policy['recurring'] +
                                status_per_policy['oneshot'])

        # no pagination scenario

        # retrieve tasks to archive
        after = _time.shift(days=-1)
        after_ts = after.format('YYYY-MM-DD')
        before = utcnow().shift(days=1)
        before_ts = before.format('YYYY-MM-DD')
        tasks_result = swh_scheduler.filter_task_to_archive(
            after_ts=after_ts, before_ts=before_ts, limit=total_tasks)

        tasks_to_archive = tasks_result['tasks']

        assert len(tasks_to_archive) == total_tasks_filtered
        assert tasks_result.get('next_page_token') is None

        actual_filtered_per_status = {'recurring': 0, 'oneshot': 0}
        for task in tasks_to_archive:
            self.assert_filtered_task_ok(task, after, before)
            actual_filtered_per_status[task['task_policy']] += 1

        assert actual_filtered_per_status == status_per_policy

        # pagination scenario

        nb_tasks = 3
        tasks_result = swh_scheduler.filter_task_to_archive(
            after_ts=after_ts, before_ts=before_ts, limit=nb_tasks)

        tasks_to_archive2 = tasks_result['tasks']

        assert len(tasks_to_archive2) == nb_tasks
        next_page_token = tasks_result['next_page_token']
        assert next_page_token is not None

        all_tasks = tasks_to_archive2
        while next_page_token is not None:  # Retrieve paginated results
            tasks_result = swh_scheduler.filter_task_to_archive(
                after_ts=after_ts, before_ts=before_ts, limit=nb_tasks,
                page_token=next_page_token)
            tasks_to_archive2 = tasks_result['tasks']
            assert len(tasks_to_archive2) <= nb_tasks
            all_tasks.extend(tasks_to_archive2)
            next_page_token = tasks_result.get('next_page_token')

        actual_filtered_per_status = {'recurring': 0, 'oneshot': 0}
        for task in all_tasks:
            self.assert_filtered_task_ok(task, after, before)
            actual_filtered_per_status[task['task_policy']] += 1

        assert actual_filtered_per_status == status_per_policy

    def test_delete_archived_tasks(self, swh_scheduler):
        self._create_task_types(swh_scheduler)
        _time = utcnow()
        recurring = tasks_from_template(
            TEMPLATES['git'], _time, 12)
        oneshots = tasks_from_template(
            TEMPLATES['hg'], _time, 12)
        total_tasks = len(recurring) + len(oneshots)
        pending_tasks = swh_scheduler.create_tasks(recurring + oneshots)
        backend_tasks = [{
            'task': task['id'],
            'backend_id': str(uuid.uuid4()),
            'scheduled': utcnow(),
        } for task in pending_tasks]
        swh_scheduler.mass_schedule_task_runs(backend_tasks)

        _tasks = []
        percent = random.randint(0, 100)  # random election removal boundary
        for task in backend_tasks:
            t = swh_scheduler.end_task_run(
                task['backend_id'], status='eventful')
            c = random.randint(0, 100)
            if c <= percent:
                _tasks.append({'task_id': t['task'], 'task_run_id': t['id']})

        swh_scheduler.delete_archived_tasks(_tasks)

        all_tasks = [task['id'] for task in swh_scheduler.search_tasks()]
        tasks_count = len(all_tasks)
        tasks_run_count = len(swh_scheduler.get_task_runs(all_tasks))

        assert tasks_count == total_tasks - len(_tasks)
        assert tasks_run_count == total_tasks - len(_tasks)

    def test_get_task_runs_no_task(self, swh_scheduler):
        '''No task exist in the scheduler's db, get_task_runs() should always return an
        empty list.

        '''
        assert not swh_scheduler.get_task_runs(task_ids=())
        assert not swh_scheduler.get_task_runs(task_ids=(1, 2, 3))
        assert not swh_scheduler.get_task_runs(task_ids=(1, 2, 3),
                                               limit=10)

    def test_get_task_runs_no_task_executed(self, swh_scheduler):
        '''No task has been executed yet, get_task_runs() should always return an empty
        list.

        '''
        self._create_task_types(swh_scheduler)
        _time = utcnow()
        recurring = tasks_from_template(
            TEMPLATES['git'], _time, 12)
        oneshots = tasks_from_template(
            TEMPLATES['hg'], _time, 12)
        swh_scheduler.create_tasks(recurring + oneshots)

        assert not swh_scheduler.get_task_runs(task_ids=())
        assert not swh_scheduler.get_task_runs(task_ids=(1, 2, 3))
        assert not swh_scheduler.get_task_runs(task_ids=(1, 2, 3), limit=10)

    def test_get_task_runs_with_scheduled(self, swh_scheduler):
        '''Some tasks have been scheduled but not executed yet, get_task_runs() should
        not return an empty list. limit should behave as expected.

        '''
        self._create_task_types(swh_scheduler)
        _time = utcnow()
        recurring = tasks_from_template(
            TEMPLATES['git'], _time, 12)
        oneshots = tasks_from_template(
            TEMPLATES['hg'], _time, 12)
        total_tasks = len(recurring) + len(oneshots)
        pending_tasks = swh_scheduler.create_tasks(recurring + oneshots)
        backend_tasks = [{
            'task': task['id'],
            'backend_id': str(uuid.uuid4()),
            'scheduled': utcnow(),
        } for task in pending_tasks]
        swh_scheduler.mass_schedule_task_runs(backend_tasks)

        assert not swh_scheduler.get_task_runs(
            task_ids=[total_tasks + 1])

        btask = backend_tasks[0]
        runs = swh_scheduler.get_task_runs(
            task_ids=[btask['task']])
        assert len(runs) == 1
        run = runs[0]

        assert subdict(run, excl=('id',)) == {
            'task': btask['task'],
            'backend_id': btask['backend_id'],
            'scheduled': btask['scheduled'],
            'started': None,
            'ended': None,
            'metadata': None,
            'status': 'scheduled',
        }

        runs = swh_scheduler.get_task_runs(
            task_ids=[bt['task'] for bt in backend_tasks], limit=2)
        assert len(runs) == 2

        runs = swh_scheduler.get_task_runs(
            task_ids=[bt['task'] for bt in backend_tasks])
        assert len(runs) == total_tasks

        keys = ('task', 'backend_id', 'scheduled')
        assert sorted([subdict(x, keys) for x in runs],
                      key=lambda x: x['task']) == backend_tasks

    def test_get_task_runs_with_executed(self, swh_scheduler):
        '''Some tasks have been executed, get_task_runs() should
        not return an empty list. limit should behave as expected.

        '''
        self._create_task_types(swh_scheduler)
        _time = utcnow()
        recurring = tasks_from_template(
            TEMPLATES['git'], _time, 12)
        oneshots = tasks_from_template(
            TEMPLATES['hg'], _time, 12)
        pending_tasks = swh_scheduler.create_tasks(recurring + oneshots)
        backend_tasks = [{
            'task': task['id'],
            'backend_id': str(uuid.uuid4()),
            'scheduled': utcnow(),
        } for task in pending_tasks]
        swh_scheduler.mass_schedule_task_runs(backend_tasks)

        btask = backend_tasks[0]
        ts = utcnow()
        swh_scheduler.start_task_run(btask['backend_id'],
                                     metadata={'something': 'stupid'},
                                     timestamp=ts)
        runs = swh_scheduler.get_task_runs(task_ids=[btask['task']])
        assert len(runs) == 1
        assert subdict(runs[0], excl=('id')) == {
            'task': btask['task'],
            'backend_id': btask['backend_id'],
            'scheduled': btask['scheduled'],
            'started': ts,
            'ended': None,
            'metadata': {'something': 'stupid'},
            'status': 'started',
            }

        ts2 = utcnow()
        swh_scheduler.end_task_run(btask['backend_id'],
                                   metadata={'other': 'stuff'},
                                   timestamp=ts2,
                                   status='eventful')
        runs = swh_scheduler.get_task_runs(task_ids=[btask['task']])
        assert len(runs) == 1
        assert subdict(runs[0], excl=('id')) == {
            'task': btask['task'],
            'backend_id': btask['backend_id'],
            'scheduled': btask['scheduled'],
            'started': ts,
            'ended': ts2,
            'metadata': {'something': 'stupid', 'other': 'stuff'},
            'status': 'eventful',
            }

    def _create_task_types(self, scheduler):
        for tt in TASK_TYPES.values():
            scheduler.create_task_type(tt)
