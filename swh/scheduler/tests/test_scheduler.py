# Copyright (C) 2017-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
import copy
import datetime
import inspect
import random
from typing import Any, Dict, List, Optional, Tuple
import uuid

import attr
import pytest

from swh.model.hashutil import hash_to_bytes
from swh.scheduler.exc import SchedulerException, StaleData, UnknownPolicy
from swh.scheduler.interface import ListedOriginPageToken, SchedulerInterface
from swh.scheduler.model import ListedOrigin, OriginVisitStats, SchedulerMetrics
from swh.scheduler.utils import utcnow

from .common import LISTERS, TASK_TYPES, TEMPLATES, tasks_from_template

ONEDAY = datetime.timedelta(days=1)


def subdict(d, keys=None, excl=()):
    if keys is None:
        keys = [k for k in d.keys()]
    return {k: d[k] for k in keys if k not in excl}


def metrics_sort_key(m: SchedulerMetrics) -> Tuple[uuid.UUID, str]:
    return (m.lister_id, m.visit_type)


def assert_metrics_equal(left, right):
    assert sorted(left, key=metrics_sort_key) == sorted(right, key=metrics_sort_key)


class TestScheduler:
    def test_interface(self, swh_scheduler):
        """Checks all methods of SchedulerInterface are implemented by this
        backend, and that they have the same signature."""
        # Create an instance of the protocol (which cannot be instantiated
        # directly, so this creates a subclass, then instantiates it)
        interface = type("_", (SchedulerInterface,), {})()

        assert "create_task_type" in dir(interface)

        missing_methods = []

        for meth_name in dir(interface):
            if meth_name.startswith("_"):
                continue
            interface_meth = getattr(interface, meth_name)
            try:
                concrete_meth = getattr(swh_scheduler, meth_name)
            except AttributeError:
                if not getattr(interface_meth, "deprecated_endpoint", False):
                    # The backend is missing a (non-deprecated) endpoint
                    missing_methods.append(meth_name)
                continue

            expected_signature = inspect.signature(interface_meth)
            actual_signature = inspect.signature(concrete_meth)

            assert expected_signature == actual_signature, meth_name

        assert missing_methods == []

    def test_get_priority_ratios(self, swh_scheduler):
        assert swh_scheduler.get_priority_ratios() == {
            "high": 0.5,
            "normal": 0.3,
            "low": 0.2,
        }

    def test_add_task_type(self, swh_scheduler):
        tt = TASK_TYPES["git"]
        swh_scheduler.create_task_type(tt)
        assert tt == swh_scheduler.get_task_type(tt["type"])
        tt2 = TASK_TYPES["hg"]
        swh_scheduler.create_task_type(tt2)
        assert tt == swh_scheduler.get_task_type(tt["type"])
        assert tt2 == swh_scheduler.get_task_type(tt2["type"])

    def test_create_task_type_idempotence(self, swh_scheduler):
        tt = TASK_TYPES["git"]
        swh_scheduler.create_task_type(tt)
        swh_scheduler.create_task_type(tt)
        assert tt == swh_scheduler.get_task_type(tt["type"])

    def test_get_task_types(self, swh_scheduler):
        tt, tt2 = TASK_TYPES["git"], TASK_TYPES["hg"]
        swh_scheduler.create_task_type(tt)
        swh_scheduler.create_task_type(tt2)
        actual_task_types = swh_scheduler.get_task_types()
        assert tt in actual_task_types
        assert tt2 in actual_task_types

    def test_create_tasks(self, swh_scheduler):
        priority_ratio = self._priority_ratio(swh_scheduler)
        self._create_task_types(swh_scheduler)
        num_tasks_priority = 100
        tasks_1 = tasks_from_template(TEMPLATES["git"], utcnow(), 100)
        tasks_2 = tasks_from_template(
            TEMPLATES["hg"],
            utcnow(),
            100,
            num_tasks_priority,
            priorities=priority_ratio,
        )
        tasks = tasks_1 + tasks_2

        # tasks are returned only once with their ids
        ret1 = swh_scheduler.create_tasks(tasks + tasks_1 + tasks_2)
        set_ret1 = set([t["id"] for t in ret1])

        # creating the same set result in the same ids
        ret = swh_scheduler.create_tasks(tasks)
        set_ret = set([t["id"] for t in ret])

        # Idempotence results
        assert set_ret == set_ret1
        assert len(ret) == len(ret1)

        ids = set()
        actual_priorities = defaultdict(int)

        for task, orig_task in zip(ret, tasks):
            task = copy.deepcopy(task)
            task_type = TASK_TYPES[orig_task["type"].split("-")[-1]]
            assert task["id"] not in ids
            assert task["status"] == "next_run_not_scheduled"
            assert task["current_interval"] == task_type["default_interval"]
            assert task["policy"] == orig_task.get("policy", "recurring")
            priority = task.get("priority")
            if priority:
                actual_priorities[priority] += 1

            assert task["retries_left"] == (task_type["num_retries"] or 0)
            ids.add(task["id"])
            del task["id"]
            del task["status"]
            del task["current_interval"]
            del task["retries_left"]
            if "policy" not in orig_task:
                del task["policy"]
            if "priority" not in orig_task:
                del task["priority"]
                assert task == orig_task

        assert dict(actual_priorities) == {
            priority: int(ratio * num_tasks_priority)
            for priority, ratio in priority_ratio.items()
        }

    def test_peek_ready_tasks_no_priority(self, swh_scheduler):
        self._create_task_types(swh_scheduler)
        t = utcnow()
        task_type = TEMPLATES["git"]["type"]
        tasks = tasks_from_template(TEMPLATES["git"], t, 100)
        random.shuffle(tasks)
        swh_scheduler.create_tasks(tasks)

        ready_tasks = swh_scheduler.peek_ready_tasks(task_type)
        assert len(ready_tasks) == len(tasks)
        for i in range(len(ready_tasks) - 1):
            assert ready_tasks[i]["next_run"] <= ready_tasks[i + 1]["next_run"]

        # Only get the first few ready tasks
        limit = random.randrange(5, 5 + len(tasks) // 2)
        ready_tasks_limited = swh_scheduler.peek_ready_tasks(task_type, num_tasks=limit)

        assert len(ready_tasks_limited) == limit
        assert ready_tasks_limited == ready_tasks[:limit]

        # Limit by timestamp
        max_ts = tasks[limit - 1]["next_run"]
        ready_tasks_timestamped = swh_scheduler.peek_ready_tasks(
            task_type, timestamp=max_ts
        )

        for ready_task in ready_tasks_timestamped:
            assert ready_task["next_run"] <= max_ts

        # Make sure we get proper behavior for the first ready tasks
        assert ready_tasks[: len(ready_tasks_timestamped)] == ready_tasks_timestamped

        # Limit by both
        ready_tasks_both = swh_scheduler.peek_ready_tasks(
            task_type, timestamp=max_ts, num_tasks=limit // 3
        )
        assert len(ready_tasks_both) <= limit // 3
        for ready_task in ready_tasks_both:
            assert ready_task["next_run"] <= max_ts
            assert ready_task in ready_tasks[: limit // 3]

    def _priority_ratio(self, swh_scheduler):
        return swh_scheduler.get_priority_ratios()

    def test_peek_ready_tasks_mixed_priorities(self, swh_scheduler):
        priority_ratio = self._priority_ratio(swh_scheduler)
        self._create_task_types(swh_scheduler)
        t = utcnow()
        task_type = TEMPLATES["git"]["type"]
        num_tasks_priority = 100
        num_tasks_no_priority = 100
        # Create tasks with and without priorities
        tasks = tasks_from_template(
            TEMPLATES["git"],
            t,
            num=num_tasks_no_priority,
            num_priority=num_tasks_priority,
            priorities=priority_ratio,
        )

        random.shuffle(tasks)
        swh_scheduler.create_tasks(tasks)

        # take all available tasks
        ready_tasks = swh_scheduler.peek_ready_tasks(task_type)

        assert len(ready_tasks) == len(tasks)
        assert num_tasks_priority + num_tasks_no_priority == len(ready_tasks)

        count_tasks_per_priority = defaultdict(int)
        for task in ready_tasks:
            priority = task.get("priority")
            if priority:
                count_tasks_per_priority[priority] += 1

        assert dict(count_tasks_per_priority) == {
            priority: int(ratio * num_tasks_priority)
            for priority, ratio in priority_ratio.items()
        }

        # Only get some ready tasks
        num_tasks = random.randrange(5, 5 + num_tasks_no_priority // 2)
        num_tasks_priority = random.randrange(5, num_tasks_priority // 2)
        ready_tasks_limited = swh_scheduler.peek_ready_tasks(
            task_type, num_tasks=num_tasks, num_tasks_priority=num_tasks_priority
        )

        count_tasks_per_priority = defaultdict(int)
        for task in ready_tasks_limited:
            priority = task.get("priority")
            count_tasks_per_priority[priority] += 1

        import math

        for priority, ratio in priority_ratio.items():
            expected_count = math.ceil(ratio * num_tasks_priority)
            actual_prio = count_tasks_per_priority[priority]
            assert actual_prio == expected_count or actual_prio == expected_count + 1

        assert count_tasks_per_priority[None] == num_tasks

    def test_grab_ready_tasks(self, swh_scheduler):
        priority_ratio = self._priority_ratio(swh_scheduler)
        self._create_task_types(swh_scheduler)
        t = utcnow()
        task_type = TEMPLATES["git"]["type"]
        num_tasks_priority = 100
        num_tasks_no_priority = 100
        # Create tasks with and without priorities
        tasks = tasks_from_template(
            TEMPLATES["git"],
            t,
            num=num_tasks_no_priority,
            num_priority=num_tasks_priority,
            priorities=priority_ratio,
        )
        random.shuffle(tasks)
        swh_scheduler.create_tasks(tasks)

        first_ready_tasks = swh_scheduler.peek_ready_tasks(
            task_type, num_tasks=10, num_tasks_priority=10
        )
        grabbed_tasks = swh_scheduler.grab_ready_tasks(
            task_type, num_tasks=10, num_tasks_priority=10
        )

        for peeked, grabbed in zip(first_ready_tasks, grabbed_tasks):
            assert peeked["status"] == "next_run_not_scheduled"
            del peeked["status"]
            assert grabbed["status"] == "next_run_scheduled"
            del grabbed["status"]
            assert peeked == grabbed
            assert peeked["priority"] == grabbed["priority"]

    def test_get_tasks(self, swh_scheduler):
        self._create_task_types(swh_scheduler)
        t = utcnow()
        tasks = tasks_from_template(TEMPLATES["git"], t, 100)
        tasks = swh_scheduler.create_tasks(tasks)
        random.shuffle(tasks)
        while len(tasks) > 1:
            length = random.randrange(1, len(tasks))
            cur_tasks = sorted(tasks[:length], key=lambda x: x["id"])
            tasks[:length] = []

            ret = swh_scheduler.get_tasks(task["id"] for task in cur_tasks)
            # result is not guaranteed to be sorted
            ret.sort(key=lambda x: x["id"])
            assert ret == cur_tasks

    def test_search_tasks(self, swh_scheduler):
        def make_real_dicts(lst):
            """RealDictRow is not a real dict."""
            return [dict(d.items()) for d in lst]

        self._create_task_types(swh_scheduler)
        t = utcnow()
        tasks = tasks_from_template(TEMPLATES["git"], t, 100)
        tasks = swh_scheduler.create_tasks(tasks)
        assert make_real_dicts(swh_scheduler.search_tasks()) == make_real_dicts(tasks)

    def assert_filtered_task_ok(
        self, task: Dict[str, Any], after: datetime.datetime, before: datetime.datetime
    ) -> None:
        """Ensure filtered tasks have the right expected properties
           (within the range, recurring disabled, etc..)

        """
        started = task["started"]
        date = started if started is not None else task["scheduled"]
        assert after <= date and date <= before
        if task["task_policy"] == "oneshot":
            assert task["task_status"] in ["completed", "disabled"]
        if task["task_policy"] == "recurring":
            assert task["task_status"] in ["disabled"]

    def test_filter_task_to_archive(self, swh_scheduler):
        """Filtering only list disabled recurring or completed oneshot tasks

        """
        self._create_task_types(swh_scheduler)
        _time = utcnow()
        recurring = tasks_from_template(TEMPLATES["git"], _time, 12)
        oneshots = tasks_from_template(TEMPLATES["hg"], _time, 12)
        total_tasks = len(recurring) + len(oneshots)

        # simulate scheduling tasks
        pending_tasks = swh_scheduler.create_tasks(recurring + oneshots)
        backend_tasks = [
            {
                "task": task["id"],
                "backend_id": str(uuid.uuid4()),
                "scheduled": utcnow(),
            }
            for task in pending_tasks
        ]
        swh_scheduler.mass_schedule_task_runs(backend_tasks)

        # we simulate the task are being done
        _tasks = []
        for task in backend_tasks:
            t = swh_scheduler.end_task_run(task["backend_id"], status="eventful")
            _tasks.append(t)

        # Randomly update task's status per policy
        status_per_policy = {"recurring": 0, "oneshot": 0}
        status_choice = {
            # policy: [tuple (1-for-filtering, 'associated-status')]
            "recurring": [
                (1, "disabled"),
                (0, "completed"),
                (0, "next_run_not_scheduled"),
            ],
            "oneshot": [
                (0, "next_run_not_scheduled"),
                (1, "disabled"),
                (1, "completed"),
            ],
        }

        tasks_to_update = defaultdict(list)
        _task_ids = defaultdict(list)
        # randomize 'disabling' recurring task or 'complete' oneshot task
        for task in pending_tasks:
            policy = task["policy"]
            _task_ids[policy].append(task["id"])
            status = random.choice(status_choice[policy])
            if status[0] != 1:
                continue
            # elected for filtering
            status_per_policy[policy] += status[0]
            tasks_to_update[policy].append(task["id"])

        swh_scheduler.disable_tasks(tasks_to_update["recurring"])
        # hack: change the status to something else than completed/disabled
        swh_scheduler.set_status_tasks(
            _task_ids["oneshot"], status="next_run_not_scheduled"
        )
        # complete the tasks to update
        swh_scheduler.set_status_tasks(tasks_to_update["oneshot"], status="completed")

        total_tasks_filtered = (
            status_per_policy["recurring"] + status_per_policy["oneshot"]
        )

        # no pagination scenario

        # retrieve tasks to archive
        after = _time - ONEDAY
        after_ts = after.strftime("%Y-%m-%d")
        before = utcnow() + ONEDAY
        before_ts = before.strftime("%Y-%m-%d")
        tasks_result = swh_scheduler.filter_task_to_archive(
            after_ts=after_ts, before_ts=before_ts, limit=total_tasks
        )

        tasks_to_archive = tasks_result["tasks"]

        assert len(tasks_to_archive) == total_tasks_filtered
        assert tasks_result.get("next_page_token") is None

        actual_filtered_per_status = {"recurring": 0, "oneshot": 0}
        for task in tasks_to_archive:
            self.assert_filtered_task_ok(task, after, before)
            actual_filtered_per_status[task["task_policy"]] += 1

        assert actual_filtered_per_status == status_per_policy

        # pagination scenario

        nb_tasks = 3
        tasks_result = swh_scheduler.filter_task_to_archive(
            after_ts=after_ts, before_ts=before_ts, limit=nb_tasks
        )

        tasks_to_archive2 = tasks_result["tasks"]

        assert len(tasks_to_archive2) == nb_tasks
        next_page_token = tasks_result["next_page_token"]
        assert next_page_token is not None

        all_tasks = tasks_to_archive2
        while next_page_token is not None:  # Retrieve paginated results
            tasks_result = swh_scheduler.filter_task_to_archive(
                after_ts=after_ts,
                before_ts=before_ts,
                limit=nb_tasks,
                page_token=next_page_token,
            )
            tasks_to_archive2 = tasks_result["tasks"]
            assert len(tasks_to_archive2) <= nb_tasks
            all_tasks.extend(tasks_to_archive2)
            next_page_token = tasks_result.get("next_page_token")

        actual_filtered_per_status = {"recurring": 0, "oneshot": 0}
        for task in all_tasks:
            self.assert_filtered_task_ok(task, after, before)
            actual_filtered_per_status[task["task_policy"]] += 1

        assert actual_filtered_per_status == status_per_policy

    def test_delete_archived_tasks(self, swh_scheduler):
        self._create_task_types(swh_scheduler)
        _time = utcnow()
        recurring = tasks_from_template(TEMPLATES["git"], _time, 12)
        oneshots = tasks_from_template(TEMPLATES["hg"], _time, 12)
        total_tasks = len(recurring) + len(oneshots)
        pending_tasks = swh_scheduler.create_tasks(recurring + oneshots)
        backend_tasks = [
            {
                "task": task["id"],
                "backend_id": str(uuid.uuid4()),
                "scheduled": utcnow(),
            }
            for task in pending_tasks
        ]
        swh_scheduler.mass_schedule_task_runs(backend_tasks)

        _tasks = []
        percent = random.randint(0, 100)  # random election removal boundary
        for task in backend_tasks:
            t = swh_scheduler.end_task_run(task["backend_id"], status="eventful")
            c = random.randint(0, 100)
            if c <= percent:
                _tasks.append({"task_id": t["task"], "task_run_id": t["id"]})

        swh_scheduler.delete_archived_tasks(_tasks)

        all_tasks = [task["id"] for task in swh_scheduler.search_tasks()]
        tasks_count = len(all_tasks)
        tasks_run_count = len(swh_scheduler.get_task_runs(all_tasks))

        assert tasks_count == total_tasks - len(_tasks)
        assert tasks_run_count == total_tasks - len(_tasks)

    def test_get_task_runs_no_task(self, swh_scheduler):
        """No task exist in the scheduler's db, get_task_runs() should always return an
        empty list.

        """
        assert not swh_scheduler.get_task_runs(task_ids=())
        assert not swh_scheduler.get_task_runs(task_ids=(1, 2, 3))
        assert not swh_scheduler.get_task_runs(task_ids=(1, 2, 3), limit=10)

    def test_get_task_runs_no_task_executed(self, swh_scheduler):
        """No task has been executed yet, get_task_runs() should always return an empty
        list.

        """
        self._create_task_types(swh_scheduler)
        _time = utcnow()
        recurring = tasks_from_template(TEMPLATES["git"], _time, 12)
        oneshots = tasks_from_template(TEMPLATES["hg"], _time, 12)
        swh_scheduler.create_tasks(recurring + oneshots)

        assert not swh_scheduler.get_task_runs(task_ids=())
        assert not swh_scheduler.get_task_runs(task_ids=(1, 2, 3))
        assert not swh_scheduler.get_task_runs(task_ids=(1, 2, 3), limit=10)

    def test_get_task_runs_with_scheduled(self, swh_scheduler):
        """Some tasks have been scheduled but not executed yet, get_task_runs() should
        not return an empty list. limit should behave as expected.

        """
        self._create_task_types(swh_scheduler)
        _time = utcnow()
        recurring = tasks_from_template(TEMPLATES["git"], _time, 12)
        oneshots = tasks_from_template(TEMPLATES["hg"], _time, 12)
        total_tasks = len(recurring) + len(oneshots)
        pending_tasks = swh_scheduler.create_tasks(recurring + oneshots)
        backend_tasks = [
            {
                "task": task["id"],
                "backend_id": str(uuid.uuid4()),
                "scheduled": utcnow(),
            }
            for task in pending_tasks
        ]
        swh_scheduler.mass_schedule_task_runs(backend_tasks)

        assert not swh_scheduler.get_task_runs(task_ids=[total_tasks + 1])

        btask = backend_tasks[0]
        runs = swh_scheduler.get_task_runs(task_ids=[btask["task"]])
        assert len(runs) == 1
        run = runs[0]

        assert subdict(run, excl=("id",)) == {
            "task": btask["task"],
            "backend_id": btask["backend_id"],
            "scheduled": btask["scheduled"],
            "started": None,
            "ended": None,
            "metadata": None,
            "status": "scheduled",
        }

        runs = swh_scheduler.get_task_runs(
            task_ids=[bt["task"] for bt in backend_tasks], limit=2
        )
        assert len(runs) == 2

        runs = swh_scheduler.get_task_runs(
            task_ids=[bt["task"] for bt in backend_tasks]
        )
        assert len(runs) == total_tasks

        keys = ("task", "backend_id", "scheduled")
        assert (
            sorted([subdict(x, keys) for x in runs], key=lambda x: x["task"])
            == backend_tasks
        )

    def test_get_task_runs_with_executed(self, swh_scheduler):
        """Some tasks have been executed, get_task_runs() should
        not return an empty list. limit should behave as expected.

        """
        self._create_task_types(swh_scheduler)
        _time = utcnow()
        recurring = tasks_from_template(TEMPLATES["git"], _time, 12)
        oneshots = tasks_from_template(TEMPLATES["hg"], _time, 12)
        pending_tasks = swh_scheduler.create_tasks(recurring + oneshots)
        backend_tasks = [
            {
                "task": task["id"],
                "backend_id": str(uuid.uuid4()),
                "scheduled": utcnow(),
            }
            for task in pending_tasks
        ]
        swh_scheduler.mass_schedule_task_runs(backend_tasks)

        btask = backend_tasks[0]
        ts = utcnow()
        swh_scheduler.start_task_run(
            btask["backend_id"], metadata={"something": "stupid"}, timestamp=ts
        )
        runs = swh_scheduler.get_task_runs(task_ids=[btask["task"]])
        assert len(runs) == 1
        assert subdict(runs[0], excl=("id")) == {
            "task": btask["task"],
            "backend_id": btask["backend_id"],
            "scheduled": btask["scheduled"],
            "started": ts,
            "ended": None,
            "metadata": {"something": "stupid"},
            "status": "started",
        }

        ts2 = utcnow()
        swh_scheduler.end_task_run(
            btask["backend_id"],
            metadata={"other": "stuff"},
            timestamp=ts2,
            status="eventful",
        )
        runs = swh_scheduler.get_task_runs(task_ids=[btask["task"]])
        assert len(runs) == 1
        assert subdict(runs[0], excl=("id")) == {
            "task": btask["task"],
            "backend_id": btask["backend_id"],
            "scheduled": btask["scheduled"],
            "started": ts,
            "ended": ts2,
            "metadata": {"something": "stupid", "other": "stuff"},
            "status": "eventful",
        }

    def test_get_or_create_lister(self, swh_scheduler):
        db_listers = []
        for lister_args in LISTERS:
            db_listers.append(swh_scheduler.get_or_create_lister(**lister_args))

        for lister, lister_args in zip(db_listers, LISTERS):
            assert lister.name == lister_args["name"]
            assert lister.instance_name == lister_args.get("instance_name", "")

            lister_get_again = swh_scheduler.get_or_create_lister(
                lister.name, lister.instance_name
            )

            assert lister == lister_get_again

    def test_get_lister(self, swh_scheduler):
        for lister_args in LISTERS:
            assert swh_scheduler.get_lister(**lister_args) is None

        db_listers = []
        for lister_args in LISTERS:
            db_listers.append(swh_scheduler.get_or_create_lister(**lister_args))

        for lister, lister_args in zip(db_listers, LISTERS):
            lister_get_again = swh_scheduler.get_lister(
                lister.name, lister.instance_name
            )

            assert lister == lister_get_again

    def test_update_lister(self, swh_scheduler, stored_lister):
        lister = attr.evolve(stored_lister, current_state={"updated": "now"})

        updated_lister = swh_scheduler.update_lister(lister)

        assert updated_lister.updated > lister.updated
        assert updated_lister == attr.evolve(lister, updated=updated_lister.updated)

    def test_update_lister_stale(self, swh_scheduler, stored_lister):
        swh_scheduler.update_lister(stored_lister)

        with pytest.raises(StaleData) as exc:
            swh_scheduler.update_lister(stored_lister)
        assert "state not updated" in exc.value.args[0]

    def test_record_listed_origins(self, swh_scheduler, listed_origins):
        ret = swh_scheduler.record_listed_origins(listed_origins)

        assert set(returned.url for returned in ret) == set(
            origin.url for origin in listed_origins
        )

        assert all(origin.first_seen == origin.last_seen for origin in ret)

    def test_record_listed_origins_upsert(self, swh_scheduler, listed_origins):
        # First, insert `cutoff` origins
        cutoff = 100
        assert cutoff < len(listed_origins)

        ret = swh_scheduler.record_listed_origins(listed_origins[:cutoff])
        assert len(ret) == cutoff

        # Then, insert all origins, including the `cutoff` first.
        ret = swh_scheduler.record_listed_origins(listed_origins)

        assert len(ret) == len(listed_origins)

        # Two different "first seen" values
        assert len(set(origin.first_seen for origin in ret)) == 2

        # But a single "last seen" value
        assert len(set(origin.last_seen for origin in ret)) == 1

    def test_get_listed_origins_exact(self, swh_scheduler, listed_origins):
        swh_scheduler.record_listed_origins(listed_origins)

        for i, origin in enumerate(listed_origins):
            ret = swh_scheduler.get_listed_origins(
                lister_id=origin.lister_id, url=origin.url
            )

            assert ret.next_page_token is None
            assert len(ret.results) == 1
            assert ret.results[0].lister_id == origin.lister_id
            assert ret.results[0].url == origin.url

    @pytest.mark.parametrize("num_origins,limit", [(20, 6), (5, 42), (20, 20)])
    def test_get_listed_origins_limit(
        self, swh_scheduler, listed_origins, num_origins, limit
    ) -> None:
        added_origins = sorted(
            listed_origins[:num_origins], key=lambda o: (o.lister_id, o.url)
        )
        swh_scheduler.record_listed_origins(added_origins)

        returned_origins: List[ListedOrigin] = []
        call_count = 0
        next_page_token: Optional[ListedOriginPageToken] = None
        while True:
            call_count += 1
            ret = swh_scheduler.get_listed_origins(
                lister_id=listed_origins[0].lister_id,
                limit=limit,
                page_token=next_page_token,
            )
            returned_origins.extend(ret.results)
            next_page_token = ret.next_page_token
            if next_page_token is None:
                break

        assert call_count == (num_origins // limit) + 1

        assert len(returned_origins) == num_origins
        assert [(origin.lister_id, origin.url) for origin in returned_origins] == [
            (origin.lister_id, origin.url) for origin in added_origins
        ]

    def test_get_listed_origins_all(self, swh_scheduler, listed_origins) -> None:
        swh_scheduler.record_listed_origins(listed_origins)

        ret = swh_scheduler.get_listed_origins(limit=len(listed_origins) + 1)
        assert ret.next_page_token is None
        assert len(ret.results) == len(listed_origins)

    def _grab_next_visits_setup(self, swh_scheduler, listed_origins_by_type):
        """Basic origins setup for scheduling policy tests"""
        visit_type = next(iter(listed_origins_by_type))
        origins = listed_origins_by_type[visit_type][:100]
        assert len(origins) > 0

        recorded_origins = swh_scheduler.record_listed_origins(origins)

        return visit_type, recorded_origins

    def _check_grab_next_visit(self, swh_scheduler, visit_type, policy, expected):
        """Calls grab_next_visits with the passed policy, and check that all
        the origins returned are the expected ones (in the same order), and
        that no extra origins are returned. Also checks the origin visits have
        been marked as scheduled, and are only re-scheduled a week later"""
        assert len(expected) != 0

        before = utcnow()
        ret = swh_scheduler.grab_next_visits(
            visit_type=visit_type,
            # Request one more than expected to check that no extra origin is returned
            count=len(expected) + 1,
            policy=policy,
        )
        after = utcnow()

        assert ret == expected
        visit_stats_list = swh_scheduler.origin_visit_stats_get(
            [(origin.url, origin.visit_type) for origin in expected]
        )
        assert len(visit_stats_list) == len(expected)
        for visit_stats in visit_stats_list:
            # Check that last_scheduled got updated
            assert before <= visit_stats.last_scheduled <= after

        # They should not be scheduled again
        ret = swh_scheduler.grab_next_visits(
            visit_type=visit_type, count=len(expected) + 1, policy=policy,
        )
        assert ret == [], "grab_next_visits returned already-scheduled origins"

        # But a week later, they should
        ret = swh_scheduler.grab_next_visits(
            visit_type=visit_type,
            count=len(expected) + 1,
            policy=policy,
            timestamp=after + datetime.timedelta(days=7),
        )
        # We need to sort them because their 'last_scheduled' field is updated to
        # exactly the same value, so the order is not deterministic
        assert sorted(ret) == sorted(
            expected
        ), "grab_next_visits didn't reschedule visits after a week"

    def test_grab_next_visits_oldest_scheduled_first(
        self, swh_scheduler, listed_origins_by_type,
    ):
        visit_type, origins = self._grab_next_visits_setup(
            swh_scheduler, listed_origins_by_type
        )

        # Give all origins but one a last_scheduled date
        base_date = datetime.datetime(2020, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        visit_stats = [
            OriginVisitStats(
                url=origin.url,
                visit_type=origin.visit_type,
                last_snapshot=None,
                last_eventful=None,
                last_uneventful=None,
                last_failed=None,
                last_notfound=None,
                last_scheduled=base_date - datetime.timedelta(seconds=i),
            )
            for i, origin in enumerate(origins[1:])
        ]
        swh_scheduler.origin_visit_stats_upsert(visit_stats)

        # We expect to retrieve the origin with a NULL last_scheduled
        # as well as those with the oldest values (i.e. the last ones), in order.
        expected = [origins[0]] + origins[1:][::-1]

        self._check_grab_next_visit(
            swh_scheduler,
            visit_type=visit_type,
            policy="oldest_scheduled_first",
            expected=expected,
        )

    def test_grab_next_visits_never_visited_oldest_update_first(
        self, swh_scheduler, listed_origins_by_type,
    ):
        visit_type, origins = self._grab_next_visits_setup(
            swh_scheduler, listed_origins_by_type
        )

        # Update known origins with a `last_update` field that we control
        base_date = datetime.datetime(2020, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        updated_origins = [
            attr.evolve(origin, last_update=base_date - datetime.timedelta(seconds=i))
            for i, origin in enumerate(origins)
        ]
        updated_origins = swh_scheduler.record_listed_origins(updated_origins)

        # We expect to retrieve origins with the oldest update date, that is
        # origins at the end of our updated_origins list.
        expected_origins = sorted(updated_origins, key=lambda o: o.last_update)

        self._check_grab_next_visit(
            swh_scheduler,
            visit_type=visit_type,
            policy="never_visited_oldest_update_first",
            expected=expected_origins,
        )

    def test_grab_next_visits_already_visited_order_by_lag(
        self, swh_scheduler, listed_origins_by_type,
    ):
        visit_type, origins = self._grab_next_visits_setup(
            swh_scheduler, listed_origins_by_type
        )

        # Update known origins with a `last_update` field that we control
        base_date = datetime.datetime(2020, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        updated_origins = [
            attr.evolve(origin, last_update=base_date - datetime.timedelta(seconds=i))
            for i, origin in enumerate(origins)
        ]
        updated_origins = swh_scheduler.record_listed_origins(updated_origins)

        # Update the visit stats with a known visit at a controlled date for
        # half the origins.  Pick the date in the middle of the
        # updated_origins' `last_update` range
        visit_date = updated_origins[len(updated_origins) // 2].last_update
        visited_origins = updated_origins[::2]
        visit_stats = [
            OriginVisitStats(
                url=origin.url,
                visit_type=origin.visit_type,
                last_snapshot=hash_to_bytes("d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"),
                last_eventful=visit_date,
                last_uneventful=None,
                last_failed=None,
                last_notfound=None,
            )
            for origin in visited_origins
        ]
        swh_scheduler.origin_visit_stats_upsert(visit_stats)

        # We expect to retrieve visited origins with the largest lag, but only
        # those which haven't been visited since their last update
        expected_origins = sorted(
            [origin for origin in visited_origins if origin.last_update > visit_date],
            key=lambda o: visit_date - o.last_update,
        )

        self._check_grab_next_visit(
            swh_scheduler,
            visit_type=visit_type,
            policy="already_visited_order_by_lag",
            expected=expected_origins,
        )

    def test_grab_next_visits_underflow(self, swh_scheduler, listed_origins_by_type):
        """Check that grab_next_visits works when there not enough origins in
        the database"""
        visit_type = next(iter(listed_origins_by_type))

        # Only add 5 origins to the database
        origins = listed_origins_by_type[visit_type][:5]
        assert origins

        swh_scheduler.record_listed_origins(origins)

        ret = swh_scheduler.grab_next_visits(
            visit_type, len(origins) + 2, policy="oldest_scheduled_first"
        )

        assert len(ret) == 5

    def test_grab_next_visits_unknown_policy(self, swh_scheduler):
        unknown_policy = "non_existing_policy"
        NUM_RESULTS = 5
        with pytest.raises(UnknownPolicy, match=unknown_policy):
            swh_scheduler.grab_next_visits("type", NUM_RESULTS, policy=unknown_policy)

    def _create_task_types(self, scheduler):
        for tt in TASK_TYPES.values():
            scheduler.create_task_type(tt)

    def test_origin_visit_stats_get_empty(self, swh_scheduler) -> None:
        assert swh_scheduler.origin_visit_stats_get([]) == []

    def test_origin_visit_stats_upsert(self, swh_scheduler) -> None:
        eventful_date = utcnow()
        url = "https://github.com/test"

        visit_stats = OriginVisitStats(
            url=url,
            visit_type="git",
            last_eventful=eventful_date,
            last_uneventful=None,
            last_failed=None,
            last_notfound=None,
        )
        swh_scheduler.origin_visit_stats_upsert([visit_stats])
        swh_scheduler.origin_visit_stats_upsert([visit_stats])

        assert swh_scheduler.origin_visit_stats_get([(url, "git")]) == [visit_stats]
        assert swh_scheduler.origin_visit_stats_get([(url, "svn")]) == []

        uneventful_date = utcnow()
        visit_stats = OriginVisitStats(
            url=url,
            visit_type="git",
            last_eventful=None,
            last_uneventful=uneventful_date,
            last_failed=None,
            last_notfound=None,
        )
        swh_scheduler.origin_visit_stats_upsert([visit_stats])

        uneventful_visits = swh_scheduler.origin_visit_stats_get([(url, "git")])

        expected_visit_stats = OriginVisitStats(
            url=url,
            visit_type="git",
            last_eventful=eventful_date,
            last_uneventful=uneventful_date,
            last_failed=None,
            last_notfound=None,
        )

        assert uneventful_visits == [expected_visit_stats]

        failed_date = utcnow()
        visit_stats = OriginVisitStats(
            url=url,
            visit_type="git",
            last_eventful=None,
            last_uneventful=None,
            last_failed=failed_date,
            last_notfound=None,
        )
        swh_scheduler.origin_visit_stats_upsert([visit_stats])

        failed_visits = swh_scheduler.origin_visit_stats_get([(url, "git")])

        expected_visit_stats = OriginVisitStats(
            url=url,
            visit_type="git",
            last_eventful=eventful_date,
            last_uneventful=uneventful_date,
            last_failed=failed_date,
            last_notfound=None,
        )

        assert failed_visits == [expected_visit_stats]

    def test_origin_visit_stats_upsert_with_snapshot(self, swh_scheduler) -> None:
        eventful_date = utcnow()
        url = "https://github.com/666/test"

        visit_stats = OriginVisitStats(
            url=url,
            visit_type="git",
            last_eventful=eventful_date,
            last_uneventful=None,
            last_failed=None,
            last_notfound=None,
            last_snapshot=hash_to_bytes("d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"),
        )
        swh_scheduler.origin_visit_stats_upsert([visit_stats])

        assert swh_scheduler.origin_visit_stats_get([(url, "git")]) == [visit_stats]
        assert swh_scheduler.origin_visit_stats_get([(url, "svn")]) == []

    def test_origin_visit_stats_upsert_batch(self, swh_scheduler) -> None:
        """Batch upsert is ok"""
        visit_stats = [
            OriginVisitStats(
                url="foo",
                visit_type="git",
                last_eventful=utcnow(),
                last_uneventful=None,
                last_failed=None,
                last_notfound=None,
                last_snapshot=hash_to_bytes("d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"),
            ),
            OriginVisitStats(
                url="bar",
                visit_type="git",
                last_eventful=None,
                last_uneventful=utcnow(),
                last_notfound=None,
                last_failed=None,
                last_snapshot=hash_to_bytes("fffcc0710eb6cf9efd5b920a8453e1e07157bfff"),
            ),
        ]

        swh_scheduler.origin_visit_stats_upsert(visit_stats)

        for visit_stat in swh_scheduler.origin_visit_stats_get(
            [(vs.url, vs.visit_type) for vs in visit_stats]
        ):
            assert visit_stat is not None

    def test_origin_visit_stats_upsert_cardinality_failing(self, swh_scheduler) -> None:
        """Batch upsert does not support altering multiple times the same origin-visit-status

        """
        with pytest.raises(SchedulerException, match="CardinalityViolation"):
            swh_scheduler.origin_visit_stats_upsert(
                [
                    OriginVisitStats(
                        url="foo",
                        visit_type="git",
                        last_eventful=None,
                        last_uneventful=utcnow(),
                        last_notfound=None,
                        last_failed=None,
                        last_snapshot=None,
                    ),
                    OriginVisitStats(
                        url="foo",
                        visit_type="git",
                        last_eventful=None,
                        last_uneventful=utcnow(),
                        last_notfound=None,
                        last_failed=None,
                        last_snapshot=None,
                    ),
                ]
            )

    def test_metrics_origins_known(self, swh_scheduler, listed_origins):
        swh_scheduler.record_listed_origins(listed_origins)

        ret = swh_scheduler.update_metrics()

        assert sum(metric.origins_known for metric in ret) == len(listed_origins)

    def test_metrics_origins_enabled(self, swh_scheduler, listed_origins):
        swh_scheduler.record_listed_origins(listed_origins)
        disabled_origin = attr.evolve(listed_origins[0], enabled=False)
        swh_scheduler.record_listed_origins([disabled_origin])

        ret = swh_scheduler.update_metrics(lister_id=disabled_origin.lister_id)
        for metric in ret:
            if metric.visit_type == disabled_origin.visit_type:
                # We disabled one of these origins
                assert metric.origins_known - metric.origins_enabled == 1
            else:
                # But these are still all enabled
                assert metric.origins_known == metric.origins_enabled

    def test_metrics_origins_never_visited(self, swh_scheduler, listed_origins):
        swh_scheduler.record_listed_origins(listed_origins)

        # Pretend that we've recorded a visit on one origin
        visited_origin = listed_origins[0]
        swh_scheduler.origin_visit_stats_upsert(
            [
                OriginVisitStats(
                    url=visited_origin.url,
                    visit_type=visited_origin.visit_type,
                    last_eventful=utcnow(),
                    last_uneventful=None,
                    last_failed=None,
                    last_notfound=None,
                    last_snapshot=hash_to_bytes(
                        "d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"
                    ),
                ),
            ]
        )

        ret = swh_scheduler.update_metrics(lister_id=visited_origin.lister_id)
        for metric in ret:
            if metric.visit_type == visited_origin.visit_type:
                # We visited one of these origins
                assert metric.origins_known - metric.origins_never_visited == 1
            else:
                # But none of these have been visited
                assert metric.origins_known == metric.origins_never_visited

    def test_metrics_origins_with_pending_changes(self, swh_scheduler, listed_origins):
        swh_scheduler.record_listed_origins(listed_origins)

        # Pretend that we've recorded a visit on one origin, in the past with
        # respect to the "last update" time for the origin
        visited_origin = listed_origins[0]
        assert visited_origin.last_update is not None
        swh_scheduler.origin_visit_stats_upsert(
            [
                OriginVisitStats(
                    url=visited_origin.url,
                    visit_type=visited_origin.visit_type,
                    last_eventful=visited_origin.last_update
                    - datetime.timedelta(days=1),
                    last_uneventful=None,
                    last_failed=None,
                    last_notfound=None,
                    last_snapshot=hash_to_bytes(
                        "d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"
                    ),
                ),
            ]
        )

        ret = swh_scheduler.update_metrics(lister_id=visited_origin.lister_id)
        for metric in ret:
            if metric.visit_type == visited_origin.visit_type:
                # We visited one of these origins, in the past
                assert metric.origins_with_pending_changes == 1
            else:
                # But none of these have been visited
                assert metric.origins_with_pending_changes == 0

    def test_update_metrics_explicit_lister(self, swh_scheduler, listed_origins):
        swh_scheduler.record_listed_origins(listed_origins)

        fake_uuid = uuid.uuid4()
        assert all(fake_uuid != origin.lister_id for origin in listed_origins)

        ret = swh_scheduler.update_metrics(lister_id=fake_uuid)

        assert len(ret) == 0

    def test_update_metrics_explicit_timestamp(self, swh_scheduler, listed_origins):
        swh_scheduler.record_listed_origins(listed_origins)

        ts = datetime.datetime(2020, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)

        ret = swh_scheduler.update_metrics(timestamp=ts)

        assert all(metric.last_update == ts for metric in ret)

    def test_update_metrics_twice(self, swh_scheduler, listed_origins):
        swh_scheduler.record_listed_origins(listed_origins)

        ts = utcnow()
        ret = swh_scheduler.update_metrics(timestamp=ts)
        assert all(metric.last_update == ts for metric in ret)

        second_ts = ts + datetime.timedelta(seconds=1)
        ret = swh_scheduler.update_metrics(timestamp=second_ts)
        assert all(metric.last_update == second_ts for metric in ret)

    def test_get_metrics(self, swh_scheduler, listed_origins):
        swh_scheduler.record_listed_origins(listed_origins)
        updated = swh_scheduler.update_metrics()

        retrieved = swh_scheduler.get_metrics()
        assert_metrics_equal(updated, retrieved)

    def test_get_metrics_by_lister(self, swh_scheduler, listed_origins):
        lister_id = listed_origins[0].lister_id
        assert lister_id is not None

        swh_scheduler.record_listed_origins(listed_origins)
        updated = swh_scheduler.update_metrics()

        retrieved = swh_scheduler.get_metrics(lister_id=lister_id)
        assert len(retrieved) > 0

        assert_metrics_equal(
            [metric for metric in updated if metric.lister_id == lister_id], retrieved
        )

    def test_get_metrics_by_visit_type(self, swh_scheduler, listed_origins):
        visit_type = listed_origins[0].visit_type
        assert visit_type is not None

        swh_scheduler.record_listed_origins(listed_origins)
        updated = swh_scheduler.update_metrics()

        retrieved = swh_scheduler.get_metrics(visit_type=visit_type)
        assert len(retrieved) > 0

        assert_metrics_equal(
            [metric for metric in updated if metric.visit_type == visit_type], retrieved
        )
