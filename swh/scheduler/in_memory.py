# Copyright (C) 2024-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
import datetime
import json
import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple
from uuid import UUID
from uuid import uuid4 as uuid

from swh.scheduler.model import (
    Task,
    TaskPolicy,
    TaskPriority,
    TaskRun,
    TaskRunStatus,
    TaskStatus,
    TaskType,
)
from swh.scheduler.utils import utcnow

from .exc import StaleData, UnknownPolicy
from .interface import ListedOriginPageToken, PaginatedListedOriginList
from .model import ListedOrigin, Lister, OriginVisitStats, SchedulerMetrics

logger = logging.getLogger(__name__)
epoch = datetime.datetime.fromtimestamp(0, datetime.timezone.utc)


class InMemoryScheduler:
    def __init__(self):
        self._task_types = set()
        self._listers = []
        self._listed_origins = []
        self._origin_visit_stats = {}
        self._tasks = []
        self._task_runs = []
        self._visit_scheduler_queue_position = {}
        self._scheduler_metrics = {}

    def create_task_type(self, task_type: TaskType) -> None:
        self._task_types.add(task_type)

    def get_task_type(self, task_type_name: str) -> Optional[TaskType]:
        sel = [tt for tt in self._task_types if tt.type == task_type_name]
        if sel:
            return sel[0]
        return None

    def get_task_types(self) -> List[TaskType]:
        return list(self._task_types)

    def get_listers(
        self,
        with_first_visits_to_schedule: bool = False,
    ) -> List[Lister]:
        """Retrieve information about all listers from the database."""

        listers = self._listers
        if with_first_visits_to_schedule:
            listers = [
                _l
                for _l in listers
                if _l.last_listing_finished_at is not None
                and _l.first_visits_queue_prefix is not None
                and _l.first_visits_scheduled_at is None
            ]
        return listers

    def get_listers_by_id(
        self,
        lister_ids: List[str],
    ) -> List[Lister]:
        return [_l for _l in self._listers if _l.id in lister_ids]

    def get_lister(
        self,
        name: str,
        instance_name: Optional[str] = None,
    ) -> Optional[Lister]:
        if instance_name is None:
            instance_name = ""
        listers = [
            _l
            for _l in self._listers
            if _l.name == name and _l.instance_name == instance_name
        ]
        return listers and listers[0] or None

    def get_or_create_lister(
        self,
        name: str,
        instance_name: Optional[str] = None,
        first_visits_queue_prefix: Optional[str] = None,
    ) -> Lister:
        if instance_name is None:
            instance_name = ""

        if self.get_lister(name, instance_name) is None:
            self._listers.append(
                Lister(
                    id=uuid(),
                    name=name,
                    instance_name=instance_name,
                    first_visits_queue_prefix=first_visits_queue_prefix,
                    updated=utcnow(),
                )
            )
        lister = self.get_lister(name, instance_name)
        assert lister is not None
        return lister

    def update_lister(self, lister: Lister) -> Lister:
        lids = [
            i
            for i, l in enumerate(self._listers)
            if l.id == lister.id and l.updated == lister.updated
        ]
        if lids:
            lid = lids[0]
            del self._listers[lid]
            self._listers.append(lister.evolve(updated=utcnow()))
            return self._listers[-1]
        raise StaleData("Stale data; Lister state not updated")

    def record_listed_origins(
        self,
        listed_origins: Iterable[ListedOrigin],
    ) -> List[ListedOrigin]:

        pk_cols = ListedOrigin.primary_key_columns()

        deduplicated_origins = {
            tuple(getattr(origin, k) for k in pk_cols): origin
            for origin in listed_origins
        }
        all_origins = {
            tuple(getattr(origin, k) for k in pk_cols): origin
            for origin in self._listed_origins
        }

        ret = []
        now = utcnow()
        for pk, o in deduplicated_origins.items():
            if pk in all_origins:
                all_origins[pk] = o.evolve(last_seen=now)
            else:
                all_origins[pk] = o.evolve(last_seen=now, first_seen=now)
            ret.append(all_origins[pk])
        self._listed_origins = list(all_origins.values())
        return ret

    def get_listed_origins(
        self,
        lister_id: Optional[UUID] = None,
        url: Optional[str] = None,
        urls: Optional[List[str]] = None,
        enabled: Optional[bool] = True,
        limit: int = 1000,
        page_token: Optional[ListedOriginPageToken] = None,
    ) -> PaginatedListedOriginList:

        origins = self._listed_origins

        if lister_id:
            origins = [o for o in origins if o.lister_id == lister_id]

        urls_ = []
        if url is not None:
            urls_.append(url)
        elif urls:
            urls_ = urls

        if urls_:
            origins = [o for o in origins if o.url in urls_]

        if enabled is not None:
            origins = [o for o in origins if o.enabled == enabled]

        if page_token is not None:
            origins = [
                o for o in origins if (str(o.lister_id), o.url) > tuple(page_token)
            ]

        origins = origins[:limit]

        if len(origins) == limit:
            page_token = (str(origins[-1].lister_id), origins[-1].url)
        else:
            page_token = None

        return PaginatedListedOriginList(origins, page_token)

    def get_visit_types_for_listed_origins(self, lister: Lister) -> List[str]:
        return list(
            {o.visit_type for o in self._listed_origins if o.lister_id == lister.id}
        )

    def grab_next_visits(
        self,
        visit_type: str,
        count: int,
        policy: str,
        enabled: bool = True,
        lister_uuid: Optional[str] = None,
        lister_name: Optional[str] = None,
        lister_instance_name: Optional[str] = None,
        timestamp: Optional[datetime.datetime] = None,
        absolute_cooldown: Optional[datetime.timedelta] = datetime.timedelta(hours=12),
        scheduled_cooldown: Optional[datetime.timedelta] = datetime.timedelta(days=7),
        failed_cooldown: Optional[datetime.timedelta] = datetime.timedelta(days=14),
        not_found_cooldown: Optional[datetime.timedelta] = datetime.timedelta(days=31),
        tablesample: Optional[float] = None,
    ) -> List[ListedOrigin]:
        if timestamp is None:
            timestamp = utcnow()

        origins = [
            o
            for o in self._listed_origins
            if o.enabled == enabled and o.visit_type == visit_type
        ]
        stats = {
            (ovs.url, ovs.visit_type): ovs
            for ovs in self.origin_visit_stats_get(
                (o.url, o.visit_type) for o in origins
            )
        }
        origins_stats = [(o, stats.get((o.url, o.visit_type))) for o in origins]

        if absolute_cooldown:
            # Don't schedule visits if they've been scheduled since the absolute cooldown
            origins_stats = [
                (o, s)
                for (o, s) in origins_stats
                if s is None
                or s.last_scheduled is None
                or s.last_scheduled < (timestamp - absolute_cooldown)
            ]

        if scheduled_cooldown:
            # Don't re-schedule visits if they're already scheduled but we haven't
            # recorded a result yet, unless they've been scheduled more than a week
            # ago (it probably means we've lost them in flight somewhere).
            origins_stats = [
                (o, s)
                for (o, s) in origins_stats
                if s is None
                or (
                    s.last_scheduled is None
                    or s.last_scheduled
                    < max((timestamp - scheduled_cooldown), s.last_visit or epoch)
                )
            ]

        if failed_cooldown:
            # Don't retry failed origins too often
            origins_stats = [
                (o, s)
                for (o, s) in origins_stats
                if s is None
                or s.last_visit_status is None
                or s.last_visit_status.value != "failed"
                or (
                    s.last_visit is not None
                    and s.last_visit < (timestamp - failed_cooldown)
                )
            ]

        if not_found_cooldown:
            # Don't retry not found origins too often
            origins_stats = [
                (o, s)
                for (o, s) in origins_stats
                if s is None
                or s.last_visit_status is None
                or s.last_visit_status.value != "not_found"
                or (
                    s.last_visit is not None
                    and s.last_visit < (timestamp - not_found_cooldown)
                )
            ]

        if policy == "oldest_scheduled_first":
            origins_stats.sort(
                key=lambda e: e[1]
                and e[1].last_scheduled is not None
                and e[1].last_scheduled.timestamp()
                or -1
            )
        elif policy == "never_visited_oldest_update_first":
            # never visited origins have a NULL last_snapshot
            origins_stats = [
                (o, s)
                for (o, s) in origins_stats
                if (s is None or s.last_snapshot is None) and o.last_update is not None
            ]
            origins_stats.sort(key=lambda e: e[0].last_update)
        elif policy == "already_visited_order_by_lag":
            origins_stats = [
                (o, s)
                for (o, s) in origins_stats
                if s
                and s.last_snapshot is not None
                and o.last_update is not None
                and o.last_update > s.last_successful
            ]
            origins_stats.sort(
                key=lambda e: (
                    e[0].last_update - (e[1] and e[1].last_successful or epoch)
                ),
                reverse=True,
            )
        elif policy == "origins_without_last_update":
            origins_stats = [
                (o, s) for (o, s) in origins_stats if o.last_update is None
            ]
            origins_stats.sort(
                key=lambda e: (
                    (e[1] and e[1].next_visit_queue_position) or -1,
                    e[0].first_seen,
                )
            )
            for o, s in origins_stats:
                self._visit_scheduler_queue_position[o.visit_type] = max(
                    self._visit_scheduler_queue_position.get(o.visit_type, 0),
                    s and s.next_visit_queue_position or 0,
                )
        elif policy == "first_visits_after_listing":
            assert lister_uuid is not None or (
                lister_name is not None and lister_instance_name is not None
            ), "first_visits_after_listing policy requires lister info "
            if lister_uuid is not None:
                listers = self.get_listers_by_id([lister_uuid])
                lister = listers[0] if listers else None
            else:
                assert lister_name is not None
                assert lister_instance_name is not None
                lister = self.get_lister(lister_name, lister_instance_name)
            assert (
                lister is not None
            ), f"Lister with name {lister_name} and instance {lister_instance_name} not found !"
            origins_stats = [
                (o, s)
                for (o, s) in origins_stats
                if s is None
                or s.last_scheduled is None
                or (
                    lister.last_listing_finished_at
                    and s.last_scheduled < lister.last_listing_finished_at
                )
            ]
            origins_stats.sort(
                key=lambda e: e[1] is not None
                and e[1].last_scheduled is not None
                and e[1].last_scheduled.timestamp()
                or -1
            )
        else:
            raise UnknownPolicy(f"Unknown scheduling policy {policy}")

        if lister_uuid:
            origins_stats = [
                (o, s) for (o, s) in origins_stats if o.lister_id == lister_uuid
            ]

        if lister_name:
            listers = [_l.id for _l in self._listers if _l.name == lister_name]
            origins_stats = [
                (o, s) for (o, s) in origins_stats if o.lister_id in listers
            ]

        if lister_instance_name:
            listers = [
                _l.id
                for _l in self._listers
                if _l.instance_name == lister_instance_name
            ]
            origins_stats = [
                (o, s) for (o, s) in origins_stats if o.lister_id in listers
            ]

        ovs = [
            OriginVisitStats(
                url=o.url,
                visit_type=o.visit_type,
                last_scheduled=(
                    s and s.last_scheduled and max(s.last_scheduled, timestamp)
                )
                or timestamp,
            )
            for (o, s) in origins_stats
        ]
        self.origin_visit_stats_upsert(ovs)

        return [o for (o, _) in origins_stats]

    def create_tasks(
        self,
        tasks: List[Task],
        policy: TaskPolicy = "recurring",
    ) -> List[Task]:
        next_id = 0
        if self._tasks:
            next_id = max(t.id for t in self._tasks) + 1

        _tasks = []
        for t in tasks:
            existing = [
                u
                for u in self._tasks
                if u.type == t.type
                and u.arguments == t.arguments
                and u.policy == t.policy
                and u.priority == t.priority
                and u.status == t.status
                and (u.policy != "oneshot" or u.next_run == t.next_run)
            ]
            if existing:
                assert len(existing) == 1
                if existing[0] not in _tasks:
                    _tasks.append(existing[0])
                continue
            tt = self.get_task_type(t.type)
            assert tt is not None
            t = t.evolve(
                id=next_id,
                policy=t.policy or policy,
                current_interval=t.current_interval or tt.default_interval,
                retries_left=t.retries_left or tt.num_retries,
            )
            self._tasks.append(t)
            _tasks.append(t)
            next_id += 1
        return _tasks

    def set_status_tasks(
        self,
        task_ids: List[int],
        status: TaskStatus = "disabled",
        next_run: Optional[datetime.datetime] = None,
    ) -> None:
        if not task_ids:
            return
        tasks = [t for t in self._tasks if t.id in task_ids]

        updated_tasks = [t.evolve(status=status) for t in tasks]
        if next_run:
            updated_tasks = [t.evolve(next_run=next_run) for t in updated_tasks]
        self._tasks = [t for t in self._tasks if t.id not in task_ids]
        self._tasks.extend(updated_tasks)

    def disable_tasks(self, task_ids: List[int]) -> None:
        self.set_status_tasks(task_ids)

    def search_tasks(
        self,
        task_id: Optional[int] = None,
        task_type: Optional[str] = None,
        status: Optional[TaskStatus] = None,
        priority: Optional[TaskPriority] = None,
        policy: Optional[TaskPolicy] = None,
        before: Optional[datetime.datetime] = None,
        after: Optional[datetime.datetime] = None,
        limit: Optional[int] = None,
    ) -> List[Task]:
        tasks = list(self._tasks)

        if task_id:
            if isinstance(task_id, (str, int)):
                tasks = [t for t in tasks if t.id == task_id]
            else:
                tasks = [t for t in tasks if t.id in task_id]
        if task_type:
            if isinstance(task_type, str):
                tasks = [t for t in tasks if t.type == task_type]
            else:
                tasks = [t for t in tasks if t.type in task_type]
        if status:
            if isinstance(status, str):
                tasks = [t for t in tasks if t.status == status]
            else:
                tasks = [t for t in tasks if t.status in status]
        if priority:
            if isinstance(priority, str):
                tasks = [t for t in tasks if t.priority == priority]
            else:
                tasks = [t for t in tasks if t.priority in priority]
        if policy:
            tasks = [t for t in tasks if t.policy == policy]
        if before:
            tasks = [t for t in tasks if t.next_run <= before]
        if after:
            tasks = [t for t in tasks if t.next_run >= after]

        if limit:
            tasks = tasks[:limit]
        return tasks

    def get_tasks(self, task_ids: List[int]) -> List[Task]:
        ids = list(task_ids)
        return [t for t in self._tasks if t.id in ids]

    def peek_ready_tasks(
        self,
        task_type: str,
        timestamp: Optional[datetime.datetime] = None,
        num_tasks: Optional[int] = None,
    ) -> List[Task]:
        if timestamp is None:
            timestamp = utcnow()
        tasks = list(
            sorted(
                [
                    t
                    for t in self._tasks
                    if t.type == task_type
                    and t.status == "next_run_not_scheduled"
                    and t.priority is None
                    and t.next_run <= timestamp
                ],
                key=lambda t: t.next_run,
            )
        )
        if num_tasks:
            tasks = tasks[:num_tasks]
        return tasks

    def grab_ready_tasks(
        self,
        task_type: str,
        timestamp: Optional[datetime.datetime] = None,
        num_tasks: Optional[int] = None,
    ) -> List[Task]:
        if timestamp is None:
            timestamp = utcnow()
        tasks = self.peek_ready_tasks(task_type, timestamp, num_tasks)
        ids = [t.id for t in tasks]
        updated_tasks = [t.evolve(status="next_run_scheduled") for t in tasks]
        self._tasks = [t for t in self._tasks if t.id not in ids]
        self._tasks.extend(updated_tasks)
        return updated_tasks

    def peek_ready_priority_tasks(
        self,
        task_type: str,
        timestamp: Optional[datetime.datetime] = None,
        num_tasks: Optional[int] = None,
    ) -> List[Task]:
        if timestamp is None:
            timestamp = utcnow()
        tasks = list(
            sorted(
                [
                    t
                    for t in self._tasks
                    if t.type == task_type
                    and t.status == "next_run_not_scheduled"
                    and t.priority is not None
                    and t.next_run <= timestamp
                ],
                key=lambda t: t.next_run,
            )
        )
        if num_tasks:
            tasks = tasks[:num_tasks]
        return tasks

    def grab_ready_priority_tasks(
        self,
        task_type: str,
        timestamp: Optional[datetime.datetime] = None,
        num_tasks: Optional[int] = None,
    ) -> List[Task]:
        if timestamp is None:
            timestamp = utcnow()
        tasks = self.peek_ready_priority_tasks(task_type, timestamp, num_tasks)
        ids = [t.id for t in tasks]
        updated_tasks = [t.evolve(status="next_run_scheduled") for t in tasks]
        self._tasks = [t for t in self._tasks if t.id not in ids]
        self._tasks.extend(updated_tasks)
        return updated_tasks

    def schedule_task_run(
        self,
        task_id: int,
        backend_id: str,
        metadata: Optional[Dict[str, Any]] = None,
        timestamp: Optional[datetime.datetime] = None,
    ) -> TaskRun:
        if metadata is None:
            metadata = {}

        if timestamp is None:
            timestamp = utcnow()
        next_id = 0
        if self._task_runs:
            max(_tr.id for _tr in self._task_runs) + 1
        tr = TaskRun(
            task=task_id,
            id=next_id,
            backend_id=backend_id,
            metadata=metadata,
            scheduled=timestamp,
            status="scheduled",
        )
        self._task_runs.append(tr)
        return tr

    def mass_schedule_task_runs(self, task_runs: List[TaskRun]) -> None:
        task_runs = [tr.evolve(status="scheduled") for tr in task_runs]
        self._task_runs.extend(task_runs)
        # Update the associated tasks' status to next_run_scheduled
        for tr in task_runs:
            task_id = tr.task
            for i, task in enumerate(self._tasks):
                if task.id == task_id:
                    self._tasks[i] = task.evolve(status="next_run_scheduled")

    def start_task_run(
        self,
        backend_id: str,
        metadata: Optional[Dict[str, Any]] = None,
        timestamp: Optional[datetime.datetime] = None,
    ) -> Optional[TaskRun]:
        if metadata is None:
            metadata = {}

        if timestamp is None:
            timestamp = utcnow()

        task_runs = [tr for tr in self._task_runs if tr.backend_id == backend_id]
        updated_task_runs = [
            tr.evolve(
                started=timestamp,
                status="started",
                metadata={**(tr.metadata or {}), **metadata},
            )
            for tr in task_runs
        ]
        self._task_runs = [tr for tr in self._task_runs if tr not in task_runs]
        self._task_runs.extend(updated_task_runs)
        if updated_task_runs:
            return updated_task_runs[0]

        logger.debug(
            "Failed to mark task run %s as started",
            backend_id,
        )
        return None

    def end_task_run(
        self,
        backend_id: str,
        status: TaskRunStatus,
        metadata: Optional[Dict[str, Any]] = None,
        timestamp: Optional[datetime.datetime] = None,
    ) -> Optional[TaskRun]:
        if metadata is None:
            metadata = {}

        if timestamp is None:
            timestamp = utcnow()

        task_runs = [tr for tr in self._task_runs if tr.backend_id == backend_id]
        updated_task_runs = [
            tr.evolve(
                ended=timestamp,
                status=status,
                metadata={**(tr.metadata or {}), **metadata},
            )
            for tr in task_runs
        ]
        self._task_runs = [tr for tr in self._task_runs if tr not in task_runs]
        self._task_runs.extend(updated_task_runs)
        if updated_task_runs:
            return updated_task_runs[0]

        logger.debug(
            "Failed to mark task run %s as ended",
            backend_id,
        )
        return None

    def filter_task_to_archive(
        self,
        after_ts: str,
        before_ts: str,
        limit: int = 10,
        page_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        assert not page_token or isinstance(page_token, str)
        last_id = -1 if page_token is None else int(page_token)
        after_dt = datetime.datetime.fromisoformat(after_ts)
        if after_dt.tzinfo is None:
            after_dt = after_dt.replace(tzinfo=datetime.timezone.utc)
        before_dt = datetime.datetime.fromisoformat(before_ts)
        if before_dt.tzinfo is None:
            before_dt = before_dt.replace(tzinfo=datetime.timezone.utc)
        task_runs = []

        for t in self._tasks:
            if (
                (t.policy == "oneshot" and t.status in ("completed", "disabled"))
                or (t.policy == "recurring" and t.status == "disabled")
            ) and (t.id >= last_id):
                task_runs.extend(
                    [
                        (t, tr)
                        for tr in self._task_runs
                        if (
                            tr.task == t.id
                            and (
                                (
                                    tr.started is not None
                                    and after_dt <= tr.started < before_dt
                                )
                                or (
                                    tr.started is None
                                    and (after_dt <= tr.scheduled < before_dt)
                                )
                            )
                        )
                    ]
                )
        task_runs.sort(key=lambda x: (x[0].id, x[1].started))
        tasks = [
            {
                "task_id": t.id,
                "task_policy": t.policy,
                "task_status": t.status,
                "task_run_id": tr.id,
                "arguments": t.arguments.to_dict(),
                "type": t.type,
                "backend_id": tr.backend_id,
                "metadata": tr.metadata,
                "scheduled": tr.scheduled,
                "started": tr.started,
                "ended": tr.ended,
                "status": tr.status,
            }
            for (t, tr) in task_runs
        ]

        for td in tasks:
            td["arguments"]["args"] = {
                i: v for i, v in enumerate(td["arguments"]["args"])
            }
            kwargs = td["arguments"]["kwargs"]
            td["arguments"]["kwargs"] = json.dumps(kwargs)

        if len(tasks) >= limit + 1:  # remains data, add pagination information
            result = {
                "tasks": tasks[:limit],
                "next_page_token": str(tasks[limit]["task_id"]),
            }
        else:
            result = {"tasks": tasks}

        return result

    def delete_archived_tasks(self, task_ids):
        _task_ids = _task_run_ids = []
        for task_id in task_ids:
            _task_ids.append(task_id["task_id"])
            _task_run_ids.append(task_id["task_run_id"])
        self._task_runs = [tr for tr in self._task_runs if tr.task not in _task_ids]
        self._tasks = [t for t in self._tasks if t.id not in _task_ids]

    def get_task_runs(
        self,
        task_ids: List[int],
        limit: Optional[int] = None,
    ) -> List[TaskRun]:
        if task_ids:
            ret = [tr for tr in self._task_runs if tr.task in task_ids]
            if limit:
                ret = ret[:limit]
            return ret
        else:
            return []

    def origin_visit_stats_upsert(
        self,
        origin_visit_stats: Iterable[OriginVisitStats],
    ) -> None:
        # remove exact duplicates
        ovs = []
        for o in origin_visit_stats:
            if o not in ovs:
                ovs.append(o)

        for o in ovs:
            key = o.url, o.visit_type
            if key not in self._origin_visit_stats:
                self._origin_visit_stats[key] = o
            else:
                _o = self._origin_visit_stats[key]
                _o = _o.evolve(
                    last_scheduled=o.last_scheduled or _o.last_scheduled,
                    last_snapshot=o.last_snapshot or _o.last_snapshot,
                    last_successful=o.last_successful or _o.last_successful,
                    last_visit=o.last_visit or _o.last_visit,
                    last_visit_status=o.last_visit_status or _o.last_visit_status,
                    next_visit_queue_position=o.next_visit_queue_position
                    or _o.next_visit_queue_position,
                    next_position_offset=o.next_position_offset
                    or _o.next_position_offset,
                    successive_visits=o.successive_visits or _o.successive_visits,
                )
                self._origin_visit_stats[key] = _o

    def origin_visit_stats_get(
        self,
        ids: Iterable[Tuple[str, str]],
    ) -> List[OriginVisitStats]:
        if not ids:
            return []
        return [
            self._origin_visit_stats[key]
            for key in ids
            if key in self._origin_visit_stats
        ]

    def visit_scheduler_queue_position_get(self) -> Dict[str, int]:
        return self._visit_scheduler_queue_position.copy()

    def visit_scheduler_queue_position_set(
        self,
        visit_type: str,
        position: int,
    ) -> None:
        self._visit_scheduler_queue_position[visit_type] = position

    def update_metrics(
        self,
        lister_id: Optional[UUID] = None,
        timestamp: Optional[datetime.datetime] = None,
    ) -> List[SchedulerMetrics]:
        if timestamp is None:
            timestamp = utcnow()
        origins = self._listed_origins
        if lister_id:
            origins = [lo for lo in origins if lo.lister_id == lister_id]

        rows = []
        keys = []
        for lo in origins:
            keys.append((lo.lister_id, lo.visit_type))
            ovs = self._origin_visit_stats.get((lo.url, lo.visit_type), None)
            rows.append(
                (
                    lo.lister_id,
                    lo.visit_type,
                    lo.url,
                    lo.enabled,
                    ovs and ovs.last_snapshot,
                    lo.last_update,
                    ovs and ovs.last_successful,
                )
            )
        metrics: Dict[Tuple[UUID, str], SchedulerMetrics] = {}
        for lister_id, visit_type in keys:
            _rows = [
                row[2:] for row in rows if row[0] == lister_id and row[1] == visit_type
            ]
            origins_known = len(_rows)
            origins_enabled = len([row for row in _rows if row[1]])
            origins_never_visited = len(
                [row for row in _rows if row[1] and row[2] is None]
            )
            origins_with_pending_changes = len(
                [
                    row
                    for row in _rows
                    if row[1]
                    and (row[4] is not None and row[3] is not None and row[3] > row[4])
                ]
            )
            assert lister_id is not None
            assert visit_type is not None
            metrics[(lister_id, visit_type)] = SchedulerMetrics(
                lister_id=lister_id,
                visit_type=visit_type,
                last_update=timestamp,
                origins_known=origins_known,
                origins_enabled=origins_enabled,
                origins_never_visited=origins_never_visited,
                origins_with_pending_changes=origins_with_pending_changes,
            )
        self._scheduler_metrics.update(metrics)
        return list(metrics.values())

    def get_metrics(
        self,
        lister_id: Optional[UUID] = None,
        visit_type: Optional[str] = None,
    ) -> List[SchedulerMetrics]:
        sms = list(self._scheduler_metrics.values())
        if lister_id:
            sms = [sm for sm in sms if sm.lister_id == lister_id]
        if visit_type:
            sms = [sm for sm in sms if sm.visit_type == visit_type]
        return sms
