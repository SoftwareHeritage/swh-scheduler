# Copyright (C) 2017-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import datetime
from typing import Any, Dict, List, Optional

from swh.scheduler.model import Task, TaskArguments, TaskPriority, TaskType
from swh.scheduler.utils import utcnow

TEMPLATES = {
    "test-git": Task(
        type="load-test-git",
        arguments=TaskArguments(),
        next_run=utcnow(),
    ),
    "test-hg": Task(
        type="load-test-hg",
        arguments=TaskArguments(),
        next_run=utcnow(),
        policy="oneshot",
    ),
}


TASK_TYPES = {
    "test-git": TaskType(
        type="load-test-git",
        description="Update a git repository",
        backend_name="swh.loader.git.tasks.UpdateGitRepository",
        default_interval=datetime.timedelta(days=64),
        min_interval=datetime.timedelta(hours=12),
        max_interval=datetime.timedelta(days=64),
        backoff_factor=2.0,
        max_queue_length=None,
        num_retries=7,
        retry_delay=datetime.timedelta(hours=2),
    ),
    "test-hg": TaskType(
        type="load-test-hg",
        description="Update a mercurial repository",
        backend_name="swh.loader.mercurial.tasks.UpdateHgRepository",
        default_interval=datetime.timedelta(days=64),
        min_interval=datetime.timedelta(hours=12),
        max_interval=datetime.timedelta(days=64),
        backoff_factor=2.0,
        max_queue_length=None,
        num_retries=7,
        retry_delay=datetime.timedelta(hours=2),
    ),
}


def _task_from_template(
    template: Task,
    next_run: datetime.datetime,
    priority: Optional[TaskPriority],
    *args,
    **kwargs,
) -> Task:
    fields_to_update: Dict[str, Any] = {"next_run": next_run}
    if priority:
        fields_to_update["priority"] = priority
    if args or kwargs:
        fields_to_update["arguments"] = TaskArguments(
            args=list(args) or [], kwargs=kwargs or {}
        )
    return template.evolve(**fields_to_update)


def tasks_from_template(
    template: Task,
    max_timestamp: datetime.datetime,
    num: Optional[int] = None,
    priority: Optional[TaskPriority] = None,
    num_priorities: Dict[Optional[TaskPriority], int] = {},
) -> List[Task]:
    """Build ``num`` tasks from template"""
    assert bool(num) != bool(num_priorities), "mutually exclusive"
    if not num_priorities:
        assert num is not None  # to please mypy
        num_priorities = {None: num}
    tasks: List[Task] = []
    for priority, num in num_priorities.items():
        for _ in range(num):
            i = len(tasks)
            tasks.append(
                _task_from_template(
                    template,
                    max_timestamp - datetime.timedelta(microseconds=i),
                    priority,
                    "argument-%03d" % i,
                    **{"kwarg%03d" % i: "bogus-kwarg"},
                )
            )
    return tasks


def tasks_with_priority_from_template(
    template: Task, max_timestamp: datetime.datetime, num: int, priority: TaskPriority
) -> List[Task]:
    """Build tasks with priority from template"""
    return [
        _task_from_template(
            template,
            max_timestamp - datetime.timedelta(microseconds=i),
            priority,
            "argument-%03d" % i,
            **{"kwarg%03d" % i: "bogus-kwarg"},
        )
        for i in range(num)
    ]


LISTERS = (
    {"name": "github", "instance_name": "github"},
    {"name": "gitlab", "instance_name": "gitlab"},
    {"name": "gitlab", "instance_name": "freedesktop"},
    {"name": "npm"},
    {"name": "pypi"},
)
