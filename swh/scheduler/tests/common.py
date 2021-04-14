# Copyright (C) 2017-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import datetime
from typing import Dict, List

TEMPLATES = {
    "git": {
        "type": "load-git",
        "arguments": {"args": [], "kwargs": {},},
        "next_run": None,
    },
    "hg": {
        "type": "load-hg",
        "arguments": {"args": [], "kwargs": {},},
        "next_run": None,
        "policy": "oneshot",
    },
}


TASK_TYPES = {
    "git": {
        "type": "load-git",
        "description": "Update a git repository",
        "backend_name": "swh.loader.git.tasks.UpdateGitRepository",
        "default_interval": datetime.timedelta(days=64),
        "min_interval": datetime.timedelta(hours=12),
        "max_interval": datetime.timedelta(days=64),
        "backoff_factor": 2,
        "max_queue_length": None,
        "num_retries": 7,
        "retry_delay": datetime.timedelta(hours=2),
    },
    "hg": {
        "type": "load-hg",
        "description": "Update a mercurial repository",
        "backend_name": "swh.loader.mercurial.tasks.UpdateHgRepository",
        "default_interval": datetime.timedelta(days=64),
        "min_interval": datetime.timedelta(hours=12),
        "max_interval": datetime.timedelta(days=64),
        "backoff_factor": 2,
        "max_queue_length": None,
        "num_retries": 7,
        "retry_delay": datetime.timedelta(hours=2),
    },
}


def _task_from_template(
    template: Dict, next_run: datetime.datetime, priority: str, *args, **kwargs
) -> Dict:
    ret = copy.deepcopy(template)
    ret["next_run"] = next_run
    if priority:
        ret["priority"] = priority
    if args:
        ret["arguments"]["args"] = list(args)
    if kwargs:
        ret["arguments"]["kwargs"] = kwargs
    return ret


def tasks_from_template(
    template: Dict,
    max_timestamp: datetime.datetime,
    num: int,
    num_priority: int = 0,
    priorities: Dict = {},
) -> List[Dict]:
    """Build tasks from template

    """

    def _pop_priority(priorities):
        if not priorities:
            return None
        for priority, remains in priorities.items():
            if remains > 0:
                priorities[priority] = remains - 1
                return priority
        return None

    if num_priority and priorities:
        priorities = {
            priority: ratio * num_priority for priority, ratio in priorities.items()
        }

    tasks = []
    for i in range(num + num_priority):
        priority = _pop_priority(priorities)
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
    template: Dict, max_timestamp: datetime.datetime, num: int, priority: str
) -> List[Dict]:
    """Build tasks with priority from template

    """
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
    {"name": "github"},
    {"name": "gitlab", "instance_name": "gitlab"},
    {"name": "gitlab", "instance_name": "freedesktop"},
    {"name": "npm"},
    {"name": "pypi"},
)
