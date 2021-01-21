# Copyright (C) 2017-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import datetime

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


def tasks_from_template(template, max_timestamp, num, num_priority=0, priorities=None):
    """Build tasks from template

    """

    def _task_from_template(template, next_run, priority, *args, **kwargs):
        ret = copy.deepcopy(template)
        ret["next_run"] = next_run
        if priority:
            ret["priority"] = priority
        if args:
            ret["arguments"]["args"] = list(args)
        if kwargs:
            ret["arguments"]["kwargs"] = kwargs
        return ret

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


LISTERS = (
    {"name": "github"},
    {"name": "gitlab", "instance_name": "gitlab"},
    {"name": "gitlab", "instance_name": "freedesktop"},
    {"name": "npm"},
    {"name": "pypi"},
)
