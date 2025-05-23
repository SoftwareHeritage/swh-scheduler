# Copyright (C) 2017-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from typing import Dict, Optional

from swh.scheduler.model import TaskPriority
from swh.scheduler.utils import utcnow

from .common import TEMPLATES, tasks_from_template


def test_tasks_from_template_no_priority():
    nb_tasks = 3
    template = TEMPLATES["test-git"]
    next_run = utcnow()
    tasks = tasks_from_template(template, next_run, nb_tasks)

    assert len(tasks) == nb_tasks

    for i, t in enumerate(tasks):
        assert t.type == template.type
        assert t.arguments is not None
        # not defined in template but is recurring by default
        assert t.policy == "recurring"
        assert len(t.arguments.args) == 1
        assert len(t.arguments.kwargs.keys()) == 1
        assert t.next_run == next_run - datetime.timedelta(microseconds=i)
        assert t.priority is None


def test_tasks_from_template_priority():
    template = TEMPLATES["test-hg"]
    num_priorities: Dict[Optional[TaskPriority], int] = {
        None: 3,
        "high": 5,
        "normal": 3,
        "low": 2,
    }

    next_run = datetime.datetime.now(tz=datetime.timezone.utc)
    tasks = tasks_from_template(
        template,
        next_run,
        num_priorities=num_priorities,
    )

    assert len(tasks) == sum(num_priorities.values())

    repartition_priority = {k: 0 for k in num_priorities}
    for i, t in enumerate(tasks):
        assert t.type == template.type
        assert t.arguments is not None
        assert t.policy == template.policy
        assert len(t.arguments.args) == 1
        assert len(t.arguments.kwargs.keys()) == 1
        assert t.next_run == next_run - datetime.timedelta(microseconds=i)
        priority = t.priority
        assert priority in num_priorities
        repartition_priority[priority] += 1

    assert repartition_priority == num_priorities
