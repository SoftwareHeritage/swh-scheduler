# Copyright (C) 2017-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

from .common import TEMPLATES, tasks_from_template


def test_tasks_from_template_no_priority():
    nb_tasks = 3
    template = TEMPLATES["git"]
    next_run = datetime.datetime.utcnow()
    tasks = tasks_from_template(template, next_run, nb_tasks)

    assert len(tasks) == nb_tasks

    for i, t in enumerate(tasks):
        assert t["type"] == template["type"]
        assert t["arguments"] is not None
        assert t.get("policy") is None  # not defined in template
        assert len(t["arguments"]["args"]) == 1
        assert len(t["arguments"]["kwargs"].keys()) == 1
        assert t["next_run"] == next_run - datetime.timedelta(microseconds=i)
        assert t.get("priority") is None


def test_tasks_from_template_priority():
    nb_tasks_no_priority = 3
    nb_tasks_priority = 10
    template = TEMPLATES["hg"]
    priorities = {
        "high": 0.5,
        "normal": 0.3,
        "low": 0.2,
    }

    next_run = datetime.datetime.utcnow()
    tasks = tasks_from_template(
        template,
        next_run,
        nb_tasks_no_priority,
        num_priority=nb_tasks_priority,
        priorities=priorities,
    )

    assert len(tasks) == nb_tasks_no_priority + nb_tasks_priority

    repartition_priority = {k: 0 for k in priorities.keys()}
    for i, t in enumerate(tasks):
        assert t["type"] == template["type"]
        assert t["arguments"] is not None
        assert t["policy"] == template["policy"]
        assert len(t["arguments"]["args"]) == 1
        assert len(t["arguments"]["kwargs"].keys()) == 1
        assert t["next_run"] == next_run - datetime.timedelta(microseconds=i)
        priority = t.get("priority")
        if priority:
            assert priority in priorities
            repartition_priority[priority] += 1

    assert repartition_priority == {
        k: v * nb_tasks_priority for k, v in priorities.items()
    }
