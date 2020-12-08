# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import logging
import random
import uuid

from click.testing import CliRunner
import pytest

from swh.scheduler.cli import cli
from swh.scheduler.utils import utcnow

from ..common import TASK_TYPES, TEMPLATES, tasks_from_template

logger = logging.getLogger(__name__)


@pytest.mark.usefixtures("swh_elasticsearch_backend")
def test_cli_archive_tasks(swh_sched, swh_sched_config_file):
    scheduler = swh_sched
    template_git = TEMPLATES["git"]
    template_hg = TEMPLATES["hg"]
    # first initialize scheduler's db (is this still needed?)
    for tt in TASK_TYPES.values():
        scheduler.create_task_type(tt)

    next_run_start = utcnow() - datetime.timedelta(days=1)

    recurring = tasks_from_template(template_git, next_run_start, 100)
    oneshots = tasks_from_template(
        template_hg, next_run_start - datetime.timedelta(days=1), 50
    )

    past_time = next_run_start - datetime.timedelta(days=7)

    all_tasks = recurring + oneshots
    result = scheduler.create_tasks(all_tasks)
    assert len(result) == len(all_tasks)

    # simulate task run
    backend_tasks = [
        {
            "task": task["id"],
            "backend_id": str(uuid.uuid4()),
            "scheduled": next_run_start - datetime.timedelta(minutes=i % 60),
        }
        for i, task in enumerate(result)
    ]
    scheduler.mass_schedule_task_runs(backend_tasks)

    # Disable some tasks
    tasks_to_disable = set()
    for task in result:
        status = random.choice(["disabled", "completed"])
        if status == "disabled":
            tasks_to_disable.add(task["id"])

    scheduler.disable_tasks(tasks_to_disable)

    git_tasks = scheduler.search_tasks(task_type=template_git["type"])
    hg_tasks = scheduler.search_tasks(task_type=template_hg["type"])
    assert len(git_tasks) + len(hg_tasks) == len(all_tasks)

    # Ensure the task_run are in expected state
    task_runs = scheduler.get_task_runs([t["id"] for t in git_tasks + hg_tasks])

    # Same for the tasks
    for t in git_tasks + hg_tasks:
        if t["id"] in tasks_to_disable:
            assert t["status"] == "disabled"

    future_time = next_run_start + datetime.timedelta(days=1)
    for tr in task_runs:
        assert past_time <= tr["scheduled"]
        assert tr["scheduled"] < future_time

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--config-file",
            swh_sched_config_file,
            "task",
            "archive",
            "--after",
            past_time.isoformat(),
            "--before",
            future_time.isoformat(),
            "--cleanup",
        ],
        obj={"log_level": logging.DEBUG,},
    )

    assert result.exit_code == 0, result.output

    # disabled tasks should no longer be in the scheduler
    git_tasks = scheduler.search_tasks(task_type=template_git["type"])
    hg_tasks = scheduler.search_tasks(task_type=template_hg["type"])
    remaining_tasks = git_tasks + hg_tasks
    count_disabled = 0
    for task in remaining_tasks:
        logger.debug(f"task status: {task['status']}")
        if task["status"] == "disabled":
            count_disabled += 1

    assert count_disabled == 0
    assert len(remaining_tasks) == len(all_tasks) - len(tasks_to_disable)
