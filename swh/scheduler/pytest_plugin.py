# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import timedelta
import glob
import os

import pytest

from swh.core.utils import numfile_sortkey as sortkey

import swh.scheduler
from swh.scheduler import get_scheduler


SQL_DIR = os.path.join(os.path.dirname(swh.scheduler.__file__), "sql")
DUMP_FILES = os.path.join(SQL_DIR, "*.sql")

# celery tasks for testing purpose; tasks themselves should be
# in swh/scheduler/tests/tasks.py
TASK_NAMES = ["ping", "multiping", "add", "error", "echo"]


@pytest.fixture
def swh_scheduler_config(request, postgresql):
    scheduler_config = {
        "db": postgresql.dsn,
    }

    all_dump_files = sorted(glob.glob(DUMP_FILES), key=sortkey)

    cursor = postgresql.cursor()
    for fname in all_dump_files:
        with open(fname) as fobj:
            cursor.execute(fobj.read())
    postgresql.commit()

    return scheduler_config


@pytest.fixture
def swh_scheduler(swh_scheduler_config):
    scheduler = get_scheduler("local", swh_scheduler_config)
    for taskname in TASK_NAMES:
        scheduler.create_task_type(
            {
                "type": "swh-test-{}".format(taskname),
                "description": "The {} testing task".format(taskname),
                "backend_name": "swh.scheduler.tests.tasks.{}".format(taskname),
                "default_interval": timedelta(days=1),
                "min_interval": timedelta(hours=6),
                "max_interval": timedelta(days=12),
            }
        )

    return scheduler


# this alias is used to be able to easily instantiate a db-backed Scheduler
# eg. for the RPC client/server test suite.
swh_db_scheduler = swh_scheduler
