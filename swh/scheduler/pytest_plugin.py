# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import timedelta
import glob
import os

from celery.contrib.testing import worker
from celery.contrib.testing.app import TestApp, setup_default_app
import pkg_resources
import pytest
from pytest_postgresql import factories

from swh.core.utils import numfile_sortkey as sortkey
import swh.scheduler
from swh.scheduler import get_scheduler

SQL_DIR = os.path.join(os.path.dirname(swh.scheduler.__file__), "sql")
DUMP_FILES = os.path.join(SQL_DIR, "*.sql")

# celery tasks for testing purpose; tasks themselves should be
# in swh/scheduler/tests/tasks.py
TASK_NAMES = ["ping", "multiping", "add", "error", "echo"]


postgresql_scheduler = factories.postgresql("postgresql_proc", db_name="scheduler")


@pytest.fixture
def swh_scheduler_config(request, postgresql_scheduler):
    scheduler_config = {
        "db": postgresql_scheduler.dsn,
    }

    all_dump_files = sorted(glob.glob(DUMP_FILES), key=sortkey)

    cursor = postgresql_scheduler.cursor()
    for fname in all_dump_files:
        with open(fname) as fobj:
            cursor.execute(fobj.read())
    postgresql_scheduler.commit()

    return scheduler_config


@pytest.fixture
def swh_scheduler(swh_scheduler_config):
    scheduler = get_scheduler("local", **swh_scheduler_config)
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


@pytest.fixture(scope="session")
def swh_scheduler_celery_app():
    """Set up a Celery app as swh.scheduler and swh worker tests would expect it"""
    test_app = TestApp(
        set_as_current=True,
        enable_logging=True,
        task_cls="swh.scheduler.task:SWHTask",
        config={
            "accept_content": ["application/x-msgpack", "application/json"],
            "task_serializer": "msgpack",
            "result_serializer": "json",
        },
    )
    with setup_default_app(test_app, use_trap=False):
        from swh.scheduler.celery_backend import config

        config.app = test_app
        test_app.set_default()
        test_app.set_current()
        yield test_app


@pytest.fixture(scope="session")
def swh_scheduler_celery_includes():
    """List of task modules that should be loaded by the swh_scheduler_celery_worker on
startup."""
    task_modules = ["swh.scheduler.tests.tasks"]

    for entrypoint in pkg_resources.iter_entry_points("swh.workers"):
        task_modules.extend(entrypoint.load()().get("task_modules", []))
    return task_modules


@pytest.fixture(scope="session")
def swh_scheduler_celery_worker(
    swh_scheduler_celery_app, swh_scheduler_celery_includes,
):
    """Spawn a worker"""
    for module in swh_scheduler_celery_includes:
        swh_scheduler_celery_app.loader.import_task_module(module)
    with worker.start_worker(swh_scheduler_celery_app, pool="solo") as w:
        yield w
