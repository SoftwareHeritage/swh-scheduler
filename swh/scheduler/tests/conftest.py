# Copyright (C) 2016-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import os
import pytest
import glob
from datetime import timedelta
import pkg_resources

from swh.core.utils import numfile_sortkey as sortkey
from swh.scheduler import get_scheduler
from swh.scheduler.tests import SQL_DIR


# make sure we are not fooled by CELERY_ config environment vars
for var in [x for x in os.environ.keys() if x.startswith('CELERY')]:
    os.environ.pop(var)


# test_cli tests depends on a en/C locale, so ensure it
os.environ['LC_ALL'] = 'C.UTF-8'

DUMP_FILES = os.path.join(SQL_DIR, '*.sql')

# celery tasks for testing purpose; tasks themselves should be
# in swh/scheduler/tests/tasks.py
TASK_NAMES = ['ping', 'multiping', 'add', 'error', 'echo']


@pytest.fixture(scope='session')
def celery_enable_logging():
    return True


@pytest.fixture(scope='session')
def celery_includes():
    task_modules = [
        'swh.scheduler.tests.tasks',
    ]
    for entrypoint in pkg_resources.iter_entry_points('swh.workers'):
        task_modules.extend(entrypoint.load()().get('task_modules', []))
    return task_modules


@pytest.fixture(scope='session')
def celery_parameters():
    return {
        'task_cls': 'swh.scheduler.task:SWHTask',
        }


@pytest.fixture(scope='session')
def celery_config():
    return {
        'accept_content': ['application/x-msgpack', 'application/json'],
        'task_serializer': 'msgpack',
        'result_serializer': 'json',
        }


# use the celery_session_app fixture to monkeypatch the 'main'
# swh.scheduler.celery_backend.config.app Celery application
# with the test application
@pytest.fixture(scope='session')
def swh_app(celery_session_app):
    from swh.scheduler.celery_backend import config
    config.app = celery_session_app
    yield celery_session_app


@pytest.fixture
def swh_scheduler_config(request, postgresql):
    scheduler_config = {
        'db': postgresql.dsn,
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
    scheduler = get_scheduler('local', swh_scheduler_config)
    for taskname in TASK_NAMES:
        scheduler.create_task_type({
            'type': 'swh-test-{}'.format(taskname),
            'description': 'The {} testing task'.format(taskname),
            'backend_name': 'swh.scheduler.tests.tasks.{}'.format(taskname),
            'default_interval': timedelta(days=1),
            'min_interval': timedelta(hours=6),
            'max_interval': timedelta(days=12),
        })

    return scheduler


# this alias is used to be able to easily instantiate a db-backed Scheduler
# eg. for the RPC client/server test suite.
swh_db_scheduler = swh_scheduler
