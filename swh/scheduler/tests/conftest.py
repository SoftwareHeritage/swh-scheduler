import os
import pytest
import glob
from datetime import timedelta

from swh.core.utils import numfile_sortkey as sortkey
from swh.scheduler import get_scheduler
from swh.scheduler.tests import SQL_DIR

DUMP_FILES = os.path.join(SQL_DIR, '*.sql')

# celery tasks for testing purpose; tasks themselves should be
# in swh/scheduler/tests/celery_tasks.py
TASK_NAMES = ['ping', 'multiping', 'add', 'error']


@pytest.fixture(scope='session')
def celery_enable_logging():
    return True


@pytest.fixture(scope='session')
def celery_includes():
    return [
        'swh.scheduler.tests.tasks',
    ]


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
        'result_serializer': 'msgpack',
        }


# override the celery_session_app fixture to monkeypatch the 'main'
# swh.scheduler.celery_backend.config.app Celery application
# with the test application.
@pytest.fixture(scope='session')
def swh_app(celery_session_app):
    import swh.scheduler.celery_backend.config
    swh.scheduler.celery_backend.config.app = celery_session_app
    yield celery_session_app


@pytest.fixture
def swh_scheduler(request, postgresql_proc, postgresql):
    scheduler_config = {
        'scheduling_db': 'postgresql://{user}@{host}:{port}/{dbname}'.format(
            host=postgresql_proc.host,
            port=postgresql_proc.port,
            user='postgres',
            dbname='tests')
    }

    all_dump_files = sorted(glob.glob(DUMP_FILES), key=sortkey)

    cursor = postgresql.cursor()
    for fname in all_dump_files:
        with open(fname) as fobj:
            cursor.execute(fobj.read())
    postgresql.commit()

    scheduler = get_scheduler('local', scheduler_config)
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
