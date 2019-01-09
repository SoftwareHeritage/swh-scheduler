import glob
import os.path
import datetime

from celery.result import AsyncResult
from celery.contrib.testing.worker import start_worker
import celery.contrib.testing.tasks  # noqa           
import pytest

from swh.core.tests.db_testing import DbTestFixture, DB_DUMP_TYPES
from swh.core.utils import numfile_sortkey as sortkey

from swh.scheduler import get_scheduler
from swh.scheduler.celery_backend.runner import run_ready_tasks
from swh.scheduler.celery_backend.config import app
from swh.scheduler.tests.celery_testing import CeleryTestFixture

from . import SQL_DIR

DUMP_FILES = os.path.join(SQL_DIR, '*.sql')


@pytest.mark.db
class SchedulerTestFixture(CeleryTestFixture, DbTestFixture):
    """Base class for test case classes, providing an SWH scheduler as
    the `scheduler` attribute."""
    SCHEDULER_DB_NAME = 'softwareheritage-scheduler-test-fixture'

    def add_scheduler_task_type(self, task_type, backend_name,
                                task_class=None):
        task_type = {
            'type': task_type,
            'description': 'Update a git repository',
            'backend_name': backend_name,
            'default_interval': datetime.timedelta(days=64),
            'min_interval': datetime.timedelta(hours=12),
            'max_interval': datetime.timedelta(days=64),
            'backoff_factor': 2,
            'max_queue_length': None,
            'num_retries': 7,
            'retry_delay': datetime.timedelta(hours=2),
        }
        self.scheduler.create_task_type(task_type)
        if task_class:
            app.register_task_class(backend_name, task_class)

    def run_ready_tasks(self):
        """Runs the scheduler and a Celery worker, then blocks until
        all tasks are completed."""

        # Make sure the worker is listening to all task-specific queues
        for task in self.scheduler.get_task_types():
            app.amqp.queues.select_add(task['backend_name'])

        with start_worker(app):
            backend_tasks = run_ready_tasks(self.scheduler, app)
            for task in backend_tasks:
                # Make sure the task completed
                AsyncResult(id=task['backend_id']).get()

    @classmethod
    def setUpClass(cls):
        all_dump_files = sorted(glob.glob(DUMP_FILES), key=sortkey)

        all_dump_files = [(x, DB_DUMP_TYPES[os.path.splitext(x)[1]])
                          for x in all_dump_files]

        cls.add_db(name=cls.SCHEDULER_DB_NAME,
                   dumps=all_dump_files)
        super().setUpClass()

    def setUp(self):
        super().setUp()
        self.scheduler_config = {
                'scheduling_db': 'dbname=' + self.SCHEDULER_DB_NAME}
        self.scheduler = get_scheduler('local', self.scheduler_config)

    def tearDown(self):
        self.scheduler.close_connection()
        super().tearDown()
