import pytest
import glob
import os

from swh.core.utils import numfile_sortkey as sortkey
from swh.scheduler.updater.backend import SchedulerUpdaterBackend
from swh.scheduler.tests import SQL_DIR
import swh.scheduler.tests.conftest  # noqa


DUMP_FILES = os.path.join(SQL_DIR, 'updater', '*.sql')


@pytest.fixture
def swh_scheduler_updater(request, postgresql_proc, postgresql):
    config = {
        'db': 'postgresql://{user}@{host}:{port}/{dbname}'.format(
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

    backend = SchedulerUpdaterBackend(**config)
    return backend
