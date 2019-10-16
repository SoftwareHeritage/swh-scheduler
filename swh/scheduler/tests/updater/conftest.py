import pytest
import glob
import os
from arrow import utcnow  # XXX

from swh.core.utils import numfile_sortkey as sortkey
from swh.scheduler.updater.backend import SchedulerUpdaterBackend
from swh.scheduler.tests import SQL_DIR
import swh.scheduler.tests.conftest  # noqa


DUMP_FILES = os.path.join(SQL_DIR, 'updater', '*.sql')


@pytest.fixture
def swh_scheduler_updater(postgresql):
    config = {
        'db': postgresql.dsn,
    }

    all_dump_files = sorted(glob.glob(DUMP_FILES), key=sortkey)

    cursor = postgresql.cursor()
    for fname in all_dump_files:
        with open(fname) as fobj:
            cursor.execute(fobj.read())
    postgresql.commit()

    backend = SchedulerUpdaterBackend(**config)
    return backend


def make_event(event_type, name, origin_type):
    return {
        'type': event_type,
        'repo': {
            'name': name,
        },
        'created_at': utcnow(),
        'origin_type': origin_type,
    }


def make_simple_event(event_type, name, origin_type):
    return {
        'type': event_type,
        'url': 'https://fakeurl/%s' % name,
        'origin_type': origin_type,
        'created_at': utcnow(),
    }


def make_events(events):
    for event_type, repo_name, origin_type in events:
        yield make_event(event_type, repo_name, origin_type)


def make_incomplete_event(event_type, name, origin_type,
                          missing_data_key):
    event = make_event(event_type, name, origin_type)
    del event[missing_data_key]
    return event


def make_incomplete_events(events):
    for event_type, repo_name, origin_type, missing_data_key in events:
        yield make_incomplete_event(event_type, repo_name,
                                    origin_type, missing_data_key)
