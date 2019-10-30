# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from glob import glob

import pytest
from pytest_postgresql.factories import postgresql as pg_fixture_factory

from os.path import join
from swh.core.utils import numfile_sortkey as sortkey
from swh.scheduler.tests import SQL_DIR
from swh.scheduler.updater.events import LISTENED_EVENTS, SWHEvent
from swh.scheduler.updater.writer import UpdaterWriter

from .conftest import make_simple_event


pg_scheduler = pg_fixture_factory('postgresql_proc', 'scheduler')
pg_updater = pg_fixture_factory('postgresql_proc', 'updater')


def pg_sched_fact(dbname, sqldir):
    @pytest.fixture
    def pg_scheduler_db(request):
        pg = request.getfixturevalue('pg_%s' % dbname)
        dump_files = sorted(glob(os.path.join(sqldir, '*.sql')),
                            key=sortkey)
        with pg.cursor() as cur:
            for fname in dump_files:
                with open(fname) as fobj:
                    sql = fobj.read().replace('concurrently', '')
                    cur.execute(sql)
            pg.commit()
        yield pg

    return pg_scheduler_db


scheduler_db = pg_sched_fact('scheduler', SQL_DIR)
updater_db = pg_sched_fact('updater', join(SQL_DIR, 'updater'))


@pytest.fixture
def swh_updater_writer(scheduler_db, updater_db):
    config = {
        'scheduler': {
            'cls': 'local',
            'args': {
                'db': scheduler_db.dsn,
            },
        },
        'scheduler_updater': {
            'cls': 'local',
            'args': {
                'db': updater_db.dsn,
                'cache_read_limit': 5,
            },
        },
        'updater_writer': {
            'pause': 0.1,
            'verbose': False,
        },
    }
    return UpdaterWriter(**config)


def test_run_ko(swh_updater_writer):
    """Only git tasks are supported for now, other types are dismissed.

    """
    scheduler = swh_updater_writer.scheduler_backend
    updater = swh_updater_writer.scheduler_updater_backend

    ready_events = [
        SWHEvent(
            make_simple_event(event_type, 'origin-%s' % i,
                              'svn'))
        for i, event_type in enumerate(LISTENED_EVENTS)
    ]

    updater.cache_put(ready_events)
    list(updater.cache_read())

    r = scheduler.peek_ready_tasks('load-git')

    # first read on an empty scheduling db results with nothing in it
    assert not r

    # Read from cache to scheduler db
    swh_updater_writer.run()

    r = scheduler.peek_ready_tasks('load-git')

    # other reads after writes are still empty since it's not supported
    assert not r


def test_run_ok(swh_updater_writer):
    """Only git origin are supported for now

    """
    scheduler = swh_updater_writer.scheduler_backend
    updater = swh_updater_writer.scheduler_updater_backend

    ready_events = [
        SWHEvent(
            make_simple_event(event_type, 'origin-%s' % i, 'git'))
        for i, event_type in enumerate(LISTENED_EVENTS)
    ]

    expected_length = len(ready_events)

    updater.cache_put(ready_events)

    data = list(updater.cache_read())
    assert len(data) == expected_length

    r = scheduler.peek_ready_tasks('load-git')

    # first read on an empty scheduling db results with nothing in it
    assert not r

    # Read from cache to scheduler db
    swh_updater_writer.run()

    # now, we should have scheduling task ready
    r = scheduler.peek_ready_tasks('load-git')

    assert len(r) == expected_length

    # Check the task has been scheduled
    for t in r:
        assert t['type'] == 'load-git'
        assert t['priority'] == 'normal'
        assert t['policy'] == 'oneshot'
        assert t['status'] == 'next_run_not_scheduled'

    # writer has nothing to do now
    swh_updater_writer.run()

    # so no more data in cache
    data = list(updater.cache_read())

    assert not data

    # provided, no runner is ran, still the same amount of scheduling tasks
    r = scheduler.peek_ready_tasks('load-git')

    assert len(r) == expected_length
