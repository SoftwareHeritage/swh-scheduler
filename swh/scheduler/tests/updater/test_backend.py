# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import unittest

from arrow import utcnow
from nose.plugins.attrib import attr
from nose.tools import istest
from hypothesis import given
from hypothesis.strategies import sets

from swh.core.tests.db_testing import SingleDbTestFixture
from swh.scheduler.updater.backend import SchedulerUpdaterBackend
from swh.scheduler.updater.events import SWHEvent

from . import from_regex


TEST_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_DATA_DIR = os.path.join(TEST_DIR, '../../../../../swh-storage-testdata')


@attr('db')
class SchedulerUpdaterBackendTest(SingleDbTestFixture, unittest.TestCase):
    TEST_DB_NAME = 'softwareheritage-scheduler-updater-test'
    TEST_DB_DUMP = os.path.join(TEST_DATA_DIR,
                                'dumps/swh-scheduler-updater.dump')

    def setUp(self):
        super().setUp()
        config = {
            'scheduling_updater_db': 'dbname=' + self.TEST_DB_NAME,
            'cache_read_limit': 1000,
        }
        self.backend = SchedulerUpdaterBackend(**config)

    def _empty_tables(self):
        self.cursor.execute(
            """SELECT table_name FROM information_schema.tables
               WHERE table_schema = %s""", ('public', ))
        tables = set(table for (table,) in self.cursor.fetchall())
        for table in tables:
            self.cursor.execute('truncate table %s cascade' % table)
        self.conn.commit()

    def tearDown(self):
        self.backend.close_connection()
        self._empty_tables()
        super().tearDown()

    @istest
    @given(sets(
        from_regex(
            r'^https://somewhere[.]org/[a-z0-9]{5,7}/[a-z0-9]{3,10}$'),
        min_size=10, max_size=15))
    def cache_read(self, urls):
        def gen_events(urls):
            for url in urls:
                yield SWHEvent({
                    'url': url,
                    'type': 'create',
                    'origin_type': 'git',
                })

        self.backend.cache_put(gen_events(urls))
        r = self.backend.cache_read(timestamp=utcnow())

        self.assertNotEqual(r, [])
