# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import os
import unittest

from nose.tools import istest
import psycopg2

from swh.core.tests.db_testing import SingleDbTestFixture
from swh.scheduler.backend import SchedulerBackend


TEST_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_DATA_DIR = os.path.join(TEST_DIR, '../../../../swh-storage-testdata')


class Scheduler(SingleDbTestFixture, unittest.TestCase):
    TEST_DB_NAME = 'softwareheritage-scheduler-test'
    TEST_DB_DUMP = os.path.join(TEST_DATA_DIR, 'dumps/swh-scheduler.dump')

    def setUp(self):
        super().setUp()
        self.config = {'scheduling_db': 'dbname=' + self.TEST_DB_NAME}
        self.backend = SchedulerBackend(**self.config)

        self.task_type = tt = {
            'type': 'update-git',
            'description': 'Update a git repository',
            'backend_name': 'swh.loader.git.tasks.UpdateGitRepository',
            'default_interval': datetime.timedelta(days=64),
            'min_interval': datetime.timedelta(hours=12),
            'max_interval': datetime.timedelta(days=64),
            'backoff_factor': 2,
        }
        self.task_type2 = tt2 = tt.copy()
        tt2['type'] = 'update-hg'
        tt2['description'] = 'Update a mercurial repository'
        tt2['backend_name'] = 'swh.loader.mercurial.tasks.UpdateHgRepository'

    def tearDown(self):
        self.backend.db.close()
        self.empty_tables()
        super().tearDown()

    def empty_tables(self):
        self.cursor.execute("""SELECT table_name FROM information_schema.tables
                               WHERE table_schema = %s""", ('public',))

        tables = set(table for (table,) in self.cursor.fetchall())

        for table in tables:
            self.cursor.execute('truncate table %s cascade' % table)
        self.conn.commit()

    @istest
    def add_task_type(self):
        tt = self.task_type
        tt2 = self.task_type2
        self.backend.create_task_type(tt)
        self.assertEqual(tt, self.backend.get_task_type(tt['type']))
        with self.assertRaisesRegex(psycopg2.IntegrityError,
                                    '\(type\)=\(%s\)' % tt['type']):
            self.backend.create_task_type(tt)
        self.backend.create_task_type(tt2)
        self.assertEqual(tt, self.backend.get_task_type(tt['type']))
        self.assertEqual(tt2, self.backend.get_task_type(tt2['type']))
