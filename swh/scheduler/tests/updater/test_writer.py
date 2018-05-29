# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import unittest


from nose.plugins.attrib import attr
from nose.tools import istest

from swh.core.tests.db_testing import DbTestFixture
from swh.scheduler.updater.events import SWHEvent
from swh.scheduler.updater.writer import UpdaterWriter
from swh.scheduler.updater.events import LISTENED_EVENTS

from . import UpdaterTestUtil


TEST_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_DATA_DIR = os.path.join(TEST_DIR, '../../../../../swh-storage-testdata')


@attr('db')
class CommonSchedulerTest(DbTestFixture):
    TEST_SCHED_DB = 'softwareheritage-scheduler-test'
    TEST_SCHED_DUMP = os.path.join(TEST_DATA_DIR,
                                   'dumps/swh-scheduler.dump')

    TEST_SCHED_UPDATER_DB = 'softwareheritage-scheduler-updater-test'
    TEST_SCHED_UPDATER_DUMP = os.path.join(TEST_DATA_DIR,
                                           'dumps/swh-scheduler-updater.dump')

    @classmethod
    def setUpClass(cls):
        cls.add_db(cls.TEST_SCHED_DB, cls.TEST_SCHED_DUMP)
        cls.add_db(cls.TEST_SCHED_UPDATER_DB, cls.TEST_SCHED_UPDATER_DUMP)
        super().setUpClass()

    def tearDown(self):
        self.reset_db_tables(self.TEST_SCHED_UPDATER_DB)
        self.reset_db_tables(self.TEST_SCHED_DB,
                             excluded=['task_type', 'priority_ratio'])
        super().tearDown()


class UpdaterWriterTest(UpdaterTestUtil, CommonSchedulerTest,
                        unittest.TestCase):
    def setUp(self):
        super().setUp()

        config = {
            'scheduler': {
                'cls': 'local',
                'args': {
                    'scheduling_db': 'dbname=softwareheritage-scheduler-test',
                },
            },
            'scheduler_updater': {
                'scheduling_updater_db':
                'dbname=softwareheritage-scheduler-updater-test',
                'cache_read_limit': 5,
            },
            'pause': 0.1,
            'verbose': False,
        }
        self.writer = UpdaterWriter(**config)
        self.scheduler_backend = self.writer.scheduler_backend
        self.scheduler_updater_backend = self.writer.scheduler_updater_backend

    def tearDown(self):
        self.scheduler_backend.close_connection()
        self.scheduler_updater_backend.close_connection()
        super().tearDown()

    @istest
    def run_ko(self):
        """Only git tasks are supported for now, other types are dismissed.

        """
        ready_events = [
            SWHEvent(
                self._make_simple_event(event_type, 'origin-%s' % i,
                                        'svn'))
            for i, event_type in enumerate(LISTENED_EVENTS)
        ]

        expected_length = len(ready_events)

        self.scheduler_updater_backend.cache_put(ready_events)
        data = list(self.scheduler_updater_backend.cache_read())
        self.assertEqual(len(data), expected_length)

        r = self.scheduler_backend.peek_ready_tasks(
            'origin-update-git')

        # first read on an empty scheduling db results with nothing in it
        self.assertEqual(len(r), 0)

        # Read from cache to scheduler db
        self.writer.run()

        r = self.scheduler_backend.peek_ready_tasks(
            'origin-update-git')

        # other reads after writes are still empty since it's not supported
        self.assertEqual(len(r), 0)

    @istest
    def run_ok(self):
        """Only git origin are supported for now

        """
        ready_events = [
            SWHEvent(
                self._make_simple_event(event_type, 'origin-%s' % i, 'git'))
            for i, event_type in enumerate(LISTENED_EVENTS)
        ]

        expected_length = len(ready_events)

        self.scheduler_updater_backend.cache_put(ready_events)

        data = list(self.scheduler_updater_backend.cache_read())
        self.assertEqual(len(data), expected_length)

        r = self.scheduler_backend.peek_ready_tasks(
            'origin-update-git')

        # first read on an empty scheduling db results with nothing in it
        self.assertEqual(len(r), 0)

        # Read from cache to scheduler db
        self.writer.run()

        # now, we should have scheduling task ready
        r = self.scheduler_backend.peek_ready_tasks(
            'origin-update-git')

        self.assertEquals(len(r), expected_length)

        # Check the task has been scheduled
        for t in r:
            self.assertEquals(t['type'], 'origin-update-git')
            self.assertEquals(t['priority'], 'normal')
            self.assertEquals(t['policy'], 'oneshot')
            self.assertEquals(t['status'], 'next_run_not_scheduled')

        # writer has nothing to do now
        self.writer.run()

        # so no more data in cache
        data = list(self.scheduler_updater_backend.cache_read())

        self.assertEqual(len(data), 0)

        # provided, no runner is ran, still the same amount of scheduling tasks
        r = self.scheduler_backend.peek_ready_tasks(
            'origin-update-git')

        self.assertEquals(len(r), expected_length)
