# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from swh.scheduler.tests.scheduler_testing import SchedulerTestFixture
from swh.scheduler.task import Task
from swh.scheduler.utils import create_task_dict

task_has_run = False


class SomeTestTask(Task):
    def run(self, *, foo):
        global task_has_run
        assert foo == 'bar'
        task_has_run = True


class FixtureTest(SchedulerTestFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.add_scheduler_task_type(
            'some_test_task_type',
            'swh.scheduler.tests.test_fixtures.SomeTestTask')

    def test_task_run(self):
        self.scheduler.create_tasks([create_task_dict(
            'some_test_task_type',
            'oneshot',
            foo='bar',
            )])

        self.assertEqual(task_has_run, False)
        self.run_ready_tasks()
        self.assertEqual(task_has_run, True)
