# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from celery import current_app as app

from swh.scheduler import task
from .celery_testing import CeleryTestFixture


class Task(CeleryTestFixture, unittest.TestCase):

    def test_not_implemented_task(self):
        class NotImplementedTask(task.Task):
            name = 'NotImplementedTask'

            pass

        app.register_task(NotImplementedTask())

        with self.assertRaises(NotImplementedError):
            NotImplementedTask().run()

    def test_add_task(self):
        class AddTask(task.Task):
            name = 'AddTask'

            def run_task(self, x, y):
                return x + y

        app.register_task(AddTask())

        r = AddTask().apply([2, 3])
        self.assertTrue(r.successful())
        self.assertEqual(r.result, 5)
