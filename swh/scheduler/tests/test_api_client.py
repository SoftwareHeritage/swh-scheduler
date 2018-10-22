# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from swh.core.tests.server_testing import ServerTestFixture
from swh.scheduler import get_scheduler
from swh.scheduler.api.server import app
from swh.scheduler.tests.test_scheduler import CommonSchedulerTest


class RemoteSchedulerTest(CommonSchedulerTest, ServerTestFixture,
                          unittest.TestCase):
    """Test the remote scheduler API.

    This class doesn't define any tests as we want identical
    functionality between local and remote scheduler. All the tests are
    therefore defined in CommonSchedulerTest.
    """

    def setUp(self):
        self.config = {
            'scheduler': {
                'cls': 'local',
                'args': {
                    'scheduling_db': 'dbname=%s' % self.TEST_DB_NAME,
                }
            }
        }
        self.app = app  # this will setup the local scheduler...
        super().setUp()
        # accessible through a remote scheduler accessible on the
        # given port
        self.backend = get_scheduler('remote', {'url': self.url()})
