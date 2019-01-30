# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import requests

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
                    'db': 'dbname=%s' % self.TEST_DB_NAME,
                }
            }
        }
        self.app = app  # this will setup the local scheduler...
        super().setUp()
        # accessible through a remote scheduler accessible on the
        # given port
        self.backend = get_scheduler('remote', {'url': self.url()})

    def test_site_map(self):
        sitemap = requests.get(self.url() + 'site-map')
        assert sitemap.headers['Content-Type'] == 'application/json'
        sitemap = sitemap.json()

        rules = set(x['rule'] for x in sitemap)
        # we expect at least these rules
        expected_rules = set('/'+rule for rule in (
            'set_status_tasks', 'create_task_type',
            'get_task_type', 'get_task_types', 'create_tasks', 'disable_tasks',
            'get_tasks', 'search_tasks', 'peek_ready_tasks',
            'grab_ready_tasks', 'schedule_task_run', 'mass_schedule_task_runs',
            'start_task_run', 'end_task_run', 'filter_task_to_archive',
            'delete_archived_tasks'))
        assert rules.issuperset(expected_rules), expected_rules - rules
