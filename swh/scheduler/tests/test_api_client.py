# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest
from flask import url_for

import swh.scheduler.api.server as server
from swh.scheduler.api.client import RemoteScheduler
from swh.scheduler.tests.test_scheduler import TestScheduler  # noqa

# tests are executed using imported class (TestScheduler) using overloaded
# swh_scheduler fixture below


# the Flask app used as server in these tests
@pytest.fixture
def app(swh_db_scheduler):
    server.scheduler = swh_db_scheduler
    yield server.app


# the RPCClient class used as client used in these tests
@pytest.fixture
def swh_rpc_client_class():
    return RemoteScheduler


@pytest.fixture
def swh_scheduler(swh_rpc_client, app):
    yield swh_rpc_client


def test_site_map(flask_app_client):
    sitemap = flask_app_client.get(url_for('site_map'))
    assert sitemap.headers['Content-Type'] == 'application/json'

    rules = set(x['rule'] for x in sitemap.json)
    # we expect at least these rules
    expected_rules = set('/'+rule for rule in (
        'set_status_tasks', 'create_task_type',
        'get_task_type', 'get_task_types', 'create_tasks', 'disable_tasks',
        'get_tasks', 'search_tasks', 'get_task_runs', 'peek_ready_tasks',
        'grab_ready_tasks', 'schedule_task_run', 'mass_schedule_task_runs',
        'start_task_run', 'end_task_run', 'filter_task_to_archive',
        'delete_archived_tasks', 'get_priority_ratios'))
    assert rules == expected_rules
