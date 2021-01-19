# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from flask import url_for
import pytest

from swh.scheduler.api.client import RemoteScheduler
import swh.scheduler.api.server as server
from swh.scheduler.tests.test_scheduler import TestScheduler  # noqa

# tests are executed using imported class (TestScheduler) using overloaded
# swh_scheduler fixture below


# the Flask app used as server in these tests
@pytest.fixture
def app(swh_db_scheduler):
    assert hasattr(server, "scheduler")
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
    sitemap = flask_app_client.get(url_for("site_map"))
    assert sitemap.headers["Content-Type"] == "application/json"

    rules = set(x["rule"] for x in sitemap.json)
    # we expect at least these rules
    expected_rules = set(
        "/" + rule
        for rule in (
            "lister/get",
            "lister/get_or_create",
            "lister/update",
            "origins/get",
            "origins/grab_next",
            "origins/record",
            "priority_ratios/get",
            "scheduler_metrics/get",
            "scheduler_metrics/update",
            "task/create",
            "task/delete_archived",
            "task/disable",
            "task/filter_for_archive",
            "task/get",
            "task/grab_ready",
            "task/peek_ready",
            "task/search",
            "task/set_status",
            "task_run/end",
            "task_run/get",
            "task_run/schedule",
            "task_run/schedule_one",
            "task_run/start",
            "task_type/create",
            "task_type/get",
            "task_type/get_all",
            "visit_stats/get",
            "visit_stats/upsert",
        )
    )
    assert rules == expected_rules


def test_root(flask_app_client):
    root = flask_app_client.get("/")
    assert root.status_code == 200
    assert b"Software Heritage scheduler RPC server" in root.data
