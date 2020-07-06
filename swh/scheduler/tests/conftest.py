# Copyright (C) 2016-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import pytest
from datetime import datetime, timezone
import pkg_resources
from typing import List

from swh.scheduler.model import ListedOrigin, Lister
from swh.scheduler.tests.common import LISTERS


# make sure we are not fooled by CELERY_ config environment vars
for var in [x for x in os.environ.keys() if x.startswith("CELERY")]:
    os.environ.pop(var)


# test_cli tests depends on a en/C locale, so ensure it
os.environ["LC_ALL"] = "C.UTF-8"


@pytest.fixture(scope="session")
def celery_enable_logging():
    return True


@pytest.fixture(scope="session")
def celery_includes():
    task_modules = [
        "swh.scheduler.tests.tasks",
    ]
    for entrypoint in pkg_resources.iter_entry_points("swh.workers"):
        task_modules.extend(entrypoint.load()().get("task_modules", []))
    return task_modules


@pytest.fixture(scope="session")
def celery_parameters():
    return {
        "task_cls": "swh.scheduler.task:SWHTask",
    }


@pytest.fixture(scope="session")
def celery_config():
    return {
        "accept_content": ["application/x-msgpack", "application/json"],
        "task_serializer": "msgpack",
        "result_serializer": "json",
    }


# use the celery_session_app fixture to monkeypatch the 'main'
# swh.scheduler.celery_backend.config.app Celery application
# with the test application
@pytest.fixture(scope="session")
def swh_app(celery_session_app):
    from swh.scheduler.celery_backend import config

    config.app = celery_session_app
    yield celery_session_app


@pytest.fixture
def stored_lister(swh_scheduler) -> Lister:
    """Store a lister in the scheduler and return its information"""
    return swh_scheduler.get_or_create_lister(**LISTERS[0])


@pytest.fixture
def listed_origins(stored_lister) -> List[ListedOrigin]:
    """Return a (fixed) set of 1000 listed origins"""
    return [
        ListedOrigin(
            lister_id=stored_lister.id,
            url=f"https://example.com/{i:04d}.git",
            visit_type="git",
            last_update=datetime(2020, 6, 15, 16, 0, 0, i, tzinfo=timezone.utc),
        )
        for i in range(1000)
    ]
