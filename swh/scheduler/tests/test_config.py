# Copyright (C) 2021 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.scheduler.celery_backend.config import (
    MAX_NUM_TASKS,
    app,
    get_available_slots,
    route_for_task,
)


@pytest.mark.parametrize("name", ["swh.something", "swh.anything"])
def test_route_for_task_routing(name):
    assert route_for_task(name, [], {}, {}) == {"queue": name}


@pytest.mark.parametrize("name", [None, "foobar"])
def test_route_for_task_no_routing(name):
    assert route_for_task(name, [], {}, {}) is None


def test_get_available_slots_no_max_length():
    actual_num = get_available_slots(app, "anything", None)
    assert actual_num == MAX_NUM_TASKS


def test_get_available_slots_issue_when_reading_queue(mocker):
    mock = mocker.patch("swh.scheduler.celery_backend.config.get_queue_length")
    mock.side_effect = ValueError

    actual_num = get_available_slots(app, "anything", max_length=10)
    assert actual_num == MAX_NUM_TASKS
    assert mock.called


def test_get_available_slots_no_queue_length(mocker):
    mock = mocker.patch("swh.scheduler.celery_backend.config.get_queue_length")
    mock.return_value = None
    actual_num = get_available_slots(app, "anything", max_length=100)
    assert actual_num == MAX_NUM_TASKS
    assert mock.called


def test_get_available_slots_no_more_slots(mocker):
    mock = mocker.patch("swh.scheduler.celery_backend.config.get_queue_length")
    max_length = 100
    queue_length = 9000
    mock.return_value = queue_length
    actual_num = get_available_slots(app, "anything", max_length)
    assert actual_num == 0
    assert mock.called


def test_get_available_slots(mocker):
    mock = mocker.patch("swh.scheduler.celery_backend.config.get_queue_length")
    max_length = 100
    queue_length = 90
    mock.return_value = queue_length
    actual_num = get_available_slots(app, "anything", max_length)
    assert actual_num == max_length - queue_length
    assert mock.called
