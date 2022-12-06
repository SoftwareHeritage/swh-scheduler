# Copyright (C) 2017-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import timezone
from unittest.mock import patch
import uuid

from swh.scheduler import model, utils

from .common import LISTERS


@patch("swh.scheduler.utils.datetime")
def test_create_oneshot_task_dict_simple(mock_datetime):
    mock_datetime.now.return_value = "some-date"

    actual_task = utils.create_oneshot_task_dict("some-task-type")

    expected_task = {
        "policy": "oneshot",
        "type": "some-task-type",
        "next_run": "some-date",
        "arguments": {
            "args": [],
            "kwargs": {},
        },
    }

    assert actual_task == expected_task
    mock_datetime.now.assert_called_once_with(tz=timezone.utc)


@patch("swh.scheduler.utils.datetime")
def test_create_oneshot_task_dict_other_call(mock_datetime):
    mock_datetime.now.return_value = "some-other-date"

    actual_task = utils.create_oneshot_task_dict(
        "some-task-type", "arg0", "arg1", priority="high", other_stuff="normal"
    )

    expected_task = {
        "policy": "oneshot",
        "type": "some-task-type",
        "next_run": "some-other-date",
        "arguments": {
            "args": ("arg0", "arg1"),
            "kwargs": {"other_stuff": "normal"},
        },
        "priority": "high",
    }

    assert actual_task == expected_task
    mock_datetime.now.assert_called_once_with(tz=timezone.utc)


@patch("swh.scheduler.utils.datetime")
def test_create_task_dict(mock_datetime):
    mock_datetime.now.return_value = "date"

    actual_task = utils.create_task_dict(
        "task-type",
        "recurring",
        "arg0",
        "arg1",
        priority="low",
        other_stuff="normal",
        retries_left=3,
    )

    expected_task = {
        "policy": "recurring",
        "type": "task-type",
        "next_run": "date",
        "arguments": {
            "args": ("arg0", "arg1"),
            "kwargs": {"other_stuff": "normal"},
        },
        "priority": "low",
        "retries_left": 3,
    }

    assert actual_task == expected_task
    mock_datetime.now.assert_called_once_with(tz=timezone.utc)


def test_create_origin_task_dict():
    lister = model.Lister(**LISTERS[1], id=uuid.uuid4())
    origin = model.ListedOrigin(
        lister_id=lister.id,
        url="http://example.com/",
        visit_type="git",
    )

    task = utils.create_origin_task_dict(origin, lister)
    assert task == {
        "type": "load-git",
        "arguments": {
            "args": [],
            "kwargs": {
                "url": "http://example.com/",
                "lister_name": LISTERS[1]["name"],
                "lister_instance_name": LISTERS[1]["instance_name"],
            },
        },
    }

    loader_args = {"foo": "bar", "baz": {"foo": "bar"}}

    origin_w_args = model.ListedOrigin(
        lister_id=lister.id,
        url="http://example.com/svn/",
        visit_type="svn",
        extra_loader_arguments=loader_args,
    )

    task_w_args = utils.create_origin_task_dict(origin_w_args, lister)
    assert task_w_args == {
        "type": "load-svn",
        "arguments": {
            "args": [],
            "kwargs": {
                "url": "http://example.com/svn/",
                "lister_name": LISTERS[1]["name"],
                "lister_instance_name": LISTERS[1]["instance_name"],
                **loader_args,
            },
        },
    }


def test_create_origin_task_dicts(swh_scheduler):
    listers = []
    for lister_args in LISTERS:
        listers.append(swh_scheduler.get_or_create_lister(**lister_args))

    origin1 = model.ListedOrigin(
        lister_id=listers[0].id,
        url="http://example.com/1",
        visit_type="git",
    )
    origin2 = model.ListedOrigin(
        lister_id=listers[0].id,
        url="http://example.com/2",
        visit_type="git",
    )
    origin3 = model.ListedOrigin(
        lister_id=listers[1].id,
        url="http://example.com/3",
        visit_type="git",
    )

    origins = [origin1, origin2, origin3]

    tasks = utils.create_origin_task_dicts(origins, swh_scheduler)
    assert tasks == [
        {
            "type": "load-git",
            "arguments": {
                "args": [],
                "kwargs": {
                    "url": "http://example.com/1",
                    "lister_name": LISTERS[0]["name"],
                    "lister_instance_name": LISTERS[0]["instance_name"],
                },
            },
        },
        {
            "type": "load-git",
            "arguments": {
                "args": [],
                "kwargs": {
                    "url": "http://example.com/2",
                    "lister_name": LISTERS[0]["name"],
                    "lister_instance_name": LISTERS[0]["instance_name"],
                },
            },
        },
        {
            "type": "load-git",
            "arguments": {
                "args": [],
                "kwargs": {
                    "url": "http://example.com/3",
                    "lister_name": LISTERS[1]["name"],
                    "lister_instance_name": LISTERS[1]["instance_name"],
                },
            },
        },
    ]
