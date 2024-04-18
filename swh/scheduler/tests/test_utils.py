# Copyright (C) 2017-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timedelta, timezone
import uuid

import pytest

from swh.scheduler.model import ListedOrigin, Lister, Task, TaskArguments
from swh.scheduler.utils import (
    create_oneshot_task,
    create_origin_task,
    create_origin_tasks,
    create_task,
    utcnow,
)

from .common import LISTERS


@pytest.fixture
def mock_datetime(mocker):
    now = datetime.now(tz=timezone.utc)
    mock_datetime = mocker.patch("swh.scheduler.utils.datetime")
    mock_datetime.now.return_value = now
    return mock_datetime


def test_create_oneshot_task_simple(mock_datetime):
    actual_task = create_oneshot_task("some-task-type")

    assert actual_task == Task(
        policy="oneshot",
        type="some-task-type",
        next_run=mock_datetime.now.return_value,
        arguments=TaskArguments(
            args=[],
            kwargs={},
        ),
    )
    mock_datetime.now.assert_called_once_with(tz=timezone.utc)


def test_create_oneshot_task_with_next_run():
    next_run = utcnow() + timedelta(hours=1)
    actual_task = create_oneshot_task("some-task-type", next_run=next_run)

    assert actual_task == Task(
        policy="oneshot",
        type="some-task-type",
        next_run=next_run,
        arguments=TaskArguments(
            args=[],
            kwargs={},
        ),
    )


def test_create_oneshot_task_other_call(mock_datetime):
    actual_task = create_oneshot_task(
        "some-task-type", "arg0", "arg1", priority="high", other_stuff="normal"
    )

    assert actual_task == Task(
        policy="oneshot",
        type="some-task-type",
        next_run=mock_datetime.now.return_value,
        arguments=TaskArguments(
            args=["arg0", "arg1"],
            kwargs={"other_stuff": "normal"},
        ),
        priority="high",
    )
    mock_datetime.now.assert_called_once_with(tz=timezone.utc)


def test_create_task(mock_datetime):
    actual_task = create_task(
        "task-type",
        "recurring",
        "arg0",
        "arg1",
        priority="low",
        other_stuff="normal",
        retries_left=3,
    )

    assert actual_task == Task(
        policy="recurring",
        type="task-type",
        next_run=mock_datetime.now.return_value,
        arguments=TaskArguments(
            args=["arg0", "arg1"],
            kwargs={"other_stuff": "normal"},
        ),
        priority="low",
        retries_left=3,
    )
    mock_datetime.now.assert_called_once_with(tz=timezone.utc)


def test_create_origin_task(mock_datetime):
    lister = Lister(**LISTERS[1], id=uuid.uuid4())
    origin = ListedOrigin(
        lister_id=lister.id,
        url="http://example.com/",
        visit_type="git",
    )

    assert create_origin_task(origin, lister) == Task(
        type="load-git",
        next_run=mock_datetime.now.return_value,
        arguments=TaskArguments(
            args=[],
            kwargs={
                "url": "http://example.com/",
                "lister_name": LISTERS[1]["name"],
                "lister_instance_name": LISTERS[1]["instance_name"],
            },
        ),
    )

    loader_args = {"foo": "bar", "baz": {"foo": "bar"}}

    origin_w_args = ListedOrigin(
        lister_id=lister.id,
        url="http://example.com/svn/",
        visit_type="svn",
        extra_loader_arguments=loader_args,
    )

    assert create_origin_task(origin_w_args, lister) == Task(
        type="load-svn",
        next_run=mock_datetime.now.return_value,
        arguments=TaskArguments(
            args=[],
            kwargs={
                "url": "http://example.com/svn/",
                "lister_name": LISTERS[1]["name"],
                "lister_instance_name": LISTERS[1]["instance_name"],
                **loader_args,
            },
        ),
    )


def test_create_origin_tasks(swh_scheduler, mock_datetime):
    listers = []
    for lister_args in LISTERS:
        listers.append(swh_scheduler.get_or_create_lister(**lister_args))

    origin1 = ListedOrigin(
        lister_id=listers[0].id,
        url="http://example.com/1",
        visit_type="git",
    )
    origin2 = ListedOrigin(
        lister_id=listers[0].id,
        url="http://example.com/2",
        visit_type="git",
    )
    origin3 = ListedOrigin(
        lister_id=listers[1].id,
        url="http://example.com/3",
        visit_type="git",
    )

    origins = [origin1, origin2, origin3]

    tasks = create_origin_tasks(origins, swh_scheduler)
    assert tasks == [
        Task(
            type="load-git",
            next_run=mock_datetime.now.return_value,
            arguments=TaskArguments(
                args=[],
                kwargs={
                    "url": "http://example.com/1",
                    "lister_name": LISTERS[0]["name"],
                    "lister_instance_name": LISTERS[0]["instance_name"],
                },
            ),
        ),
        Task(
            type="load-git",
            next_run=mock_datetime.now.return_value,
            arguments=TaskArguments(
                args=[],
                kwargs={
                    "url": "http://example.com/2",
                    "lister_name": LISTERS[0]["name"],
                    "lister_instance_name": LISTERS[0]["instance_name"],
                },
            ),
        ),
        Task(
            type="load-git",
            next_run=mock_datetime.now.return_value,
            arguments=TaskArguments(
                args=[],
                kwargs={
                    "url": "http://example.com/3",
                    "lister_name": LISTERS[1]["name"],
                    "lister_instance_name": LISTERS[1]["instance_name"],
                },
            ),
        ),
    ]
