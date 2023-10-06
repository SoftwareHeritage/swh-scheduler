# Copyright (C) 2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import timedelta

import pytest

from swh.scheduler.cli.utils import lister_task_type, parse_time_interval


@pytest.mark.parametrize(
    "lister_name, lister_type, expected_task_type",
    [
        ["cgit", None, "list-cgit"],
        ["gitlab", "full", "list-gitlab-full"],
        ["gitea", "incremental", "list-gitea-incremental"],
    ],
)
def test_lister_task_type(lister_name, lister_type, expected_task_type):
    assert lister_task_type(lister_name, lister_type) == expected_task_type


@pytest.mark.parametrize(
    "time_str,expected_timedelta",
    [
        ("1 day", timedelta(days=1)),
        ("1 days", timedelta(days=1)),
        ("2 hours", timedelta(hours=2)),
        ("99 hour", timedelta(hours=99)),
    ],
)
def test_parse_time_interval(time_str, expected_timedelta):
    assert parse_time_interval(time_str) == expected_timedelta
