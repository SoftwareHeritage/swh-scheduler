# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.scheduler.cli.origin_utils import get_scheduler_task_type


def test_get_scheduler_task_type_found(swh_scheduler, task_types):
    """It should find standard task types"""
    # Check we found the standard task type
    for task_type, task_type_info in task_types.items():
        actual_task_info = get_scheduler_task_type(swh_scheduler, task_type)
        assert actual_task_info == task_type_info


@pytest.mark.parametrize(
    "task_type_suffix", ["bitbucket", "extra-mercurial", "overly-long-task-suffix"]
)
def test_get_scheduler_task_type_found_derivative(
    swh_scheduler, task_types, task_type_suffix
):
    """It should find derivative task types"""
    # Create another task type derivative out of a standard task type
    first_task_type = next(iter(task_types))
    another_task_type = f"{first_task_type}-{task_type_suffix}"

    actual_task_info_2 = get_scheduler_task_type(swh_scheduler, another_task_type)
    # we found it and it matches the origin task type it derives from
    assert actual_task_info_2 == task_types[first_task_type]


def test_get_scheduler_task_type_found_raise(swh_scheduler, task_types):
    """It should raise when no task_type is found"""
    unknown_task_type = "foobar-task-type"
    assert unknown_task_type not in task_types

    with pytest.raises(ValueError, match="not find scheduler"):
        get_scheduler_task_type(swh_scheduler, unknown_task_type)
