# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.scheduler.cli.utils import lister_task_type


@pytest.mark.parametrize(
    "lister_name,listing_type", [("foo", "full"), ("bar", "incremental")]
)
def test_lister_task_type(lister_name, listing_type):
    assert lister_task_type(lister_name, listing_type) == (
        f"list-{lister_name}-{listing_type}"
    )
