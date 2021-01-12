# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Tuple

import pytest

from swh.scheduler.cli.origin import format_origins
from swh.scheduler.tests.test_cli import invoke as basic_invoke


def invoke(scheduler, args: Tuple[str, ...] = (), catch_exceptions: bool = False):
    return basic_invoke(
        scheduler, args=["origin", *args], catch_exceptions=catch_exceptions
    )


def test_cli_origin(swh_scheduler):
    """Check that swh scheduler origin returns its help text"""

    result = invoke(swh_scheduler)

    assert "Commands:" in result.stdout


def test_format_origins_basic(listed_origins):
    listed_origins = listed_origins[:100]

    basic_output = list(format_origins(listed_origins))
    # 1 header line + all origins
    assert len(basic_output) == len(listed_origins) + 1

    no_header_output = list(format_origins(listed_origins, with_header=False))
    assert basic_output[1:] == no_header_output


def test_format_origins_fields_unknown(listed_origins):
    listed_origins = listed_origins[:10]

    it = format_origins(listed_origins, fields=["unknown_field"])

    with pytest.raises(ValueError, match="unknown_field"):
        next(it)


def test_format_origins_fields(listed_origins):
    listed_origins = listed_origins[:10]
    fields = ["lister_id", "url", "visit_type"]

    output = list(format_origins(listed_origins, fields=fields))
    assert output[0] == ",".join(fields)
    for i, origin in enumerate(listed_origins):
        assert output[i + 1] == f"{origin.lister_id},{origin.url},{origin.visit_type}"


def test_grab_next(swh_scheduler, listed_origins):
    num_origins = 10
    assert len(listed_origins) >= num_origins

    swh_scheduler.record_listed_origins(listed_origins)

    result = invoke(swh_scheduler, args=("grab-next", str(num_origins)))
    assert result.exit_code == 0

    out_lines = result.stdout.splitlines()
    assert len(out_lines) == num_origins + 1

    fields = out_lines[0].split(",")
    returned_origins = [dict(zip(fields, line.split(","))) for line in out_lines[1:]]

    # Check that we've received origins we had listed in the first place
    assert set(origin["url"] for origin in returned_origins) <= set(
        origin.url for origin in listed_origins
    )
