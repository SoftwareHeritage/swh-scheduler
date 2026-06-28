# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import date, datetime, timezone
from email.utils import format_datetime

import pytest

from swh.scheduler.cli.click_utils import DATETIME


def iso(timespec):
    if timespec == "days":
        now = date.today()
        args = {}
    else:
        now = datetime.now(timezone.utc)
        args = {"timespec": timespec}
    return now.isoformat(**args)


iso_timespecs = "days hours minutes seconds milliseconds microseconds".split()


@pytest.mark.parametrize(
    "dt",
    list(iso(spec) for spec in iso_timespecs)
    + [
        # ISO 8601 UTC with Z
        datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        # RFC 822/2822/5322
        format_datetime(datetime.now(timezone.utc)),
        # dateutil can handle this
        "1st Jan 2027",
        # dateparser can handle this
        "yesterday",
    ],
)
def test_date_formats(dt):
    assert DATETIME.convert(dt, None, None)
