# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import functools
from itertools import permutations

import pytest

from swh.model.hashutil import hash_to_bytes
from swh.scheduler.journal_client import max_date, process_journal_objects
from swh.scheduler.model import OriginVisitStats
from swh.scheduler.utils import utcnow


def test_journal_client_origin_visit_status_from_journal_fail(swh_scheduler):
    process_fn = functools.partial(process_journal_objects, scheduler=swh_scheduler,)

    with pytest.raises(AssertionError, match="Got unexpected origin_visit"):
        process_fn({"origin_visit": [{"url": "http://foobar.baz"},]})

    with pytest.raises(AssertionError, match="Expected origin_visit_status"):
        process_fn({})


ONE_DAY = datetime.timedelta(days=1)

DATE3 = utcnow()
DATE2 = DATE3 - ONE_DAY
DATE1 = DATE2 - ONE_DAY


assert DATE1 < DATE2 < DATE3


@pytest.mark.parametrize(
    "d1,d2,expected_max_date",
    [
        (None, DATE2, DATE2),
        (DATE1, None, DATE1),
        (DATE1, DATE2, DATE2),
        (DATE2, DATE1, DATE2),
    ],
)
def test_max_date(d1, d2, expected_max_date):
    assert max_date(d1, d2) == expected_max_date


def test_max_date_raise():
    with pytest.raises(ValueError, match="valid datetime"):
        max_date(None, None)


def test_journal_client_origin_visit_status_from_journal_ignored_status(swh_scheduler):
    """Only final statuses (full, partial) are important, the rest remain ignored.

    """
    visit_statuses = [
        {
            "origin": "foo",
            "visit": 1,
            "status": "created",
            "date": utcnow(),
            "type": "git",
            "snapshot": None,
        },
        {
            "origin": "bar",
            "visit": 1,
            "status": "ongoing",
            "date": utcnow(),
            "type": "svn",
            "snapshot": None,
        },
    ]

    process_journal_objects(
        {"origin_visit_status": visit_statuses}, scheduler=swh_scheduler
    )

    # Ensure those visit status are ignored
    for visit_status in visit_statuses:
        actual_origin_visit_stats = swh_scheduler.origin_visit_stats_get(
            visit_status["origin"], visit_status["type"]
        )
        assert actual_origin_visit_stats is None


def test_journal_client_origin_visit_status_from_journal_last_notfound(swh_scheduler):
    visit_status = {
        "origin": "foo",
        "visit": 1,
        "status": "not_found",
        "date": DATE1,
        "type": "git",
        "snapshot": None,
    }

    process_journal_objects(
        {"origin_visit_status": [visit_status]}, scheduler=swh_scheduler
    )

    actual_origin_visit_stats = swh_scheduler.origin_visit_stats_get(
        visit_status["origin"], visit_status["type"]
    )
    assert actual_origin_visit_stats == OriginVisitStats(
        url=visit_status["origin"],
        visit_type=visit_status["type"],
        last_eventful=None,
        last_uneventful=None,
        last_failed=None,
        last_notfound=visit_status["date"],
        last_snapshot=None,
    )

    visit_statuses = [
        {
            "origin": "foo",
            "visit": 4,
            "status": "not_found",
            "date": DATE3,
            "type": "git",
            "snapshot": None,
        },
        {
            "origin": "foo",
            "visit": 3,
            "status": "not_found",
            "date": DATE2,
            "type": "git",
            "snapshot": None,
        },
    ]

    process_journal_objects(
        {"origin_visit_status": visit_statuses}, scheduler=swh_scheduler
    )

    for visit_status in visit_statuses:
        actual_origin_visit_stats = swh_scheduler.origin_visit_stats_get(
            visit_status["origin"], visit_status["type"]
        )
        assert actual_origin_visit_stats is not None
        assert actual_origin_visit_stats == OriginVisitStats(
            url=visit_status["origin"],
            visit_type=visit_status["type"],
            last_eventful=None,
            last_uneventful=None,
            last_failed=None,
            last_notfound=DATE3,
            last_snapshot=None,
        )


def test_journal_client_origin_visit_status_from_journal_last_failed(swh_scheduler):
    visit_statuses = [
        {
            "origin": "foo",
            "visit": 1,
            "status": "partial",
            "date": utcnow(),
            "type": "git",
            "snapshot": None,
        },
        {
            "origin": "bar",
            "visit": 2,
            "status": "full",
            "date": DATE1,
            "type": "git",
            "snapshot": None,
        },
    ]

    process_journal_objects(
        {"origin_visit_status": visit_statuses}, scheduler=swh_scheduler
    )

    # Ensure those visit status are ignored
    for visit_status in visit_statuses:
        actual_origin_visit_stats = swh_scheduler.origin_visit_stats_get(
            visit_status["origin"], visit_status["type"]
        )
        assert actual_origin_visit_stats is not None
        assert actual_origin_visit_stats == OriginVisitStats(
            url=visit_status["origin"],
            visit_type=visit_status["type"],
            last_eventful=None,
            last_uneventful=None,
            last_failed=visit_status["date"],
            last_notfound=None,
            last_snapshot=None,
        )

    visit_statuses = [
        {
            "origin": "bar",
            "visit": 3,
            "status": "full",
            "date": DATE3,
            "type": "git",
            "snapshot": None,
        },
        {
            "origin": "bar",
            "visit": 4,
            "status": "full",
            "date": DATE2,
            "type": "git",
            "snapshot": None,
        },
    ]

    process_journal_objects(
        {"origin_visit_status": visit_statuses}, scheduler=swh_scheduler
    )

    for visit_status in visit_statuses:
        actual_origin_visit_stats = swh_scheduler.origin_visit_stats_get(
            visit_status["origin"], visit_status["type"]
        )
        assert actual_origin_visit_stats is not None
        assert actual_origin_visit_stats == OriginVisitStats(
            url=visit_status["origin"],
            visit_type=visit_status["type"],
            last_eventful=None,
            last_uneventful=None,
            last_failed=DATE3,
            last_notfound=None,
            last_snapshot=None,
        )


def test_journal_client_origin_visit_status_from_journal_last_eventful(swh_scheduler):
    visit_statuses = [
        {
            "origin": "bar",
            "visit": 1,
            "status": "partial",
            "date": utcnow(),
            "type": "git",
            "snapshot": hash_to_bytes("d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"),
        },
        {
            "origin": "foo",
            "visit": 2,
            "status": "full",
            "date": DATE1,
            "type": "git",
            "snapshot": hash_to_bytes("eeecc0710eb6cf9efd5b920a8453e1e07157bfff"),
        },
    ]

    process_journal_objects(
        {"origin_visit_status": visit_statuses}, scheduler=swh_scheduler
    )

    for visit_status in visit_statuses:
        actual_origin_visit_stats = swh_scheduler.origin_visit_stats_get(
            visit_status["origin"], visit_status["type"]
        )
        assert actual_origin_visit_stats is not None
        assert actual_origin_visit_stats == OriginVisitStats(
            url=visit_status["origin"],
            visit_type=visit_status["type"],
            last_eventful=visit_status["date"],
            last_uneventful=None,
            last_failed=None,
            last_notfound=None,
            last_snapshot=visit_status["snapshot"],
        )

    visit_statuses = [
        {
            "origin": "foo",
            "visit": 4,
            "status": "full",
            "date": DATE3,
            "type": "git",
            "snapshot": hash_to_bytes("dddcc0710eb6cf9efd5b920a8453e1e07157bddd"),
        },
        {
            "origin": "foo",
            "visit": 3,
            "status": "partial",
            "date": DATE2,
            "type": "git",
            "snapshot": hash_to_bytes("aaacc0710eb6cf9efd5b920a8453e1e07157baaa"),
        },
    ]

    process_journal_objects(
        {"origin_visit_status": visit_statuses}, scheduler=swh_scheduler
    )

    for visit_status in visit_statuses:
        actual_origin_visit_stats = swh_scheduler.origin_visit_stats_get(
            visit_status["origin"], visit_status["type"]
        )
        assert actual_origin_visit_stats is not None
        assert actual_origin_visit_stats == OriginVisitStats(
            url=visit_status["origin"],
            visit_type=visit_status["type"],
            last_eventful=DATE3,
            last_uneventful=None,
            last_failed=None,
            last_notfound=None,
            last_snapshot=hash_to_bytes("dddcc0710eb6cf9efd5b920a8453e1e07157bddd"),
        )


def test_journal_client_origin_visit_status_from_journal_last_uneventful(swh_scheduler):
    visit_status = {
        "origin": "foo",
        "visit": 1,
        "status": "full",
        "date": DATE3 + ONE_DAY,
        "type": "git",
        "snapshot": hash_to_bytes("d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"),
    }

    # Let's insert some visit stats with some previous visit information
    swh_scheduler.origin_visit_stats_upsert(
        [
            OriginVisitStats(
                url=visit_status["origin"],
                visit_type=visit_status["type"],
                last_eventful=DATE1,
                last_uneventful=DATE3,
                last_failed=DATE2,
                last_notfound=DATE1,
                last_snapshot=visit_status["snapshot"],
            )
        ]
    )

    process_journal_objects(
        {"origin_visit_status": [visit_status]}, scheduler=swh_scheduler
    )

    actual_origin_visit_stats = swh_scheduler.origin_visit_stats_get(
        visit_status["origin"], visit_status["type"]
    )
    assert actual_origin_visit_stats is not None
    assert actual_origin_visit_stats == OriginVisitStats(
        url=visit_status["origin"],
        visit_type=visit_status["type"],
        last_eventful=DATE1,
        last_uneventful=visit_status["date"],  # most recent date but uneventful
        last_failed=DATE2,
        last_notfound=DATE1,
        last_snapshot=visit_status["snapshot"],
    )


VISIT_STATUSES = [
    {**ovs, "date": DATE1 + n * ONE_DAY}
    for n, ovs in enumerate(
        [
            {
                "origin": "foo",
                "type": "git",
                "visit": 1,
                "status": "created",
                "snapshot": None,
            },
            {
                "origin": "foo",
                "type": "git",
                "visit": 1,
                "status": "full",
                "snapshot": hash_to_bytes("d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"),
            },
            {
                "origin": "foo",
                "type": "git",
                "visit": 2,
                "status": "created",
                "snapshot": None,
            },
            {
                "origin": "foo",
                "type": "git",
                "visit": 2,
                "status": "full",
                "snapshot": hash_to_bytes("d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"),
            },
        ]
    )
]


@pytest.mark.parametrize(
    "visit_statuses", permutations(VISIT_STATUSES, len(VISIT_STATUSES))
)
def test_journal_client_origin_visit_status_permutation0(visit_statuses, swh_scheduler):
    """Ensure out of order topic subscription ends up in the same final state

    """
    process_journal_objects(
        {"origin_visit_status": visit_statuses}, scheduler=swh_scheduler
    )

    expected_visit_stats = OriginVisitStats(
        url="foo",
        visit_type="git",
        last_eventful=DATE1 + ONE_DAY,
        last_uneventful=DATE1 + 3 * ONE_DAY,
        last_failed=None,
        last_notfound=None,
        last_snapshot=hash_to_bytes("d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"),
    )

    assert swh_scheduler.origin_visit_stats_get("foo", "git") == expected_visit_stats


VISIT_STATUSES1 = [
    {**ovs, "date": DATE1 + n * ONE_DAY}
    for n, ovs in enumerate(
        [
            {
                "origin": "cavabarder",
                "type": "hg",
                "visit": 1,
                "status": "partial",
                "snapshot": hash_to_bytes("d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"),
            },
            {
                "origin": "cavabarder",
                "type": "hg",
                "visit": 2,
                "status": "full",
                "snapshot": hash_to_bytes("d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"),
            },
            {
                "origin": "cavabarder",
                "type": "hg",
                "visit": 3,
                "status": "full",
                "snapshot": hash_to_bytes("aaaaaabbbeb6cf9efd5b920a8453e1e07157b6cd"),
            },
            {
                "origin": "cavabarder",
                "type": "hg",
                "visit": 4,
                "status": "full",
                "snapshot": hash_to_bytes("aaaaaabbbeb6cf9efd5b920a8453e1e07157b6cd"),
            },
        ]
    )
]


@pytest.mark.parametrize(
    "visit_statuses", permutations(VISIT_STATUSES1, len(VISIT_STATUSES1))
)
def test_journal_client_origin_visit_status_permutation1(visit_statuses, swh_scheduler):
    """Ensure out of order topic subscription ends up in the same final state

    """
    process_journal_objects(
        {"origin_visit_status": visit_statuses}, scheduler=swh_scheduler
    )

    expected_visit_stats = OriginVisitStats(
        url="cavabarder",
        visit_type="hg",
        last_eventful=DATE1 + 2 * ONE_DAY,
        last_uneventful=DATE1 + 3 * ONE_DAY,
        last_failed=None,
        last_notfound=None,
        last_snapshot=hash_to_bytes("aaaaaabbbeb6cf9efd5b920a8453e1e07157b6cd"),
    )

    assert (
        swh_scheduler.origin_visit_stats_get("cavabarder", "hg") == expected_visit_stats
    )
