# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from datetime import timedelta
import functools
from itertools import permutations
from typing import List
from unittest.mock import Mock

import attr
import pytest

from swh.model.hashutil import hash_to_bytes
from swh.scheduler.journal_client import (
    from_position_offset_to_days,
    max_date,
    next_visit_queue_position,
    process_journal_objects,
)
from swh.scheduler.model import LastVisitStatus, ListedOrigin, OriginVisitStats
from swh.scheduler.utils import utcnow


def test_journal_client_origin_visit_status_from_journal_fail(swh_scheduler):
    process_fn = functools.partial(process_journal_objects, scheduler=swh_scheduler,)

    with pytest.raises(AssertionError, match="Got unexpected origin_visit"):
        process_fn({"origin_visit": [{"url": "http://foobar.baz"},]})

    with pytest.raises(AssertionError, match="Expected origin_visit_status"):
        process_fn({})


ONE_DAY = datetime.timedelta(days=1)
ONE_YEAR = datetime.timedelta(days=366)

DATE3 = utcnow()
DATE2 = DATE3 - ONE_DAY
DATE1 = DATE2 - ONE_DAY


assert DATE1 < DATE2 < DATE3


@pytest.mark.parametrize(
    "dates,expected_max_date",
    [
        ((DATE1,), DATE1),
        ((None, DATE2), DATE2),
        ((DATE1, None), DATE1),
        ((DATE1, DATE2), DATE2),
        ((DATE2, DATE1), DATE2),
        ((DATE1, DATE2, DATE3), DATE3),
        ((None, DATE2, DATE3), DATE3),
        ((None, None, DATE3), DATE3),
        ((DATE1, None, DATE3), DATE3),
    ],
)
def test_max_date(dates, expected_max_date):
    assert max_date(*dates) == expected_max_date


def test_max_date_raise():
    with pytest.raises(ValueError, match="valid datetime"):
        max_date()
    with pytest.raises(ValueError, match="valid datetime"):
        max_date(None)
    with pytest.raises(ValueError, match="valid datetime"):
        max_date(None, None)


def test_journal_client_origin_visit_status_from_journal_ignored_status(swh_scheduler):
    """Only final statuses (full, partial) are important, the rest remain ignored.

    """
    # Trace method calls on the swh_scheduler
    swh_scheduler = Mock(wraps=swh_scheduler)

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

    # All messages have been ignored: no stats have been upserted
    swh_scheduler.origin_visit_stats_upsert.assert_not_called()


def test_journal_client_ignore_missing_type(swh_scheduler):
    """Ignore statuses with missing type key"""
    # Trace method calls on the swh_scheduler
    swh_scheduler = Mock(wraps=swh_scheduler)

    date = utcnow()
    snapshot = hash_to_bytes("dddcc0710eb6cf9efd5b920a8453e1e07157bddd")
    visit_statuses = [
        {
            "origin": "foo",
            "visit": 1,
            "status": "full",
            "date": date,
            "snapshot": snapshot,
        },
    ]

    process_journal_objects(
        {"origin_visit_status": visit_statuses}, scheduler=swh_scheduler
    )

    # The message has been ignored: no stats have been upserted
    swh_scheduler.origin_visit_stats_upsert.assert_not_called()


def assert_visit_stats_ok(
    actual_visit_stats: OriginVisitStats,
    expected_visit_stats: OriginVisitStats,
    ignore_fields: List[str] = ["next_visit_queue_position"],
):
    """Utility test function to ensure visits stats read from the backend are in the right
    shape. The comparison on the next_visit_queue_position will be dealt with in
    dedicated tests so it's not tested in tests that are calling this function.

    """
    fields = attr.fields_dict(OriginVisitStats)
    defaults = {field: fields[field].default for field in ignore_fields}

    actual_visit_stats = attr.evolve(actual_visit_stats, **defaults)
    assert actual_visit_stats == expected_visit_stats


def test_journal_client_origin_visit_status_from_journal_last_not_found(swh_scheduler):
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

    actual_origin_visit_stats = swh_scheduler.origin_visit_stats_get([("foo", "git")])
    assert_visit_stats_ok(
        actual_origin_visit_stats[0],
        OriginVisitStats(
            url="foo",
            visit_type="git",
            last_visit=visit_status["date"],
            last_visit_status=LastVisitStatus.not_found,
            next_position_offset=4,
            successive_visits=1,
        ),
    )

    visit_statuses = [
        {
            "origin": "foo",
            "visit": 3,
            "status": "not_found",
            "date": DATE2,
            "type": "git",
            "snapshot": None,
        },
        {
            "origin": "foo",
            "visit": 4,
            "status": "not_found",
            "date": DATE3,
            "type": "git",
            "snapshot": None,
        },
    ]

    process_journal_objects(
        {"origin_visit_status": visit_statuses}, scheduler=swh_scheduler
    )

    actual_origin_visit_stats = swh_scheduler.origin_visit_stats_get([("foo", "git")])
    assert_visit_stats_ok(
        actual_origin_visit_stats[0],
        OriginVisitStats(
            url="foo",
            visit_type="git",
            last_visit=DATE3,
            last_visit_status=LastVisitStatus.not_found,
            next_position_offset=6,
            successive_visits=3,
        ),
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
            "visit": 1,
            "status": "full",
            "date": DATE1,
            "type": "git",
            "snapshot": None,
        },
        {
            "origin": "bar",
            "visit": 2,
            "status": "full",
            "date": DATE2,
            "type": "git",
            "snapshot": None,
        },
        {
            "origin": "bar",
            "visit": 3,
            "status": "full",
            "date": DATE3,
            "type": "git",
            "snapshot": None,
        },
    ]

    process_journal_objects(
        {"origin_visit_status": visit_statuses}, scheduler=swh_scheduler
    )

    actual_origin_visit_stats = swh_scheduler.origin_visit_stats_get([("bar", "git")])
    assert_visit_stats_ok(
        actual_origin_visit_stats[0],
        OriginVisitStats(
            url="bar",
            visit_type="git",
            last_visit=DATE3,
            last_visit_status=LastVisitStatus.failed,
            next_position_offset=6,
            successive_visits=3,
        ),
    )


def test_journal_client_origin_visit_status_from_journal_last_failed2(swh_scheduler):
    visit_statuses = [
        {
            "origin": "bar",
            "visit": 2,
            "status": "failed",
            "date": DATE1,
            "type": "git",
            "snapshot": hash_to_bytes("d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"),
        },
        {
            "origin": "bar",
            "visit": 3,
            "status": "failed",
            "date": DATE2,
            "type": "git",
            "snapshot": None,
        },
    ]

    process_journal_objects(
        {"origin_visit_status": visit_statuses}, scheduler=swh_scheduler
    )

    actual_origin_visit_stats = swh_scheduler.origin_visit_stats_get([("bar", "git")])
    assert_visit_stats_ok(
        actual_origin_visit_stats[0],
        OriginVisitStats(
            url="bar",
            visit_type="git",
            last_visit=DATE2,
            last_visit_status=LastVisitStatus.failed,
            next_position_offset=5,
            successive_visits=2,
        ),
    )


def test_journal_client_origin_visit_status_from_journal_last_successful(swh_scheduler):
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
            "visit": 1,
            "status": "full",
            "date": DATE1,
            "type": "git",
            "snapshot": hash_to_bytes("eeecc0710eb6cf9efd5b920a8453e1e07157bfff"),
        },
        {
            "origin": "foo",
            "visit": 2,
            "status": "partial",
            "date": DATE2,
            "type": "git",
            "snapshot": hash_to_bytes("aaacc0710eb6cf9efd5b920a8453e1e07157baaa"),
        },
        {
            "origin": "foo",
            "visit": 3,
            "status": "full",
            "date": DATE3,
            "type": "git",
            "snapshot": hash_to_bytes("dddcc0710eb6cf9efd5b920a8453e1e07157bddd"),
        },
    ]

    process_journal_objects(
        {"origin_visit_status": visit_statuses}, scheduler=swh_scheduler
    )

    actual_origin_visit_stats = swh_scheduler.origin_visit_stats_get([("foo", "git")])
    assert_visit_stats_ok(
        actual_origin_visit_stats[0],
        OriginVisitStats(
            url="foo",
            visit_type="git",
            last_successful=DATE3,
            last_visit=DATE3,
            last_visit_status=LastVisitStatus.successful,
            last_snapshot=hash_to_bytes("dddcc0710eb6cf9efd5b920a8453e1e07157bddd"),
            next_position_offset=0,
            successive_visits=3,
        ),
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
                last_successful=DATE2,
                last_visit=DATE3,
                last_visit_status=LastVisitStatus.failed,
                last_snapshot=visit_status["snapshot"],
                next_visit_queue_position=None,
                next_position_offset=4,
                successive_visits=1,
            )
        ]
    )

    process_journal_objects(
        {"origin_visit_status": [visit_status]}, scheduler=swh_scheduler
    )

    actual_origin_visit_stats = swh_scheduler.origin_visit_stats_get(
        [(visit_status["origin"], visit_status["type"])]
    )

    assert_visit_stats_ok(
        actual_origin_visit_stats[0],
        OriginVisitStats(
            url=visit_status["origin"],
            visit_type=visit_status["type"],
            last_visit=DATE3 + ONE_DAY,
            last_successful=DATE3 + ONE_DAY,
            last_visit_status=LastVisitStatus.successful,
            last_snapshot=visit_status["snapshot"],
            next_visit_queue_position=None,
            next_position_offset=5,
            successive_visits=1,
        ),
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

    actual_origin_visit_stats = swh_scheduler.origin_visit_stats_get([("foo", "git")])
    visit_stats = actual_origin_visit_stats[0]
    assert_visit_stats_ok(
        visit_stats,
        OriginVisitStats(
            url="foo",
            visit_type="git",
            last_successful=DATE1 + 3 * ONE_DAY,
            last_visit=DATE1 + 3 * ONE_DAY,
            last_visit_status=LastVisitStatus.successful,
            last_snapshot=hash_to_bytes("d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"),
        ),
        ignore_fields=[
            "next_visit_queue_position",
            "next_position_offset",
            "successive_visits",
        ],
    )

    # We ignore out of order messages, so the next_position_offset isn't exact
    # depending on the permutation. What matters is consistency of the final
    # dates (last_visit and last_successful).
    assert 4 <= visit_stats.next_position_offset <= 5
    # same goes for successive_visits
    assert 1 <= visit_stats.successive_visits <= 2


VISIT_STATUSES_1 = [
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
    "visit_statuses", permutations(VISIT_STATUSES_1, len(VISIT_STATUSES_1))
)
def test_journal_client_origin_visit_status_permutation1(visit_statuses, swh_scheduler):
    """Ensure out of order topic subscription ends up in the same final state

    """
    process_journal_objects(
        {"origin_visit_status": visit_statuses}, scheduler=swh_scheduler
    )

    actual_visit_stats = swh_scheduler.origin_visit_stats_get([("cavabarder", "hg")])
    visit_stats = actual_visit_stats[0]

    assert_visit_stats_ok(
        visit_stats,
        OriginVisitStats(
            url="cavabarder",
            visit_type="hg",
            last_successful=DATE1 + 3 * ONE_DAY,
            last_visit=DATE1 + 3 * ONE_DAY,
            last_visit_status=LastVisitStatus.successful,
            last_snapshot=hash_to_bytes("aaaaaabbbeb6cf9efd5b920a8453e1e07157b6cd"),
        ),
        ignore_fields=[
            "next_visit_queue_position",
            "next_position_offset",
            "successive_visits",
        ],
    )

    # We ignore out of order messages, so the next_position_offset isn't exact
    # depending on the permutation. What matters is consistency of the final
    # dates (last_visit and last_successful).
    assert 2 <= visit_stats.next_position_offset <= 5
    # same goes for successive_visits
    assert 1 <= visit_stats.successive_visits <= 4


VISIT_STATUSES_2 = [
    {**ovs, "date": DATE1 + n * ONE_DAY}
    for n, ovs in enumerate(
        [
            {
                "origin": "cavabarder",
                "type": "hg",
                "visit": 1,
                "status": "full",
                "snapshot": hash_to_bytes("0000000000000000000000000000000000000000"),
            },
            {
                "origin": "cavabarder",
                "type": "hg",
                "visit": 2,
                "status": "full",
                "snapshot": hash_to_bytes("1111111111111111111111111111111111111111"),
            },
            {
                "origin": "iciaussi",
                "type": "hg",
                "visit": 1,
                "status": "full",
                "snapshot": hash_to_bytes("2222222222222222222222222222222222222222"),
            },
            {
                "origin": "iciaussi",
                "type": "hg",
                "visit": 2,
                "status": "full",
                "snapshot": hash_to_bytes("3333333333333333333333333333333333333333"),
            },
            {
                "origin": "cavabarder",
                "type": "git",
                "visit": 1,
                "status": "full",
                "snapshot": hash_to_bytes("4444444444444444444444444444444444444444"),
            },
            {
                "origin": "cavabarder",
                "type": "git",
                "visit": 2,
                "status": "full",
                "snapshot": hash_to_bytes("5555555555555555555555555555555555555555"),
            },
            {
                "origin": "iciaussi",
                "type": "git",
                "visit": 1,
                "status": "full",
                "snapshot": hash_to_bytes("6666666666666666666666666666666666666666"),
            },
            {
                "origin": "iciaussi",
                "type": "git",
                "visit": 2,
                "status": "full",
                "snapshot": hash_to_bytes("7777777777777777777777777777777777777777"),
            },
        ]
    )
]


def test_journal_client_origin_visit_status_after_grab_next_visits(
    swh_scheduler, stored_lister
):
    """Ensure OriginVisitStat entries created in the db as a result of calling
    grab_next_visits() do not mess the OriginVisitStats upsert mechanism.

    """

    listed_origins = [
        ListedOrigin(lister_id=stored_lister.id, url=url, visit_type=visit_type)
        for (url, visit_type) in set((v["origin"], v["type"]) for v in VISIT_STATUSES_2)
    ]
    swh_scheduler.record_listed_origins(listed_origins)
    before = utcnow()
    swh_scheduler.grab_next_visits(
        visit_type="git", count=10, policy="oldest_scheduled_first"
    )
    after = utcnow()

    assert swh_scheduler.origin_visit_stats_get([("cavabarder", "hg")]) == []
    assert swh_scheduler.origin_visit_stats_get([("cavabarder", "git")])[0] is not None

    process_journal_objects(
        {"origin_visit_status": VISIT_STATUSES_2}, scheduler=swh_scheduler
    )

    for url in ("cavabarder", "iciaussi"):
        ovs = swh_scheduler.origin_visit_stats_get([(url, "git")])[0]
        assert before <= ovs.last_scheduled <= after

        ovs = swh_scheduler.origin_visit_stats_get([(url, "hg")])[0]
        assert ovs.last_scheduled is None

    ovs = swh_scheduler.origin_visit_stats_get([("cavabarder", "git")])[0]
    assert ovs.last_successful == DATE1 + 5 * ONE_DAY
    assert ovs.last_visit == DATE1 + 5 * ONE_DAY
    assert ovs.last_visit_status == LastVisitStatus.successful
    assert ovs.last_snapshot == hash_to_bytes(
        "5555555555555555555555555555555555555555"
    )


def test_journal_client_origin_visit_status_duplicated_messages(swh_scheduler):
    """A duplicated message must be ignored

    """
    visit_status = {
        "origin": "foo",
        "visit": 1,
        "status": "full",
        "date": DATE1,
        "type": "git",
        "snapshot": hash_to_bytes("aaaaaabbbeb6cf9efd5b920a8453e1e07157b6cd"),
    }

    process_journal_objects(
        {"origin_visit_status": [visit_status]}, scheduler=swh_scheduler
    )

    process_journal_objects(
        {"origin_visit_status": [visit_status]}, scheduler=swh_scheduler
    )

    actual_origin_visit_stats = swh_scheduler.origin_visit_stats_get([("foo", "git")])
    assert_visit_stats_ok(
        actual_origin_visit_stats[0],
        OriginVisitStats(
            url="foo",
            visit_type="git",
            last_successful=DATE1,
            last_visit=DATE1,
            last_visit_status=LastVisitStatus.successful,
            last_snapshot=hash_to_bytes("aaaaaabbbeb6cf9efd5b920a8453e1e07157b6cd"),
            successive_visits=1,
        ),
    )


def test_journal_client_origin_visit_status_several_upsert(swh_scheduler):
    """An old message updates old information

    """
    visit_status1 = {
        "origin": "foo",
        "visit": 1,
        "status": "full",
        "date": DATE1,
        "type": "git",
        "snapshot": hash_to_bytes("aaaaaabbbeb6cf9efd5b920a8453e1e07157b6cd"),
    }

    visit_status2 = {
        "origin": "foo",
        "visit": 1,
        "status": "full",
        "date": DATE2,
        "type": "git",
        "snapshot": hash_to_bytes("aaaaaabbbeb6cf9efd5b920a8453e1e07157b6cd"),
    }

    process_journal_objects(
        {"origin_visit_status": [visit_status2]}, scheduler=swh_scheduler
    )

    process_journal_objects(
        {"origin_visit_status": [visit_status1]}, scheduler=swh_scheduler
    )

    actual_origin_visit_stats = swh_scheduler.origin_visit_stats_get([("foo", "git")])
    assert_visit_stats_ok(
        actual_origin_visit_stats[0],
        OriginVisitStats(
            url="foo",
            visit_type="git",
            last_successful=DATE2,
            last_visit=DATE2,
            last_visit_status=LastVisitStatus.successful,
            last_snapshot=hash_to_bytes("aaaaaabbbeb6cf9efd5b920a8453e1e07157b6cd"),
            next_position_offset=4,
            successive_visits=1,
        ),
    )


VISIT_STATUSES_SAME_SNAPSHOT = [
    {**ovs, "date": DATE1 + n * ONE_YEAR}
    for n, ovs in enumerate(
        [
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
    "visit_statuses",
    permutations(VISIT_STATUSES_SAME_SNAPSHOT, len(VISIT_STATUSES_SAME_SNAPSHOT)),
)
def test_journal_client_origin_visit_statuses_same_snapshot_permutation(
    visit_statuses, swh_scheduler
):
    """Ensure out of order topic subscription ends up in the same final state

    """
    process_journal_objects(
        {"origin_visit_status": visit_statuses}, scheduler=swh_scheduler
    )

    actual_origin_visit_stats = swh_scheduler.origin_visit_stats_get(
        [("cavabarder", "hg")]
    )
    visit_stats = actual_origin_visit_stats[0]
    assert_visit_stats_ok(
        visit_stats,
        OriginVisitStats(
            url="cavabarder",
            visit_type="hg",
            last_successful=DATE1 + 2 * ONE_YEAR,
            last_visit=DATE1 + 2 * ONE_YEAR,
            last_visit_status=LastVisitStatus.successful,
            last_snapshot=hash_to_bytes("aaaaaabbbeb6cf9efd5b920a8453e1e07157b6cd"),
        ),
        ignore_fields=[
            "next_visit_queue_position",
            "next_position_offset",
            "successive_visits",
        ],
    )

    # We ignore out of order messages, so the next_position_offset isn't exact
    # depending on the permutation. What matters is consistency of the final
    # dates (last_visit and last_successful).
    assert 4 <= visit_stats.next_position_offset <= 6
    # same goes for successive_visits
    assert 1 <= visit_stats.successive_visits <= 3


@pytest.mark.parametrize(
    "position_offset, interval",
    [
        (0, 1),
        (1, 1),
        (2, 2),
        (3, 2),
        (4, 2),
        (5, 4),
        (6, 16),
        (7, 64),
        (8, 256),
        (9, 1024),
        (10, 4096),
        (11, 16384),
    ],
)
def test_journal_client_from_position_offset_to_days(position_offset, interval):
    assert from_position_offset_to_days(position_offset) == interval


def test_journal_client_from_position_offset_to_days_only_positive_input():
    with pytest.raises(AssertionError):
        from_position_offset_to_days(-1)


@pytest.mark.parametrize(
    "fudge_factor,next_position_offset", [(0.01, 1), (-0.01, 5), (0.1, 8), (-0.1, 10),]
)
def test_next_visit_queue_position(mocker, fudge_factor, next_position_offset):
    mock_random = mocker.patch("swh.scheduler.journal_client.random.uniform")
    mock_random.return_value = fudge_factor

    date_now = utcnow()

    mock_now = mocker.patch("swh.scheduler.journal_client.utcnow")
    mock_now.return_value = date_now

    actual_position = next_visit_queue_position(
        {}, {"next_position_offset": next_position_offset, "visit_type": "svn",}
    )

    assert actual_position == date_now + timedelta(
        days=from_position_offset_to_days(next_position_offset) * (1 + fudge_factor)
    )

    assert mock_now.called
    assert mock_random.called


@pytest.mark.parametrize(
    "fudge_factor,next_position_offset", [(0.02, 2), (-0.02, 3), (0, 7), (-0.09, 9),]
)
def test_next_visit_queue_position_with_state(
    mocker, fudge_factor, next_position_offset
):
    mock_random = mocker.patch("swh.scheduler.journal_client.random.uniform")
    mock_random.return_value = fudge_factor

    date_now = utcnow()

    actual_position = next_visit_queue_position(
        {"git": date_now},
        {"next_position_offset": next_position_offset, "visit_type": "git",},
    )

    assert actual_position == date_now + timedelta(
        days=from_position_offset_to_days(next_position_offset) * (1 + fudge_factor)
    )

    assert mock_random.called


@pytest.mark.parametrize(
    "fudge_factor,next_position_offset", [(0.03, 3), (-0.03, 4), (0.08, 7), (-0.08, 9),]
)
def test_next_visit_queue_position_with_next_visit_queue(
    mocker, fudge_factor, next_position_offset
):
    mock_random = mocker.patch("swh.scheduler.journal_client.random.uniform")
    mock_random.return_value = fudge_factor

    date_now = utcnow()

    actual_position = next_visit_queue_position(
        {},
        {
            "next_position_offset": next_position_offset,
            "visit_type": "hg",
            "next_visit_queue_position": date_now,
        },
    )

    assert actual_position == date_now + timedelta(
        days=from_position_offset_to_days(next_position_offset) * (1 + fudge_factor)
    )

    assert mock_random.called


def test_disable_failing_origins(swh_scheduler):
    """Origin with too many failed attempts ends up being deactivated in the scheduler.

    """

    # actually store the origin in the scheduler so we can check it's deactivated in the
    # end.
    lister = swh_scheduler.get_or_create_lister(
        name="something", instance_name="something"
    )
    origin = ListedOrigin(
        url="bar", enabled=True, visit_type="svn", lister_id=lister.id
    )
    swh_scheduler.record_listed_origins([origin])

    visit_statuses = [
        {
            "origin": "bar",
            "visit": 2,
            "status": "failed",
            "date": DATE1,
            "type": "svn",
            "snapshot": None,
        },
        {
            "origin": "bar",
            "visit": 3,
            "status": "failed",
            "date": DATE2,
            "type": "svn",
            "snapshot": None,
        },
        {
            "origin": "bar",
            "visit": 3,
            "status": "failed",
            "date": DATE3,
            "type": "svn",
            "snapshot": None,
        },
    ]

    process_journal_objects(
        {"origin_visit_status": visit_statuses}, scheduler=swh_scheduler
    )

    actual_origin_visit_stats = swh_scheduler.origin_visit_stats_get([("bar", "svn")])
    assert_visit_stats_ok(
        actual_origin_visit_stats[0],
        OriginVisitStats(
            url="bar",
            visit_type="svn",
            last_successful=None,
            last_visit=DATE3,
            last_visit_status=LastVisitStatus.failed,
            next_position_offset=6,
            successive_visits=3,
        ),
    )

    # Now check that the origin in question is disabled
    actual_page = swh_scheduler.get_listed_origins(url="bar")

    assert len(actual_page.results) == 1
    assert actual_page.next_page_token is None

    for origin in actual_page.results:
        assert origin.enabled is False
        assert origin.lister_id == lister.id
        assert origin.url == "bar"
        assert origin.visit_type == "svn"
