# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import functools
from itertools import permutations
from typing import Callable

from hypothesis import HealthCheck, given, settings, strategies
import pytest

from swh.core.utils import grouper
from swh.model.hashutil import hash_to_bytes
from swh.scheduler.journal_client import max_date, process_journal_objects
from swh.scheduler.model import ListedOrigin, OriginVisitStats
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
    actual_origin_visit_stats = swh_scheduler.origin_visit_stats_get(
        [(vs["origin"], vs["type"]) for vs in visit_statuses]
    )
    assert actual_origin_visit_stats == []


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

    actual_origin_visit_stats = swh_scheduler.origin_visit_stats_get([("foo", "git")])
    assert actual_origin_visit_stats == [
        OriginVisitStats(
            url="foo",
            visit_type="git",
            last_eventful=None,
            last_uneventful=None,
            last_failed=None,
            last_notfound=visit_status["date"],
            last_snapshot=None,
        )
    ]

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
    assert actual_origin_visit_stats == [
        OriginVisitStats(
            url="foo",
            visit_type="git",
            last_eventful=None,
            last_uneventful=None,
            last_failed=None,
            last_notfound=DATE3,
            last_snapshot=None,
        )
    ]


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
    assert actual_origin_visit_stats == [
        OriginVisitStats(
            url="bar",
            visit_type="git",
            last_eventful=None,
            last_uneventful=None,
            last_failed=DATE3,
            last_notfound=None,
            last_snapshot=None,
        )
    ]


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
    assert actual_origin_visit_stats == [
        OriginVisitStats(
            url="foo",
            visit_type="git",
            last_eventful=DATE3,
            last_uneventful=None,
            last_failed=None,
            last_notfound=None,
            last_snapshot=hash_to_bytes("dddcc0710eb6cf9efd5b920a8453e1e07157bddd"),
        )
    ]


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
        [(visit_status["origin"], visit_status["type"])]
    )
    assert actual_origin_visit_stats == [
        OriginVisitStats(
            url=visit_status["origin"],
            visit_type=visit_status["type"],
            last_eventful=DATE1,
            last_uneventful=visit_status["date"],  # most recent date but uneventful
            last_failed=DATE2,
            last_notfound=DATE1,
            last_snapshot=visit_status["snapshot"],
        )
    ]


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

    assert swh_scheduler.origin_visit_stats_get([("foo", "git")]) == [
        expected_visit_stats
    ]


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

    expected_visit_stats = OriginVisitStats(
        url="cavabarder",
        visit_type="hg",
        last_eventful=DATE1 + 2 * ONE_DAY,
        last_uneventful=DATE1 + 3 * ONE_DAY,
        last_failed=None,
        last_notfound=None,
        last_snapshot=hash_to_bytes("aaaaaabbbeb6cf9efd5b920a8453e1e07157b6cd"),
    )

    assert swh_scheduler.origin_visit_stats_get([("cavabarder", "hg")]) == [
        expected_visit_stats
    ]


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
    assert ovs.last_eventful == DATE1 + 5 * ONE_DAY
    assert ovs.last_uneventful is None
    assert ovs.last_failed is None
    assert ovs.last_notfound is None
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

    expected_visit_stats = OriginVisitStats(
        url="foo",
        visit_type="git",
        last_eventful=DATE1,
        last_uneventful=None,
        last_failed=None,
        last_notfound=None,
        last_snapshot=hash_to_bytes("aaaaaabbbeb6cf9efd5b920a8453e1e07157b6cd"),
    )

    assert swh_scheduler.origin_visit_stats_get([("foo", "git")]) == [
        expected_visit_stats
    ]


def test_journal_client_origin_visit_status_several_upsert(swh_scheduler):
    """A duplicated message must be ignored

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

    expected_visit_stats = OriginVisitStats(
        url="foo",
        visit_type="git",
        last_eventful=DATE1,
        last_uneventful=DATE2,
        last_failed=None,
        last_notfound=None,
        last_snapshot=hash_to_bytes("aaaaaabbbeb6cf9efd5b920a8453e1e07157b6cd"),
    )

    assert swh_scheduler.origin_visit_stats_get([("foo", "git")]) == [
        expected_visit_stats
    ]


LINUX_STATUSES = [
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 1,
        "status": "full",
        "date": datetime.datetime(2015, 7, 9, 21, 9, 24, tzinfo=datetime.timezone.utc),
        "type": "git",
        "snapshot": hash_to_bytes("62841f16e8592344b51afc272b98e98108f0b5c5"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 2,
        "status": "full",
        "date": datetime.datetime(
            2016, 2, 23, 18, 5, 23, 312045, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("26befdbf4b393d1e03aa80f2a955bc38b241a8ac"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 3,
        "status": "full",
        "date": datetime.datetime(
            2016, 3, 28, 1, 35, 6, 554111, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("a07fe7f5bfacf1db47450f04340c7a7b45d3da74"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 4,
        "status": "full",
        "date": datetime.datetime(
            2016, 6, 18, 1, 22, 24, 808485, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("ce21f317d9fd74bb4af31b06207240031f4b2516"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 5,
        "status": "full",
        "date": datetime.datetime(
            2016, 8, 14, 12, 10, 0, 536702, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("fe0eac19141fdcdf039e8f5ace5e41b9a2398a49"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 6,
        "status": "full",
        "date": datetime.datetime(
            2016, 8, 17, 9, 16, 22, 52065, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("6903f868df6d94a444818b50becd4835b29be274"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 7,
        "status": "full",
        "date": datetime.datetime(
            2016, 8, 29, 18, 55, 54, 153721, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("6bd66993839dc897aa15a443c4e3b9164f811499"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 8,
        "status": "full",
        "date": datetime.datetime(
            2016, 9, 7, 8, 44, 47, 861875, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("c06a965f855f4d73c84fbefd859f7df507187d9c"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 9,
        "status": "full",
        "date": datetime.datetime(
            2016, 9, 14, 10, 36, 21, 505296, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("40a5381e2b6c0c04775c5b7e7b37284c3affc129"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 10,
        "status": "full",
        "date": datetime.datetime(
            2016, 9, 23, 10, 14, 2, 169862, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("2252b4d49b9e786eb777a0097a42e51c7193bb9c"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 11,
        "status": "partial",
        "date": datetime.datetime(
            2017, 2, 16, 7, 53, 39, 467657, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("1a8893e6a86f444e8be8e7bda6cb34fb1735a00e"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 12,
        "status": "full",
        "date": datetime.datetime(
            2017, 5, 4, 19, 40, 9, 336451, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("5c9b454dae068281d48445c79a2408a2c243bca2"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 13,
        "status": "full",
        "date": datetime.datetime(
            2017, 9, 7, 18, 43, 13, 21746, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("3e3045be901bacc7594176e79ba13fe030f601e2"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 14,
        "status": "full",
        "date": datetime.datetime(
            2017, 9, 9, 5, 14, 33, 466107, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("1ef6b371c85746e260af3c8413f802fdd72cec9d"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 15,
        "status": "full",
        "date": datetime.datetime(
            2017, 9, 9, 17, 18, 54, 307789, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("add0a02362f3e59314e80fa601edd6092a64ba6c"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 16,
        "status": "full",
        "date": datetime.datetime(
            2017, 9, 10, 5, 29, 1, 462971, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("887b853081ddff2ea21b35ae59461b7d28e73934"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 17,
        "status": "full",
        "date": datetime.datetime(
            2017, 9, 10, 17, 35, 20, 158515, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("f3bba40a96477bc4e136caa3bf95fe4ca0e012a2"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 18,
        "status": "full",
        "date": datetime.datetime(
            2017, 9, 11, 5, 49, 58, 300518, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("20ec8f21e9b740b59bbd04e6384308957b9897d5"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 19,
        "status": "full",
        "date": datetime.datetime(
            2017, 9, 11, 18, 0, 15, 37345, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("20ec8f21e9b740b59bbd04e6384308957b9897d5"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 20,
        "status": "full",
        "date": datetime.datetime(
            2017, 9, 12, 6, 6, 34, 703343, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("20ec8f21e9b740b59bbd04e6384308957b9897d5"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 21,
        "status": "full",
        "date": datetime.datetime(
            2017, 9, 12, 18, 12, 35, 344511, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("e5b1bd38d5b06cf99f314cf839d54d6960e166a1"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 22,
        "status": "full",
        "date": datetime.datetime(
            2017, 9, 13, 6, 26, 36, 580675, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("82d91171ab474a50b4653d49af9fab1b9f1af9c1"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 23,
        "status": "partial",
        "date": datetime.datetime(
            2017, 10, 2, 9, 32, 32, 890258, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("1a8893e6a86f444e8be8e7bda6cb34fb1735a00e"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 24,
        "status": "full",
        "date": datetime.datetime(
            2017, 10, 6, 4, 27, 51, 146287, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("759aef822f07ef790a6505bf6b275b570838de0e"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 25,
        "status": "full",
        "date": datetime.datetime(
            2017, 10, 8, 11, 54, 25, 582463, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("1b327857f72e8cdeb4d177487274af18b2690312"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 26,
        "status": "full",
        "date": datetime.datetime(
            2017, 10, 18, 6, 25, 9, 481005, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("8326eabafa96fd12c5fbd97355d372cd7ab83e23"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 27,
        "status": "full",
        "date": datetime.datetime(
            2017, 11, 5, 22, 45, 54, 59797, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("6cd9089faa91789fb441eebb90243b0fead62073"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 28,
        "status": "full",
        "date": datetime.datetime(
            2017, 11, 21, 19, 37, 42, 517795, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("863d469d26313c436175b4db04293675972bf96c"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 29,
        "status": "partial",
        "date": datetime.datetime(
            2018, 3, 8, 0, 0, 53, 641972, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": None,
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 30,
        "status": "partial",
        "date": datetime.datetime(
            2018, 6, 1, 12, 48, 0, 902827, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": None,
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 31,
        "status": "partial",
        "date": datetime.datetime(
            2018, 7, 1, 3, 51, 21, 433801, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": None,
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 32,
        "status": "partial",
        "date": datetime.datetime(
            2018, 7, 4, 9, 1, 25, 333541, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": None,
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 33,
        "status": "ongoing",
        "date": datetime.datetime(
            2018, 7, 4, 12, 3, 2, 684804, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": None,
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 34,
        "status": "full",
        "date": datetime.datetime(
            2018, 7, 4, 12, 3, 59, 959837, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("91ecbc6b3d9e243b2b1f9fc9614ed685fec47372"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 35,
        "status": "partial",
        "date": datetime.datetime(
            2018, 7, 5, 17, 18, 5, 610308, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": None,
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 36,
        "status": "partial",
        "date": datetime.datetime(
            2018, 8, 1, 7, 26, 22, 603055, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": None,
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 37,
        "status": "partial",
        "date": datetime.datetime(
            2018, 8, 23, 11, 53, 6, 553328, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": None,
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 38,
        "status": "partial",
        "date": datetime.datetime(
            2018, 8, 23, 22, 43, 57, 639257, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": None,
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 39,
        "status": "partial",
        "date": datetime.datetime(
            2018, 8, 31, 6, 32, 17, 742578, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": None,
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 40,
        "status": "partial",
        "date": datetime.datetime(
            2018, 8, 31, 18, 53, 23, 407469, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": None,
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 41,
        "status": "partial",
        "date": datetime.datetime(
            2018, 9, 6, 23, 54, 1, 808678, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": None,
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 42,
        "status": "partial",
        "date": datetime.datetime(
            2018, 9, 7, 20, 8, 54, 581862, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": None,
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 43,
        "status": "partial",
        "date": datetime.datetime(
            2018, 9, 8, 21, 36, 3, 892238, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": None,
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 44,
        "status": "partial",
        "date": datetime.datetime(
            2018, 9, 9, 12, 40, 48, 143883, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": None,
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 45,
        "status": "partial",
        "date": datetime.datetime(
            2018, 9, 17, 6, 15, 20, 340219, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": None,
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 46,
        "status": "partial",
        "date": datetime.datetime(
            2018, 9, 18, 2, 22, 45, 500261, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": None,
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 47,
        "status": "partial",
        "date": datetime.datetime(
            2018, 9, 18, 6, 38, 28, 79159, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": None,
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 48,
        "status": "full",
        "date": datetime.datetime(
            2018, 9, 22, 13, 51, 35, 886745, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("768a69f205d4fa34690c3e9a34120841194f3fa8"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 49,
        "status": "full",
        "date": datetime.datetime(
            2018, 9, 23, 1, 14, 36, 694248, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("768a69f205d4fa34690c3e9a34120841194f3fa8"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 50,
        "status": "full",
        "date": datetime.datetime(
            2018, 9, 24, 6, 23, 8, 81958, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("4928e66b3d5a37075d15daaecd9a3e21abf249cf"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 51,
        "status": "full",
        "date": datetime.datetime(
            2018, 9, 30, 15, 54, 18, 68701, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("df1d58dbaf6f4a21213dce641a4c67ab4e3e2e97"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 52,
        "status": "full",
        "date": datetime.datetime(
            2018, 9, 30, 20, 11, 45, 338783, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("df1d58dbaf6f4a21213dce641a4c67ab4e3e2e97"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 53,
        "status": "full",
        "date": datetime.datetime(
            2018, 10, 4, 18, 58, 58, 156154, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("7709862daae730e3a043b54688244b7df213405d"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 54,
        "status": "full",
        "date": datetime.datetime(
            2018, 10, 13, 5, 8, 9, 273129, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("4cc32cd1562734e13db59629c87a1e3ae61b76a8"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 55,
        "status": "full",
        "date": datetime.datetime(
            2018, 10, 13, 19, 9, 33, 773125, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("4cc32cd1562734e13db59629c87a1e3ae61b76a8"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 56,
        "status": "full",
        "date": datetime.datetime(
            2018, 10, 21, 5, 22, 50, 678295, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("2d41805937ba31d3bb6db37cfa8a9a236aa9810f"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 57,
        "status": "full",
        "date": datetime.datetime(
            2018, 12, 27, 7, 47, 0, 154971, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("7cc19b4a7b59f0bba206b02c7aac4df8629fa652"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 58,
        "status": "full",
        "date": datetime.datetime(
            2019, 3, 30, 10, 44, 8, 484859, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("4aae5a00446aa069d93067624947b50eb2ff76db"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 59,
        "status": "full",
        "date": datetime.datetime(
            2019, 6, 21, 19, 28, 48, 774654, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("cb0e56448fea3c69a27925398965f807dcba8f24"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 60,
        "status": "full",
        "date": datetime.datetime(
            2019, 8, 5, 2, 16, 12, 840378, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("8e9c503b5de5488d17cf58b8ce1c7c5f8278d707"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 61,
        "status": "full",
        "date": datetime.datetime(
            2019, 8, 25, 14, 4, 7, 603463, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("eb8087624d47f6e8ee89692df041b2f568fb0e5f"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 62,
        "status": "ongoing",
        "date": datetime.datetime(
            2019, 12, 16, 13, 44, 56, 685885, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": None,
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 63,
        "status": "full",
        "date": datetime.datetime(
            2020, 1, 20, 19, 50, 46, 750039, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("cabcc7d7bf639bbe1cc3b41989e1806618dd5764"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 64,
        "status": "full",
        "date": datetime.datetime(
            2020, 3, 19, 23, 29, 59, 614232, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("89eed60d46be8b8963a1a2268762aee5bbb41038"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 65,
        "status": "created",
        "date": datetime.datetime(
            2020, 8, 24, 9, 22, 41, 181224, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": None,
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 65,
        "status": "full",
        "date": datetime.datetime(
            2020, 8, 24, 11, 51, 54, 472736, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("b16664848afbd3e867e8fce516ef15c1772679b2"),
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 66,
        "status": "created",
        "date": datetime.datetime(
            2020, 9, 21, 17, 7, 41, 94459, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": None,
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 66,
        "status": "partial",
        "date": datetime.datetime(
            2020, 9, 21, 17, 12, 11, 930011, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": None,
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 67,
        "status": "created",
        "date": datetime.datetime(
            2020, 9, 21, 19, 15, 24, 238712, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": None,
    },
    {
        "origin": "https://github.com/torvalds/linux",
        "visit": 67,
        "status": "full",
        "date": datetime.datetime(
            2020, 9, 21, 21, 55, 1, 586191, tzinfo=datetime.timezone.utc
        ),
        "type": "git",
        "snapshot": hash_to_bytes("c7beb2432b7e93c4cf6ab09cd194c7c1998df2f9"),
    },
]

try:
    # We explicitly reset this fixture, we can ignore the Hypothesis health check
    ignore_fixture_reset: Callable = settings(
        suppress_health_check=(HealthCheck.function_scoped_fixture,)
    )
except AttributeError:
    # Old Hypothesis versions have this as a warning
    ignore_fixture_reset = pytest.mark.filterwarnings(
        "ignore:.*uses the .* fixture, which is reset.*"
    )


@ignore_fixture_reset
@given(strategies.permutations(LINUX_STATUSES))
def test_journal_client_linux(swh_scheduler, statuses):
    db = swh_scheduler.get_db()
    try:
        with db.transaction() as cur:
            cur.execute("delete from origin_visit_stats")
    finally:
        swh_scheduler.put_db(db)

    for batch in grouper(statuses, n=10):
        process_journal_objects({"origin_visit_status": batch}, scheduler=swh_scheduler)

    stats = swh_scheduler.origin_visit_stats_get(
        [("https://github.com/torvalds/linux", "git")]
    )[0]

    assert stats.last_eventful == datetime.datetime(
        2020, 9, 21, 21, 55, 1, 586191, tzinfo=datetime.timezone.utc
    )
    assert stats.last_uneventful is None or stats.last_uneventful <= datetime.datetime(
        2018, 10, 13, 19, 9, 33, 773125, tzinfo=datetime.timezone.utc
    )
    assert stats.last_failed is None or stats.last_failed <= datetime.datetime(
        2020, 9, 21, 17, 12, 11, 930011, tzinfo=datetime.timezone.utc
    )
    assert stats.last_snapshot == hash_to_bytes(
        "c7beb2432b7e93c4cf6ab09cd194c7c1998df2f9"
    )
