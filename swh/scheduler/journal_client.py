# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime
from typing import Dict, List, Optional, Tuple

import attr

from swh.scheduler.interface import SchedulerInterface
from swh.scheduler.model import OriginVisitStats

msg_type = "origin_visit_status"


def max_date(d1: Optional[datetime], d2: Optional[datetime]) -> datetime:
    """Return the max date of the visit stats

    """
    if d1 is None and d2 is None:
        raise ValueError("At least one date should be a valid datetime")
    if d1 is None:
        assert d2 is not None  # make mypy happy
        return d2
    if d2 is None:
        return d1
    return max(d1, d2)


def process_journal_objects(
    messages: Dict[str, List[Dict]], *, scheduler: SchedulerInterface
) -> None:
    """Read messages from origin_visit_status journal topics, then inserts them in the
    scheduler "origin_visit_stats" table.

    Worker function for `JournalClient.process(worker_fn)`, after
    currification of `scheduler` and `task_names`.

    """
    assert set(messages) <= {
        msg_type
    }, f"Got unexpected {', '.join(set(messages) - set([msg_type]))} message types"
    assert msg_type in messages, f"Expected {msg_type} messages"

    origin_visit_stats: Dict[Tuple[str, str], Dict] = {}
    for msg_dict in messages[msg_type]:
        if msg_dict["status"] in ("created", "ongoing"):
            continue
        origin = msg_dict["origin"]
        visit_type = msg_dict["type"]
        empty_object = {
            "url": origin,
            "visit_type": visit_type,
            "last_uneventful": None,
            "last_eventful": None,
            "last_failed": None,
            "last_notfound": None,
            "last_snapshot": None,
        }
        pk = origin, visit_type
        if pk not in origin_visit_stats:
            visit_stats = scheduler.origin_visit_stats_get(origin, visit_type)
            origin_visit_stats[pk] = (
                attr.asdict(visit_stats) if visit_stats else empty_object
            )

        visit_stats_d = origin_visit_stats[pk]

        if msg_dict["status"] == "not_found":
            visit_stats_d["last_notfound"] = max_date(
                msg_dict["date"], visit_stats_d.get("last_notfound")
            )
        elif msg_dict["snapshot"] is None:
            visit_stats_d["last_failed"] = max_date(
                msg_dict["date"], visit_stats_d.get("last_failed")
            )
        else:  # visit with snapshot, something happened
            if visit_stats_d["last_snapshot"] is None:
                # first time visit with snapshot, we keep relevant information
                visit_stats_d["last_eventful"] = msg_dict["date"]
                visit_stats_d["last_snapshot"] = msg_dict["snapshot"]
            else:
                # visit with snapshot already stored, last_eventful should already be
                # stored
                assert visit_stats_d["last_eventful"] is not None
                latest_recorded_visit_date = max_date(
                    visit_stats_d["last_eventful"], visit_stats_d["last_uneventful"]
                )
                current_status_date = msg_dict["date"]
                previous_snapshot = visit_stats_d["last_snapshot"]
                if msg_dict["snapshot"] != previous_snapshot:
                    if (
                        latest_recorded_visit_date
                        and current_status_date < latest_recorded_visit_date
                    ):
                        # out of order message so ignored
                        continue
                    # new eventful visit (new snapshot)
                    visit_stats_d["last_eventful"] = current_status_date
                    visit_stats_d["last_snapshot"] = msg_dict["snapshot"]
                else:
                    # same snapshot as before
                    if (
                        latest_recorded_visit_date
                        and current_status_date < latest_recorded_visit_date
                    ):
                        # we receive an old message which is an earlier "eventful" event
                        # than what we had, we consider the last_eventful event as
                        # actually an uneventful event. The true eventful message is the
                        # current one
                        visit_stats_d["last_uneventful"] = visit_stats_d[
                            "last_eventful"
                        ]
                        visit_stats_d["last_eventful"] = current_status_date
                    else:
                        # uneventful event
                        visit_stats_d["last_uneventful"] = current_status_date

    scheduler.origin_visit_stats_upsert(
        OriginVisitStats(**ovs) for ovs in origin_visit_stats.values()
    )
