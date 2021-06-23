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


def max_date(*dates: Optional[datetime]) -> datetime:
    """Return the max date of given (possibly None) dates

    At least one date must be not None.
    """
    filtered_dates = [d for d in dates if d is not None]
    if not filtered_dates:
        raise ValueError("At least one date should be a valid datetime")

    return max(filtered_dates)


def update_next_position_offset(visit_stats: Dict, increment: int) -> None:
    """Update the next position offset according to existing value and the increment. The
    resulting value must be a positive integer.

    """
    visit_stats["next_position_offset"] = max(
        0, visit_stats["next_position_offset"] + increment
    )


def process_journal_objects(
    messages: Dict[str, List[Dict]], *, scheduler: SchedulerInterface
) -> None:
    """Read messages from origin_visit_status journal topic to update "origin_visit_stats"
    information on (origin, visit_type). The goal is to compute visit stats information
    per origin and visit_type: last_eventful, last_uneventful, last_failed,
    last_notfound, last_snapshot, ...

    Details:

        - This journal consumes origin visit status information for final visit status
          ("full", "partial", "failed", "not_found"). It drops the information on non
          final visit statuses ("ongoing", "created").

        - The snapshot is used to determine the "eventful/uneventful" nature of the
          origin visit status.

        - When no snapshot is provided, the visit is considered as failed so the
          last_failed column is updated.

        - As there is no time guarantee when reading message from the topic, the code
          tries to keep the data in the most timely ordered as possible.

        - Compared to what is already stored in the origin_visit_stats table, only most
          recent information is kept.

        - This updates the `next_visit_queue_position` (time at which some new objects
          are expected to be added for the origin), and `next_position_offset` (duration
          that we expect to wait between visits of this origin).

    This is a worker function to be used with `JournalClient.process(worker_fn)`, after
    currification of `scheduler` and `task_names`.

    """
    assert set(messages) <= {
        msg_type
    }, f"Got unexpected {', '.join(set(messages) - set([msg_type]))} message types"
    assert msg_type in messages, f"Expected {msg_type} messages"

    interesting_messages = [
        msg
        for msg in messages[msg_type]
        if "type" in msg and msg["status"] not in ("created", "ongoing")
    ]

    if not interesting_messages:
        return

    origin_visit_stats: Dict[Tuple[str, str], Dict] = {
        (visit_stats.url, visit_stats.visit_type): attr.asdict(visit_stats)
        for visit_stats in scheduler.origin_visit_stats_get(
            list(set((vs["origin"], vs["type"]) for vs in interesting_messages))
        )
    }
    # Use the default values from the model object
    empty_object = {
        field.name: field.default if field.default != attr.NOTHING else None
        for field in attr.fields(OriginVisitStats)
    }

    for msg_dict in interesting_messages:
        origin = msg_dict["origin"]
        visit_type = msg_dict["type"]
        pk = origin, visit_type
        if pk not in origin_visit_stats:
            origin_visit_stats[pk] = {
                **empty_object,
                "url": origin,
                "visit_type": visit_type,
            }

        visit_stats_d = origin_visit_stats[pk]

        if msg_dict["status"] == "not_found":
            visit_stats_d["last_notfound"] = max_date(
                msg_dict["date"], visit_stats_d.get("last_notfound")
            )
            update_next_position_offset(visit_stats_d, 1)  # visit less often
        elif msg_dict["status"] == "failed" or msg_dict["snapshot"] is None:
            visit_stats_d["last_failed"] = max_date(
                msg_dict["date"], visit_stats_d.get("last_failed")
            )
            update_next_position_offset(visit_stats_d, 1)  # visit less often
        else:  # visit with snapshot, something happened
            if visit_stats_d["last_snapshot"] is None:
                # first time visit with snapshot, we keep relevant information
                visit_stats_d["last_eventful"] = msg_dict["date"]
                visit_stats_d["last_snapshot"] = msg_dict["snapshot"]
            else:
                # last_snapshot is set, so an eventful visit should have previously been
                # recorded
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
                    # Visit this origin more often in the future
                    update_next_position_offset(visit_stats_d, -2)
                else:
                    # same snapshot as before
                    if (
                        latest_recorded_visit_date
                        and current_status_date < latest_recorded_visit_date
                    ):
                        # we receive an old message which is an earlier "eventful" event
                        # than what we had, we consider the last_eventful event as
                        # actually an uneventful event.
                        # The last uneventful visit remains the most recent:
                        # max, previously computed
                        visit_stats_d["last_uneventful"] = latest_recorded_visit_date
                        # The eventful visit remains the oldest one: min
                        visit_stats_d["last_eventful"] = min(
                            visit_stats_d["last_eventful"], current_status_date
                        )
                        # Visit this origin less often in the future
                        update_next_position_offset(visit_stats_d, 1)
                    elif (
                        latest_recorded_visit_date
                        and current_status_date == latest_recorded_visit_date
                    ):
                        # A duplicated message must be ignored to avoid
                        # populating the last_uneventful message
                        continue
                    else:
                        # uneventful event
                        visit_stats_d["last_uneventful"] = current_status_date
                        # Visit this origin less often in the future
                        update_next_position_offset(visit_stats_d, 1)

    scheduler.origin_visit_stats_upsert(
        OriginVisitStats(**ovs) for ovs in origin_visit_stats.values()
    )
