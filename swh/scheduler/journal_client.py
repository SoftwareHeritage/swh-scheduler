# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
from datetime import datetime, timedelta
import random
from typing import Dict, List, Optional, Tuple

import attr

from swh.scheduler.interface import SchedulerInterface
from swh.scheduler.model import LastVisitStatus, OriginVisitStats
from swh.scheduler.utils import utcnow

msg_type = "origin_visit_status"

DISABLE_ORIGIN_THRESHOLD = 3
"""Threshold to disable failing origins"""

MAX_NEXT_POSITION_OFFSET = 10
"""Max next position offset to avoid date computation overflow"""


def max_date(*dates: Optional[datetime]) -> datetime:
    """Return the max date of given (possibly None) dates

    At least one date must be not None.
    """
    filtered_dates = [d for d in dates if d is not None]
    if not filtered_dates:
        raise ValueError("At least one date should be a valid datetime")

    return max(filtered_dates)


def from_position_offset_to_days(position_offset: int) -> int:
    """Compute position offset to interval in days. Note that this does not bound the
       position_offset input so client code should limit the date computation to avoid
       overflow errors.

        - index in [0:1]: interval 1 day
        - index in [2:4]: interval 2 days
        - index in [5:+inf]: interval `4^(index-4)` days

    Args:
        position_offset: The actual position offset for a given visit stats

    Returns:
        The offset as an interval in number of days.

    """
    assert position_offset >= 0
    if position_offset < 2:
        result = 1
    elif position_offset < 5:
        result = 2
    else:
        result = 4 ** (position_offset - 4)
    return result


def next_visit_queue_position(
    queue_position_per_visit_type: Dict, visit_stats: Dict
) -> datetime:
    """Compute the next visit queue position for the given visit_stats.

    This takes the visit_stats next_position_offset value and compute a corresponding
    interval in days (with a random fudge factor of -/+ 10% range to avoid scheduling
    burst for hosters). Then computes out of this visit interval and the current visit
    stats's position in the queue a new position.

    As an implementation detail, if the visit stats does not have a queue position yet,
    this fallbacks to use the current global position (for the same visit type as the
    visit stats) to compute the new position in the queue. If there is no global state
    yet for the visit type, this starts up using the ``utcnow`` function as default
    value.

    Args:
        queue_position_per_visit_type: The global state of the queue per visit type
        visit_stats: The actual visit information to compute the next position for

    Returns:
        The actual next visit queue position for that visit stats

    """
    days = from_position_offset_to_days(visit_stats["next_position_offset"])
    random_fudge_factor = random.uniform(-0.1, 0.1)
    visit_interval = timedelta(days=days * (1 + random_fudge_factor))
    # Use the current queue position per visit type as starting position if none is
    # already set
    default_queue_position = queue_position_per_visit_type.get(
        visit_stats["visit_type"], utcnow()
    )
    current_position = (
        visit_stats["next_visit_queue_position"]
        if visit_stats.get("next_visit_queue_position")
        else default_queue_position
    )
    return current_position + visit_interval


def get_last_status(
    incoming_visit_status: Dict, known_visit_stats: Dict
) -> Tuple[LastVisitStatus, Optional[bool]]:
    """Determine the `last_visit_status` and eventfulness of an origin according to
    the received visit_status object, and the state of the origin_visit_stats in db.

    Note that at the time this function is called, out of order messages were already
    discarded. Thus why the implementation is rather simple.

    Args:
        incoming_visit_status: Incoming visit status read ouf of the journal
        known_visit_stats: Visit stats already registered in the backend

    Returns:
        A tuple of (LastVisitStatus, Optional[bool]). LastVisitStatus represents the
        successfulness of the visit. Optional[bool] represents whether the snapshot is
        fresher than before (True/False) or None if there is no snapshot at all.

    """

    status = incoming_visit_status["status"]
    if status in ("not_found", "failed"):
        return LastVisitStatus(status), None

    assert status in ("full", "partial")

    if incoming_visit_status["snapshot"] is None:
        return LastVisitStatus.failed, None

    if incoming_visit_status["snapshot"] != known_visit_stats.get("last_snapshot"):
        return LastVisitStatus.successful, True

    return LastVisitStatus.successful, False


def process_journal_objects(
    messages: Dict[str, List[Dict]], *, scheduler: SchedulerInterface
) -> None:
    f"""Read messages from origin_visit_status journal topic to update "origin_visit_stats"
    information on (origin, visit_type). The goal is to compute visit stats information
    per origin and visit_type: `last_successful`, `last_visit`, `last_visit_status`, ...

    Details:

        - This journal consumes origin visit status information for final visit
          status (`"full"`, `"partial"`, `"failed"`, `"not_found"`). It drops
          the information of non final visit statuses (`"ongoing"`,
          `"created"`).

        - This journal client only considers messages that arrive in
          chronological order. Messages that arrive out of order (i.e. with a
          date field smaller than the latest recorded visit of the origin) are
          ignored. This is a tradeoff between correctness and simplicity of
          implementation [1]_.

        - The snapshot is used to determine the eventful or uneventful nature of
          the origin visit.

        - When no snapshot is provided, the visit is considered as failed.

        - Finally, the `next_visit_queue_position` (time at which some new objects
          are expected to be added for the origin), and `next_position_offset` (duration
          that we expect to wait between visits of this origin) are updated.

        - When visits fails at least {DISABLE_ORIGIN_THRESHOLD} times in a row, the
          origins are disabled in the scheduler table. It's up to the lister to activate
          those back when they are listed again.

    This is a worker function to be used with `JournalClient.process(worker_fn)`, after
    currification of `scheduler` and `task_names`.

    .. [1] Ignoring out of order messages makes the initialization of the
      origin_visit_status table (from a full journal) less deterministic: only the
      `last_visit`, `last_visit_state` and `last_successful` fields are guaranteed
      to be exact, the `next_position_offset` field is a best effort estimate
      (which should converge once the client has run for a while on in-order
      messages).

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
    existing_origin_visit_stats = copy.deepcopy(origin_visit_stats)

    # Use the default values from the model object
    empty_object = {
        field.name: field.default if field.default != attr.NOTHING else None
        for field in attr.fields(OriginVisitStats)
    }

    disabled_urls: List[str] = []

    # Retrieve the global queue state
    queue_position_per_visit_type = scheduler.visit_scheduler_queue_position_get()

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

        if (
            visit_stats_d.get("last_visit")
            and msg_dict["date"] <= visit_stats_d["last_visit"]
        ):
            # message received out of order, ignore
            continue

        # Compare incoming message to known status of the origin, to determine
        # eventfulness
        last_visit_status, eventful = get_last_status(msg_dict, visit_stats_d)

        # Update the position offset according to the visit status,
        # if we had already visited this origin before.

        if visit_stats_d.get("last_visit"):
            # Update the next position offset according to the existing value and the
            # eventfulness of the visit.
            increment = -2 if eventful else 1
            # Limit the next_position_offset for acceptable date computations
            current_offset = min(
                visit_stats_d["next_position_offset"] + increment,
                MAX_NEXT_POSITION_OFFSET,
            )
            visit_stats_d["next_position_offset"] = max(0, current_offset)
            # increment the counter when last_visit_status is the same
            same_visit_status = last_visit_status == visit_stats_d["last_visit_status"]
        else:
            same_visit_status = False

        # Record current visit date as highest known date (we've rejected out of order
        # messages earlier).
        visit_stats_d["last_visit"] = msg_dict["date"]
        visit_stats_d["last_visit_status"] = last_visit_status

        # Record last successful visit date
        if last_visit_status == LastVisitStatus.successful:
            visit_stats_d["last_successful"] = max_date(
                msg_dict["date"], visit_stats_d.get("last_successful")
            )
            visit_stats_d["last_snapshot"] = msg_dict["snapshot"]

        # Update the next visit queue position (which will be used solely for origin
        # without any last_update, cf. the dedicated scheduling policy
        # "origins_without_last_update")
        visit_stats_d["next_visit_queue_position"] = next_visit_queue_position(
            queue_position_per_visit_type, visit_stats_d
        )

        visit_stats_d["successive_visits"] = (
            visit_stats_d["successive_visits"] + 1 if same_visit_status else 1
        )

        # Disable recurring failing/not-found origins
        if (
            visit_stats_d["last_visit_status"]
            in [LastVisitStatus.not_found, LastVisitStatus.failed]
        ) and visit_stats_d["successive_visits"] >= DISABLE_ORIGIN_THRESHOLD:
            disabled_urls.append(visit_stats_d["url"])

    # Only upsert changed values
    to_upsert = []
    for key, ovs in origin_visit_stats.items():
        if (
            key not in existing_origin_visit_stats
            or ovs != existing_origin_visit_stats[key]
        ):
            to_upsert.append(OriginVisitStats(**ovs))

    if to_upsert:
        scheduler.origin_visit_stats_upsert(to_upsert)

    # Disable any origins if any
    if disabled_urls:
        disabled_origins = []
        for url in disabled_urls:
            origins = scheduler.get_listed_origins(url=url).results
            if len(origins) > 0:
                origin = attr.evolve(origins[0], enabled=False)
                disabled_origins.append(origin)

        if disabled_origins:
            scheduler.record_listed_origins(disabled_origins)
