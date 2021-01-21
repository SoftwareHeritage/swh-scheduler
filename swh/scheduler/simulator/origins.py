# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""This module implements a model of the frequency of updates of an origin
and how long it takes to load it.

For each origin, a commit frequency is chosen deterministically based on the
hash of its URL and assume all origins were created on an arbitrary epoch.
From this we compute a number of commits, that is the product of these two.

And the run time of a load task is approximated as proportional to the number
of commits since the previous visit of the origin (possibly 0)."""

from datetime import datetime, timedelta, timezone
import hashlib
import logging
import os
from typing import Generator, Iterator, List, Optional, Tuple
import uuid

import attr
from simpy import Event

from swh.model.model import OriginVisitStatus
from swh.scheduler.model import ListedOrigin, OriginVisitStats

from .common import Environment, Queue, Task, TaskEvent

logger = logging.getLogger(__name__)


_nb_generated_origins = 0


def generate_listed_origin(
    lister_id: uuid.UUID, now: Optional[datetime] = None
) -> ListedOrigin:
    """Returns a globally unique new origin. Seed the `last_update` value
    according to the OriginModel and the passed timestamp.

    Arguments:
      lister: instance of the lister that generated this origin
      now: time of listing, to emulate last_update (defaults to :func:`datetime.now`)
    """
    global _nb_generated_origins
    _nb_generated_origins += 1
    assert _nb_generated_origins < 10 ** 6, "Too many origins!"

    if now is None:
        now = datetime.now(tz=timezone.utc)

    url = f"https://example.com/{_nb_generated_origins:6d}.git"
    visit_type = "git"
    origin = OriginModel(visit_type, url)

    return ListedOrigin(
        lister_id=lister_id,
        url=url,
        visit_type=visit_type,
        last_update=origin.get_last_update(now),
    )


class OriginModel:
    MIN_RUN_TIME = 0.5
    """Minimal run time for a visit (retrieved from production data)"""

    MAX_RUN_TIME = 7200
    """Max run time for a visit"""

    PER_COMMIT_RUN_TIME = 0.1
    """Run time per commit"""

    EPOCH = datetime(2015, 9, 1, 0, 0, 0, tzinfo=timezone.utc)
    """The origin of all origins (at least according to Software Heritage)"""

    def __init__(self, type: str, origin: str):
        self.type = type
        self.origin = origin

    def seconds_between_commits(self):
        """Returns a random 'average time between two commits' of this origin,
        used to estimate the run time of a load task, and how much the loading
        architecture is lagging behind origin updates."""
        n_bytes = 2
        num_buckets = 2 ** (8 * n_bytes)

        # Deterministic seed to generate "random" characteristics of this origin
        bucket = int.from_bytes(
            hashlib.md5(self.origin.encode()).digest()[0:n_bytes], "little"
        )

        # minimum: 1 second (bucket == 0)
        # max: 10 years (bucket == num_buckets - 1)
        ten_y = 10 * 365 * 24 * 3600

        return ten_y ** (bucket / num_buckets)
        # return 1 + (ten_y - 1) * (bucket / (num_buckets - 1))

    def get_last_update(self, now: datetime):
        """Get the last_update value for this origin.

        We assume that the origin had its first commit at `EPOCH`, and that one
        commit happened every `self.seconds_between_commits()`. This returns
        the last commit date before or equal to `now`.
        """
        _, time_since_last_commit = divmod(
            (now - self.EPOCH).total_seconds(), self.seconds_between_commits()
        )

        return now - timedelta(seconds=time_since_last_commit)

    def load_task_characteristics(
        self, env: Environment, stats: Optional[OriginVisitStats]
    ) -> Tuple[float, bool, str]:
        """Returns the (run_time, eventfulness, end_status) of the next
        origin visit."""
        if stats and stats.last_eventful:
            time_since_last_successful_run = env.time - stats.last_eventful
        else:
            time_since_last_successful_run = timedelta(days=365)

        seconds_between_commits = self.seconds_between_commits()
        logger.debug(
            "Interval between commits: %s", timedelta(seconds=seconds_between_commits)
        )

        seconds_since_last_successful = time_since_last_successful_run.total_seconds()
        if seconds_since_last_successful < seconds_between_commits:
            # No commits since last visit => uneventful
            return (self.MIN_RUN_TIME, False, "full")
        else:
            n_commits = seconds_since_last_successful / seconds_between_commits
            run_time = self.MIN_RUN_TIME + self.PER_COMMIT_RUN_TIME * n_commits
            if run_time > self.MAX_RUN_TIME:
                return (self.MAX_RUN_TIME, False, "partial")
            else:
                return (run_time, True, "full")


def lister_process(
    env: Environment, lister_id: uuid.UUID
) -> Generator[Event, Event, None]:
    """Every hour, generate new origins and update the `last_update` field for
    the ones this process generated in the past"""
    NUM_NEW_ORIGINS = 100

    origins: List[ListedOrigin] = []

    while True:
        updated_origins = []
        for origin in origins:
            model = OriginModel(origin.visit_type, origin.url)
            updated_origins.append(
                attr.evolve(origin, last_update=model.get_last_update(env.time))
            )

        origins = updated_origins
        origins.extend(
            generate_listed_origin(lister_id, now=env.time)
            for _ in range(NUM_NEW_ORIGINS)
        )

        env.scheduler.record_listed_origins(origins)

        yield env.timeout(3600)


def load_task_process(
    env: Environment, task: Task, status_queue: Queue
) -> Iterator[Event]:
    """A loading task. This pushes OriginVisitStatus objects to the
    status_queue to simulate the visible outcomes of the task.

    Uses the `load_task_duration` function to determine its run time.
    """
    # This is cheating; actual tasks access the state from the storage, not the
    # scheduler
    pk = task.origin, task.visit_type
    visit_stats = env.scheduler.origin_visit_stats_get([pk])
    stats: Optional[OriginVisitStats] = visit_stats[0] if len(visit_stats) > 0 else None
    last_snapshot = stats.last_snapshot if stats else None

    status = OriginVisitStatus(
        origin=task.origin,
        visit=42,
        type=task.visit_type,
        status="created",
        date=env.time,
        snapshot=None,
    )
    origin_model = OriginModel(task.visit_type, task.origin)
    (run_time, eventful, end_status) = origin_model.load_task_characteristics(
        env, stats
    )
    logger.debug("%s task %s origin=%s: Start", env.time, task.visit_type, task.origin)
    yield status_queue.put(TaskEvent(task=task, status=status))
    yield env.timeout(run_time)
    logger.debug("%s task %s origin=%s: End", env.time, task.visit_type, task.origin)

    new_snapshot = os.urandom(20) if eventful else last_snapshot
    yield status_queue.put(
        TaskEvent(
            task=task,
            status=attr.evolve(
                status, status=end_status, date=env.time, snapshot=new_snapshot
            ),
            eventful=eventful,
        )
    )

    env.report.record_visit(run_time, eventful, end_status)
