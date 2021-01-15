# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Agents using the new origin-aware scheduler."""

import logging
from typing import Any, Dict, Generator, Iterator, List

from simpy import Event

from swh.model.model import OriginVisitStatus
from swh.scheduler.journal_client import process_journal_objects

from . import Environment
from .common import Queue

logger = logging.getLogger(__name__)


def scheduler_runner_process(
    env: Environment, task_queues: Dict[str, Queue], policy: str,
) -> Iterator[Event]:
    """Scheduler runner. Grabs next visits from the database according to the
    scheduling policy, and fills the task_queues accordingly."""

    while True:
        for visit_type, queue in task_queues.items():
            min_batch_size = max(queue.capacity // 10, 1)
            remaining = queue.slots_remaining()
            if remaining < min_batch_size:
                continue
            next_origins = env.scheduler.grab_next_visits(
                visit_type, remaining, policy=policy
            )
            logger.debug(
                "%s runner: running %s %s tasks",
                env.time,
                visit_type,
                len(next_origins),
            )
            for origin in next_origins:
                yield queue.put((origin.visit_type, origin.url))

        yield env.timeout(10.0)


def scheduler_journal_client_process(
    env: Environment, status_queue: Queue
) -> Generator[Event, OriginVisitStatus, None]:
    """Scheduler journal client. Every once in a while, pulls
    `OriginVisitStatus`es from the status_queue to update the scheduler
    origin_visit_stats table."""
    BATCH_SIZE = 100

    statuses: List[Dict[str, Any]] = []
    while True:
        statuses.append((yield status_queue.get()).to_dict())
        if len(statuses) < BATCH_SIZE:
            continue

        logger.debug(
            "%s journal client: processing %s statuses", env.time, len(statuses)
        )
        process_journal_objects(
            {"origin_visit_status": statuses}, scheduler=env.scheduler
        )

        statuses = []
