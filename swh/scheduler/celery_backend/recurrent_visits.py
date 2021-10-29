# Copyright (C) 2021 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""This schedules the recurrent visits, for listed origins, in Celery.

For "oneshot" (save code now, lister) tasks, check the
:mod:`swh.scheduler.celery_backend.runner` and
:mod:`swh.scheduler.celery_backend.pika_listener` modules.

"""

from __future__ import annotations

from itertools import chain
import logging
from queue import Empty, Queue
import random
from threading import Thread
import time
from typing import TYPE_CHECKING, Any, Dict, List, Tuple

from kombu.utils.uuid import uuid

from swh.scheduler.celery_backend.config import get_available_slots

if TYPE_CHECKING:
    from ..interface import SchedulerInterface
    from ..model import ListedOrigin

logger = logging.getLogger(__name__)


_VCS_POLICY_WEIGHTS: Dict[str, float] = {
    "already_visited_order_by_lag": 49,
    "never_visited_oldest_update_first": 49,
    "origins_without_last_update": 2,
}

POLICY_WEIGHTS: Dict[str, Dict[str, float]] = {
    "default": {
        "already_visited_order_by_lag": 50,
        "never_visited_oldest_update_first": 50,
    },
    "git": _VCS_POLICY_WEIGHTS,
    "hg": _VCS_POLICY_WEIGHTS,
    "svn": _VCS_POLICY_WEIGHTS,
    "cvs": _VCS_POLICY_WEIGHTS,
    "bzr": _VCS_POLICY_WEIGHTS,
}
"""Scheduling policies to use to retrieve visits for the given visit types, with their
relative weights"""

MIN_SLOTS_RATIO = 0.05
"""Quantity of slots that need to be available (with respect to max_queue_length) for
:py:func:`~swh.scheduler.interface.SchedulerInterface.grab_next_visits` to trigger"""

QUEUE_FULL_BACKOFF = 60
"""Backoff time (in seconds) if there's fewer than :py:data:`MIN_SLOTS_RATIO` slots
available in the queue."""

NO_ORIGINS_SCHEDULED_BACKOFF = 20 * 60
"""Backoff time (in seconds) if no origins have been scheduled in the current
iteration"""

BACKOFF_SPLAY = 5.0
"""Amplitude of the fuzziness between backoffs"""

TERMINATE = object()
"""Termination request received from command queue (singleton used for identity
comparison)"""


def grab_next_visits_policy_weights(
    scheduler: SchedulerInterface, visit_type: str, num_visits: int
) -> List[ListedOrigin]:
    """Get the next ``num_visits`` for the given ``visit_type`` using the corresponding
    set of scheduling policies.

    The :py:data:`POLICY_WEIGHTS` dict sets, for each visit type, the scheduling
    policies used to pull the next tasks, and what proportion of the available
    num_visits they take.

    This function emits a warning if the ratio of retrieved origins is off of
    the requested ratio by more than 5%.

    Returns:
       at most ``num_visits`` :py:class:`~swh.scheduler.model.ListedOrigin` objects
    """
    policy_weights = POLICY_WEIGHTS.get(visit_type, POLICY_WEIGHTS["default"])
    total_weight = sum(policy_weights.values())

    if not total_weight:
        raise ValueError(f"No policy weights set for visit type {visit_type}")

    policy_ratio = {
        policy: weight / total_weight for policy, weight in policy_weights.items()
    }

    fetched_origins: Dict[str, List[ListedOrigin]] = {}

    for policy, ratio in policy_ratio.items():
        num_tasks_to_send = int(num_visits * ratio)
        fetched_origins[policy] = scheduler.grab_next_visits(
            visit_type, num_tasks_to_send, policy=policy
        )

    all_origins: List[ListedOrigin] = list(
        chain.from_iterable(fetched_origins.values())
    )
    if not all_origins:
        return []

    # Check whether the ratios of origins fetched are skewed with respect to the
    # ones we requested
    fetched_origin_ratios = {
        policy: len(origins) / len(all_origins)
        for policy, origins in fetched_origins.items()
    }

    for policy, expected_ratio in policy_ratio.items():
        # 5% of skew with respect to request
        if abs(fetched_origin_ratios[policy] - expected_ratio) / expected_ratio > 0.05:
            logger.info(
                "Skewed fetch for visit type %s with policy %s: fetched %s, "
                "requested %s",
                visit_type,
                policy,
                fetched_origin_ratios[policy],
                expected_ratio,
            )

    return all_origins


def splay():
    """Return a random short interval by which to vary the backoffs for the visit
    scheduling threads"""
    return random.uniform(0, BACKOFF_SPLAY)


def send_visits_for_visit_type(
    scheduler: SchedulerInterface, app, visit_type: str, task_type: Dict,
) -> float:
    """Schedule the next batch of visits for the given ``visit_type``.

    First, we determine the number of available slots by introspecting the RabbitMQ
    queue.

    If there's fewer than :py:data:`MIN_SLOTS_RATIO` slots available in the queue, we
    wait for :py:data:`QUEUE_FULL_BACKOFF` seconds. This avoids running the expensive
    :py:func:`~swh.scheduler.interface.SchedulerInterface.grab_next_visits` queries when
    there's not many jobs to queue.

    Once there's more than :py:data:`MIN_SLOTS_RATIO` slots available, we run
    :py:func:`grab_next_visits_policy_weights` to retrieve the next set of origin visits
    to schedule, and we send them to celery.

    If the last scheduling attempt didn't return any origins, we sleep for
    :py:data:`NO_ORIGINS_SCHEDULED_BACKOFF` seconds. This avoids running the expensive
    :py:func:`~swh.scheduler.interface.SchedulerInterface.grab_next_visits` queries too
    often if there's nothing left to schedule.

    Returns:
       the earliest :py:func:`time.monotonic` value at which to run the next iteration
       of the loop.

    """
    queue_name = task_type["backend_name"]
    max_queue_length = task_type.get("max_queue_length") or 0
    min_available_slots = max_queue_length * MIN_SLOTS_RATIO

    current_iteration_start = time.monotonic()

    # Check queue level
    available_slots = get_available_slots(app, queue_name, max_queue_length)
    logger.debug(
        "%s available slots for visit type %s in queue %s",
        available_slots,
        visit_type,
        queue_name,
    )
    if available_slots < min_available_slots:
        return current_iteration_start + QUEUE_FULL_BACKOFF

    origins = grab_next_visits_policy_weights(scheduler, visit_type, available_slots)

    if not origins:
        logger.debug("No origins to visit for type %s", visit_type)
        return current_iteration_start + NO_ORIGINS_SCHEDULED_BACKOFF

    # Try to smooth the ingestion load, origins pulled by different
    # scheduling policies have different resource usage patterns
    random.shuffle(origins)

    for origin in origins:
        task_dict = origin.as_task_dict()
        app.send_task(
            queue_name,
            task_id=uuid(),
            args=task_dict["arguments"]["args"],
            kwargs=task_dict["arguments"]["kwargs"],
            queue=queue_name,
        )

    logger.info(
        "%s: %s visits scheduled in queue %s", visit_type, len(origins), queue_name,
    )

    # When everything worked, we can try to schedule origins again ASAP.
    return time.monotonic()


def visit_scheduler_thread(
    config: Dict,
    visit_type: str,
    command_queue: Queue[object],
    exc_queue: Queue[Tuple[str, BaseException]],
):
    """Target function for the visit sending thread, which initializes local connections
    and handles exceptions by sending them back to the main thread."""

    from swh.scheduler import get_scheduler
    from swh.scheduler.celery_backend.config import build_app

    try:
        # We need to reinitialize these connections because they're not generally
        # thread-safe
        app = build_app(config.get("celery"))
        scheduler = get_scheduler(**config["scheduler"])
        task_type = scheduler.get_task_type(f"load-{visit_type}")

        if task_type is None:
            raise ValueError(f"Unknown task type: load-{visit_type}")

        next_iteration = time.monotonic()

        while True:
            # vary the next iteration time a little bit
            next_iteration = next_iteration + splay()
            while time.monotonic() < next_iteration:
                # Wait for next iteration to start. Listen for termination message.
                try:
                    msg = command_queue.get(block=True, timeout=1)
                except Empty:
                    continue

                if msg is TERMINATE:
                    return
                else:
                    logger.warn("Received unexpected message %s in command queue", msg)

            next_iteration = send_visits_for_visit_type(
                scheduler, app, visit_type, task_type
            )

    except BaseException as e:
        exc_queue.put((visit_type, e))


VisitSchedulerThreads = Dict[str, Tuple[Thread, Queue]]
"""Dict storing the visit scheduler threads and their command queues"""


def spawn_visit_scheduler_thread(
    threads: VisitSchedulerThreads,
    exc_queue: Queue[Tuple[str, BaseException]],
    config: Dict[str, Any],
    visit_type: str,
):
    """Spawn a new thread to schedule the visits of type ``visit_type``."""
    command_queue: Queue[object] = Queue()
    thread = Thread(
        target=visit_scheduler_thread,
        kwargs={
            "config": config,
            "visit_type": visit_type,
            "command_queue": command_queue,
            "exc_queue": exc_queue,
        },
    )
    threads[visit_type] = (thread, command_queue)
    thread.start()


def terminate_visit_scheduler_threads(threads: VisitSchedulerThreads) -> List[str]:
    """Terminate all visit scheduler threads"""
    logger.info("Termination requested...")
    for _, command_queue in threads.values():
        command_queue.put(TERMINATE)

    loops = 0
    while threads and loops < 10:
        logger.info(
            "Terminating visit scheduling threads: %s", ", ".join(sorted(threads))
        )
        loops += 1
        for visit_type, (thread, _) in list(threads.items()):
            thread.join(timeout=1)
            if not thread.is_alive():
                logger.debug("Thread %s terminated", visit_type)
                del threads[visit_type]

    if threads:
        logger.warn(
            "Could not reap the following threads after 10 attempts: %s",
            ", ".join(sorted(threads)),
        )

    return list(sorted(threads))
