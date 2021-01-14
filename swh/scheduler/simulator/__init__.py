# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timedelta, timezone
import logging
import os
from typing import Dict, Generator, Iterator, Tuple

import attr
from simpy import Environment as _Environment
from simpy import Event, Store

from swh.model.model import OriginVisitStatus
from swh.scheduler import get_scheduler
from swh.scheduler.model import ListedOrigin

logger = logging.getLogger(__name__)


class Queue(Store):
    """Model a queue of objects to be passed between processes."""

    def __len__(self):
        return len(self.items or [])

    def slots_remaining(self):
        return self.capacity - len(self)


class Environment(_Environment):
    def __init__(self, start_time: datetime):
        if start_time.tzinfo is None:
            raise ValueError("start_time must have timezone information")
        self.start_time = start_time
        super().__init__()

    @property
    def time(self):
        """Get the current simulated wall clock time"""
        return self.start_time + timedelta(seconds=self.now)


class OriginModel:
    def __init__(self, type: str, origin: str):
        self.type = type
        self.origin = origin

    def load_task_characteristics(self) -> Tuple[float, bool, str]:
        """Returns the (run_time, eventfulness, end_status) of the next
        origin visit."""
        return (101.0, True, "full")


def worker_process(
    env: Environment, name: str, task_queue: Queue, status_queue: Queue
) -> Generator[Event, Tuple[str, str], None]:
    """A worker which consumes tasks from the input task_queue. Tasks
    themselves send OriginVisitStatus objects to the status_queue."""
    logger.debug("%s worker %s: Start", env.time, name)
    while True:
        visit_type, origin = yield task_queue.get()
        logger.debug(
            "%s worker %s: Run task %s origin=%s", env.time, name, visit_type, origin
        )
        yield env.process(
            load_task_process(env, visit_type, origin, status_queue=status_queue)
        )


def load_task_process(
    env: Environment, visit_type: str, origin: str, status_queue: Queue
) -> Iterator[Event]:
    """A loading task. This pushes OriginVisitStatus objects to the
    status_queue to simulate the visible outcomes of the task.

    Uses the `load_task_duration` function to determine its run time.
    """
    # This is cheating; actual tasks access the state from the storage, not the
    # scheduler
    stats = env.scheduler.origin_visit_stats_get(origin, visit_type)
    last_snapshot = stats.last_snapshot if stats else None

    status = OriginVisitStatus(
        origin=origin,
        visit=42,
        type=visit_type,
        status="created",
        date=env.time,
        snapshot=None,
    )
    origin_model = OriginModel(visit_type, origin)
    (run_time, eventful, end_status) = origin_model.load_task_characteristics()
    logger.debug("%s task %s origin=%s: Start", env.time, visit_type, origin)
    yield status_queue.put(status)
    yield env.timeout(run_time)
    logger.debug("%s task %s origin=%s: End", env.time, visit_type, origin)

    new_snapshot = os.urandom(20) if eventful else last_snapshot
    yield status_queue.put(
        attr.evolve(status, status=end_status, date=env.time, snapshot=new_snapshot)
    )


def scheduler_runner_process(
    env: Environment, task_queues: Dict[str, Queue], policy: str,
) -> Iterator[Event]:
    """Scheduler runner. Grabs next visits from the database according to the
    scheduling policy, and fills the task_queues accordingly."""

    while True:
        for visit_type, queue in task_queues.items():
            logger.debug("%s runner: processing %s", env.time, visit_type)
            min_batch_size = max(queue.capacity // 10, 1)
            remaining = queue.slots_remaining()
            if remaining < min_batch_size:
                logger.debug(
                    "%s runner: not enough slots in %s: %s",
                    env.time,
                    visit_type,
                    remaining,
                )
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


def setup(
    env: Environment,
    workers_per_type: Dict[str, int],
    task_queue_capacity: int,
    policy: str,
):
    # We expect PGHOST, PGPORT, ... set externally
    task_queues = {
        visit_type: Queue(env, capacity=task_queue_capacity)
        for visit_type in workers_per_type
    }
    status_queue = Queue(env)

    env.process(scheduler_runner_process(env, task_queues, policy))

    for visit_type, num_workers in workers_per_type.items():
        task_queue = task_queues[visit_type]
        for i in range(num_workers):
            worker_name = f"worker-{visit_type}-{i}"
            env.process(worker_process(env, worker_name, task_queue, status_queue))


def fill_test_data():
    scheduler = get_scheduler(cls="local", db="")
    stored_lister = scheduler.get_or_create_lister(name="example")
    scheduler.record_listed_origins(
        [
            ListedOrigin(
                lister_id=stored_lister.id,
                url=f"https://example.com/{i:04d}.git",
                visit_type="git",
                last_update=datetime(2020, 6, 15, 16, 0, 0, i, tzinfo=timezone.utc),
            )
            for i in range(1000)
        ]
    )


def run():
    logging.basicConfig(level=logging.DEBUG)
    NUM_WORKERS = 2
    env = Environment(start_time=datetime.now(tz=timezone.utc))
    env.scheduler = get_scheduler(cls="local", db="")
    setup(
        env,
        workers_per_type={"git": NUM_WORKERS},
        task_queue_capacity=100,
        policy="oldest_scheduled_first",
    )
    env.run(until=10000)
