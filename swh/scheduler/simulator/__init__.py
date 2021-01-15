# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timedelta, timezone
import hashlib
import logging
import os
import textwrap
from typing import Any, Dict, Generator, Iterator, List, Optional, Tuple

import attr
import plotille
from simpy import Environment as _Environment
from simpy import Event, Store

from swh.model.model import OriginVisitStatus
from swh.scheduler import get_scheduler
from swh.scheduler.interface import SchedulerInterface
from swh.scheduler.journal_client import process_journal_objects
from swh.scheduler.model import ListedOrigin, OriginVisitStats

logger = logging.getLogger(__name__)


@attr.s
class SimulationReport:
    DURATION_THRESHOLD = 3600
    """Max duration for histograms"""

    total_visits = attr.ib(type=int, default=0)
    """Total count of finished visits"""

    visit_runtimes = attr.ib(type=Dict[Tuple[str, bool], List[float]], factory=dict)
    """Collect the visit runtimes for each (status, eventful) tuple"""

    def record_visit(self, duration: float, eventful: bool, status: str) -> None:
        self.total_visits += 1
        self.visit_runtimes.setdefault((status, eventful), []).append(duration)

    @property
    def useless_visits(self):
        """Number of uneventful, full visits"""
        return len(self.visit_runtimes.get(("full", False), []))

    def runtime_histogram(self, status: str, eventful: bool) -> str:
        runtimes = self.visit_runtimes.get((status, eventful), [])
        return plotille.hist(
            [runtime for runtime in runtimes if runtime <= self.DURATION_THRESHOLD]
        )

    def format(self):
        full_visits = self.visit_runtimes.get(("full", True), [])
        histogram = self.runtime_histogram("full", True)
        long_tasks = sum(runtime > self.DURATION_THRESHOLD for runtime in full_visits)

        return (
            textwrap.dedent(
                f"""\
                Total visits: {self.total_visits}
                Useless visits: {self.useless_visits}
                Eventful visits: {len(full_visits)}
                Very long running tasks: {long_tasks}
                Visit time histogram for eventful visits:
                """
            )
            + histogram
        )


class Queue(Store):
    """Model a queue of objects to be passed between processes."""

    def __len__(self):
        return len(self.items or [])

    def slots_remaining(self):
        return self.capacity - len(self)


class Environment(_Environment):
    report: SimulationReport
    scheduler: SchedulerInterface

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
    MIN_RUN_TIME = 0.5
    """Minimal run time for a visit (retrieved from production data)"""

    MAX_RUN_TIME = 7200
    """Max run time for a visit"""

    PER_COMMIT_RUN_TIME = 0.1
    """Run time per commit"""

    def __init__(self, type: str, origin: str):
        self.type = type
        self.origin = origin

    def seconds_between_commits(self):
        n_bytes = 2
        num_buckets = 2 ** (8 * n_bytes)
        bucket = int.from_bytes(
            hashlib.md5(self.origin.encode()).digest()[0:n_bytes], "little"
        )
        # minimum: 1 second (bucket == 0)
        # max: 10 years (bucket == num_buckets - 1)
        ten_y = 10 * 365 * 24 * 3600

        return ten_y ** (bucket / num_buckets)
        # return 1 + (ten_y - 1) * (bucket / (num_buckets - 1))

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
    (run_time, eventful, end_status) = origin_model.load_task_characteristics(
        env, stats
    )
    logger.debug("%s task %s origin=%s: Start", env.time, visit_type, origin)
    yield status_queue.put(status)
    yield env.timeout(run_time)
    logger.debug("%s task %s origin=%s: End", env.time, visit_type, origin)

    new_snapshot = os.urandom(20) if eventful else last_snapshot
    yield status_queue.put(
        attr.evolve(status, status=end_status, date=env.time, snapshot=new_snapshot)
    )

    env.report.record_visit(run_time, eventful, end_status)


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
    env.process(scheduler_journal_client_process(env, status_queue))

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
            for i in range(100000)
        ]
    )


def run(runtime: Optional[int]):
    logging.basicConfig(level=logging.INFO)
    NUM_WORKERS = 48
    start_time = datetime.now(tz=timezone.utc)
    env = Environment(start_time=start_time)
    env.scheduler = get_scheduler(cls="local", db="")
    env.report = SimulationReport()
    setup(
        env,
        workers_per_type={"git": NUM_WORKERS},
        task_queue_capacity=10000,
        policy="oldest_scheduled_first",
    )
    try:
        env.run(until=runtime)
    except KeyboardInterrupt:
        pass
    finally:
        end_time = env.time
        print("total time:", end_time - start_time)
        print(env.report.format())
