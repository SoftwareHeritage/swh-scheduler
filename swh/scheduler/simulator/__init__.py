# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""This package runs the scheduler in a simulated environment, to evaluate
various metrics. See :ref:`swh-scheduler-simulator`.

This module orchestrates of the simulator by initializing processes and connecting
them together; these processes are defined in modules in the package and
simulate/call specific components."""

from datetime import datetime, timedelta, timezone
import logging
from typing import Dict, Generator, Optional

from simpy import Event

from swh.scheduler.interface import SchedulerInterface
from swh.scheduler.model import ListedOrigin

from . import origin_scheduler, task_scheduler
from .common import Environment, Queue, SimulationReport, Task
from .origins import load_task_process

logger = logging.getLogger(__name__)


def update_metrics_process(
    env: Environment, update_interval: int
) -> Generator[Event, None, None]:
    """Update the scheduler metrics every `update_interval` (simulated) seconds,
    and add them to the SimulationReport
    """
    t0 = env.time
    while True:
        metrics = env.scheduler.update_metrics(timestamp=env.time)
        env.report.record_metrics(env.time, metrics)
        dt = env.time - t0
        logger.info("time:%s visits:%s", dt, env.report.total_visits)
        yield env.timeout(update_interval)


def worker_process(
    env: Environment, name: str, task_queue: Queue, status_queue: Queue
) -> Generator[Event, Task, None]:
    """A worker which consumes tasks from the input task_queue. Tasks
    themselves send OriginVisitStatus objects to the status_queue."""
    logger.debug("%s worker %s: Start", env.time, name)
    while True:
        task = yield task_queue.get()
        logger.debug(
            "%s worker %s: Run task %s origin=%s",
            env.time,
            name,
            task.visit_type,
            task.origin,
        )
        yield env.process(load_task_process(env, task, status_queue=status_queue))


def setup(
    env: Environment,
    scheduler_type: str,
    policy: Optional[str],
    workers_per_type: Dict[str, int],
    task_queue_capacity: int,
    min_batch_size: int,
    metrics_update_interval: int,
):
    task_queues = {
        visit_type: Queue(env, capacity=task_queue_capacity)
        for visit_type in workers_per_type
    }
    status_queue = Queue(env)

    if scheduler_type == "origin_scheduler":
        if policy is None:
            raise ValueError("origin_scheduler needs a scheduling policy")
        env.process(
            origin_scheduler.scheduler_runner_process(
                env, task_queues, policy, min_batch_size=min_batch_size
            )
        )
        env.process(
            origin_scheduler.scheduler_journal_client_process(env, status_queue)
        )
    elif scheduler_type == "task_scheduler":
        if policy is not None:
            raise ValueError("task_scheduler doesn't support a scheduling policy")
        env.process(
            task_scheduler.scheduler_runner_process(
                env, task_queues, min_batch_size=min_batch_size
            )
        )
        env.process(task_scheduler.scheduler_listener_process(env, status_queue))
    else:
        raise ValueError(f"Unknown scheduler type to simulate: {scheduler_type}")

    env.process(update_metrics_process(env, metrics_update_interval))

    for visit_type, num_workers in workers_per_type.items():
        task_queue = task_queues[visit_type]
        for i in range(num_workers):
            worker_name = f"worker-{visit_type}-{i}"
            env.process(worker_process(env, worker_name, task_queue, status_queue))


def fill_test_data(scheduler: SchedulerInterface, num_origins: int = 100000):
    """Fills the database with mock data to test the simulator."""
    stored_lister = scheduler.get_or_create_lister(name="example")
    assert stored_lister.id is not None

    origins = [
        ListedOrigin(
            lister_id=stored_lister.id,
            url=f"https://example.com/{i:04d}.git",
            visit_type="git",
            last_update=datetime(2020, 6, 15, 16, 0, 0, i, tzinfo=timezone.utc),
        )
        for i in range(num_origins)
    ]
    scheduler.record_listed_origins(origins)

    scheduler.create_tasks(
        [
            {
                **origin.as_task_dict(),
                "policy": "recurring",
                "next_run": origin.last_update,
                "interval": timedelta(days=64),
            }
            for origin in origins
        ]
    )


def run(
    scheduler: SchedulerInterface,
    scheduler_type: str,
    policy: Optional[str],
    runtime: Optional[int],
):
    NUM_WORKERS = 48
    start_time = datetime.now(tz=timezone.utc)
    env = Environment(start_time=start_time)
    env.scheduler = scheduler
    env.report = SimulationReport()
    setup(
        env,
        scheduler_type=scheduler_type,
        policy=policy,
        workers_per_type={"git": NUM_WORKERS},
        task_queue_capacity=10000,
        min_batch_size=1000,
        metrics_update_interval=3600,
    )
    try:
        env.run(until=runtime)
    except KeyboardInterrupt:
        pass
    finally:
        end_time = env.time
        print("total simulated time:", end_time - start_time)
        metrics = env.scheduler.update_metrics(timestamp=end_time)
        env.report.record_metrics(end_time, metrics)
        print(env.report.format())
