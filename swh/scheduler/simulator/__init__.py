# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timedelta, timezone
import logging
from typing import Dict, Generator, Optional

from simpy import Event

from swh.scheduler import get_scheduler
from swh.scheduler.model import ListedOrigin

from . import origin_scheduler, task_scheduler
from .common import Environment, Queue, SimulationReport, Task
from .origins import load_task_process

logger = logging.getLogger(__name__)


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
    scheduler: str,
    policy: Optional[str],
    workers_per_type: Dict[str, int],
    task_queue_capacity: int,
):
    # We expect PGHOST, PGPORT, ... set externally
    task_queues = {
        visit_type: Queue(env, capacity=task_queue_capacity)
        for visit_type in workers_per_type
    }
    status_queue = Queue(env)

    if scheduler == "origin_scheduler":
        if policy is None:
            raise ValueError("origin_scheduler needs a scheduling policy")
        env.process(origin_scheduler.scheduler_runner_process(env, task_queues, policy))
        env.process(
            origin_scheduler.scheduler_journal_client_process(env, status_queue)
        )
    elif scheduler == "task_scheduler":
        if policy is not None:
            raise ValueError("task_scheduler doesn't support a scheduling policy")
        env.process(task_scheduler.scheduler_runner_process(env, task_queues))
        env.process(task_scheduler.scheduler_listener_process(env, status_queue))
    else:
        raise ValueError(f"Unknown scheduler: {scheduler}")

    for visit_type, num_workers in workers_per_type.items():
        task_queue = task_queues[visit_type]
        for i in range(num_workers):
            worker_name = f"worker-{visit_type}-{i}"
            env.process(worker_process(env, worker_name, task_queue, status_queue))


def fill_test_data():
    scheduler = get_scheduler(cls="local", db="")
    stored_lister = scheduler.get_or_create_lister(name="example")
    origins = [
        ListedOrigin(
            lister_id=stored_lister.id,
            url=f"https://example.com/{i:04d}.git",
            visit_type="git",
            last_update=datetime(2020, 6, 15, 16, 0, 0, i, tzinfo=timezone.utc),
        )
        for i in range(100000)
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


def run(scheduler: str, policy: Optional[str], runtime: Optional[int]):
    NUM_WORKERS = 48
    start_time = datetime.now(tz=timezone.utc)
    env = Environment(start_time=start_time)
    env.scheduler = get_scheduler(cls="local", db="")
    env.report = SimulationReport()
    setup(
        env,
        scheduler=scheduler,
        policy=policy,
        workers_per_type={"git": NUM_WORKERS},
        task_queue_capacity=10000,
    )
    try:
        env.run(until=runtime)
    except KeyboardInterrupt:
        pass
    finally:
        end_time = env.time
        print("total time:", end_time - start_time)
        print(env.report.format())
