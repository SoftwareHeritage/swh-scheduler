# Copyright (C) 2016-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import logging
import time
from typing import TYPE_CHECKING, List, Tuple

import click
import sentry_sdk

from . import cli

if TYPE_CHECKING:
    from ..interface import SchedulerInterface


@cli.command("start-runner")
@click.option(
    "--period",
    "-p",
    default=0,
    help=(
        "Period (in s) at witch pending tasks are checked and "
        "executed. Set to 0 (default) for a one shot."
    ),
)
@click.option(
    "--task-type",
    "task_type_names",
    multiple=True,
    default=[],
    help=(
        "Task types to schedule. If not provided, this iterates over every "
        "task types referenced in the scheduler backend."
    ),
)
@click.option(
    "--task-type-pattern",
    "task_type_patterns",
    multiple=True,
    default=[],
    help=(
        "Task type patterns allowed to be scheduled. Empty by default so no filtering."
    ),
)
@click.option(
    "--with-priority/--without-priority",
    is_flag=True,
    default=False,
    help=(
        "Determine if those tasks should be the ones with priority or not."
        "By default, this deals with tasks without any priority."
    ),
)
@click.pass_context
def runner(ctx, period, task_type_names, task_type_patterns, with_priority):
    """Starts a swh-scheduler runner instance.

    This process is responsible for scheduling ready-to-run tasks.

    It's possible to provide the task types allowed to be scheduled. It's either by
    specifying their names or providing task type patterns or both. In which case, only
    the allowed task types will be scheduled. If no task type names or patterns are
    specified, all registered scheduler task types will be scheduled (if any tasks
    matching the task types is ready).

    Expected configuration:

    \b
    * :ref:`cli-config-celery`
    * :ref:`cli-config-scheduler`

    """
    from swh.scheduler.celery_backend.config import build_app
    from swh.scheduler.celery_backend.runner import run_ready_tasks

    config = ctx.obj["config"]
    app = build_app(config.get("celery"))
    app.set_current()

    logger = logging.getLogger(__name__ + ".runner")
    scheduler = ctx.obj["scheduler"]
    logger.debug("Scheduler %s", scheduler)
    task_types = []
    for task_type_name in task_type_names:
        task_type = scheduler.get_task_type(task_type_name)
        if not task_type:
            ctx.fail(f"Unknown {task_type_name}")
        task_types.append(task_type)

    try:
        while True:
            logger.debug("Run ready tasks")
            try:
                ntasks = len(
                    run_ready_tasks(
                        scheduler, app, task_types, task_type_patterns, with_priority
                    )
                )
                if ntasks:
                    logger.info("Scheduled %s tasks", ntasks)
            except Exception:
                logger.exception("Unexpected error in run_ready_tasks()")
                sentry_sdk.capture_exception()
            if not period:
                break
            time.sleep(period)
    except KeyboardInterrupt:
        ctx.exit(0)


@cli.command("start-listener")
@click.pass_context
def listener(ctx):
    """Starts a swh-scheduler listener service.

    This service is responsible for listening at task lifecycle events and
    handle their workflow status in the database.

    Expected configuration:

    \b
    * :ref:`cli-config-celery`
    * :ref:`cli-config-scheduler`
    """
    scheduler_backend = ctx.obj["scheduler"]
    if not scheduler_backend:
        ctx.fail("Scheduler class must be instantiated")

    broker = (
        ctx.obj["config"]
        .get("celery", {})
        .get("task_broker", "amqp://guest@localhost/%2f")
    )

    from swh.scheduler.celery_backend.pika_listener import get_listener

    listener = get_listener(broker, "celeryev.listener", scheduler_backend)
    try:
        listener.start_consuming()
    finally:
        listener.stop_consuming()


@cli.command("schedule-recurrent")
@click.option(
    "--visit-type",
    "visit_types",
    multiple=True,
    default=[],
    help=(
        "Visit types to schedule. If not provided, this iterates over every "
        "corresponding load task types referenced in the scheduler backend."
    ),
)
@click.pass_context
def schedule_recurrent(ctx, visit_types: List[str]):
    """Starts the scheduler for recurrent visits.

    This runs one thread for each visit type, which regularly sends new visits
    to celery.

    Expected configuration:

    \b
    * :ref:`cli-config-celery`
    * :ref:`cli-config-scheduler`
    """
    from queue import Queue

    from swh.scheduler.celery_backend.recurrent_visits import (
        VisitSchedulerThreads,
        logger,
        spawn_visit_scheduler_thread,
        terminate_visit_scheduler_threads,
    )

    config = ctx.obj["config"]
    scheduler = ctx.obj["scheduler"]

    if not visit_types:
        visit_types = []
        # Figure out which visit types exist in the scheduler
        all_task_types = scheduler.get_task_types()
        for task_type in all_task_types:
            if not task_type.type.startswith("load-"):
                # only consider loading tasks as recurring ones, the rest is dismissed
                continue
            # get visit type name from task type
            visit_types.append(task_type.type[5:])
    else:
        # Check that the passed visit types exist in the scheduler
        for visit_type in visit_types:
            task_type_name = f"load-{visit_type}"
            task_type = scheduler.get_task_type(task_type_name)
            if not task_type:
                ctx.fail(f"Unknown task type: {task_type_name}")

    exc_queue: Queue[Tuple[str, BaseException]] = Queue()
    threads: VisitSchedulerThreads = {}

    try:
        # Spawn initial threads
        for visit_type in visit_types:
            spawn_visit_scheduler_thread(threads, exc_queue, config, visit_type)

        # Handle exceptions from child threads
        while True:
            visit_type, exc_info = exc_queue.get(block=True)

            logger.exception(
                "Thread %s died with exception; respawning",
                visit_type,
                exc_info=exc_info,
            )
            sentry_sdk.capture_exception(exc_info)

            dead_thread = threads[visit_type][0]
            dead_thread.join(timeout=1)

            if dead_thread.is_alive():
                logger.warning(
                    "The thread for %s is still alive after sending an exception?! "
                    "Respawning anyway.",
                    visit_type,
                )

            spawn_visit_scheduler_thread(threads, exc_queue, config, visit_type)

    except SystemExit:
        remaining_threads = terminate_visit_scheduler_threads(threads)
        if remaining_threads:
            ctx.exit(1)
        ctx.exit(0)


@cli.command("start-runner-first-visits")
@click.option(
    "--period",
    "-p",
    default=0,
    help=(
        "Period (in s) at witch pending tasks are checked and "
        "executed. Set to 0 (default) for a one shot."
    ),
)
@click.pass_context
def runner_first_visits(ctx, period):
    """Starts a swh-scheduler runner service for first visits.

    This process is responsible for scheduling the first visits for origins registered
    by listers having their first_visits_priority_queue attribute set. It's looping
    forever with a given period.

    """

    from swh.scheduler.celery_backend.first_visits import schedule_first_visits

    logger = logging.getLogger(__name__ + ".runner_first_visits")
    scheduler: SchedulerInterface = ctx.obj["scheduler"]

    try:
        while True:
            logger.debug("Schedule first visits...")
            start = time.monotonic()
            try:
                nb_first_visits = schedule_first_visits(scheduler)
                logger.info(
                    "Scheduled %s first visits in %s seconds",
                    nb_first_visits,
                    time.monotonic() - start,
                )
            except Exception:
                logger.exception("Unexpected error in run_high_priority_first_visits()")
                sentry_sdk.capture_exception()
            if not period:
                break
            time.sleep(period)
    except KeyboardInterrupt:
        ctx.exit(0)


@cli.command("rpc-serve")
@click.option("--host", default="0.0.0.0", help="Host to run the scheduler server api")
@click.option("--port", default=5008, type=click.INT, help="Binding port of the server")
@click.option(
    "--debug/--nodebug",
    default=None,
    help=(
        "Indicates if the server should run in debug mode. "
        "Defaults to True if log-level is DEBUG, False otherwise."
    ),
)
@click.pass_context
def rpc_server(ctx, host, port, debug):
    """Starts a swh-scheduler API HTTP server.

    Expected configuration:

    \b
    * :ref:`cli-config-celery`
    * :ref:`cli-config-scheduler`
    """
    if ctx.obj["config"]["scheduler"]["cls"] == "remote":
        click.echo(
            "The API server cannot be started with a 'remote' "
            "backend cls configuration",
            err=True,
        )
        ctx.exit(1)

    from swh.scheduler.api import server

    server.app.config.update(ctx.obj["config"])
    if debug is None:
        debug = ctx.obj["log_level"] <= logging.DEBUG
    server.app.run(host, port=port, debug=bool(debug))
