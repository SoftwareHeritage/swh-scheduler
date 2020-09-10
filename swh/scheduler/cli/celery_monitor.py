# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import logging
import sys
import time
from typing import Any, Dict, Optional

import click

from . import cli

logger = logging.getLogger(__name__)


def destination_from_pattern(ctx: click.Context, pattern: Optional[str]):
    """Get the celery destination pattern from host and type values"""
    if pattern is None:
        logger.debug("Matching all workers")
    elif "*" in pattern:
        ctx.obj["inspect"].pattern = pattern
        ctx.obj["inspect"].matcher = "glob"
        logger.debug("Using glob pattern %s", pattern)
    else:
        destination = pattern.split(",")
        ctx.obj["inspect"].destination = destination
        logger.debug("Using destinations %s", ", ".join(destination))


@cli.group("celery-monitor")
@click.option(
    "--timeout", type=float, default=3.0, help="Timeout for celery remote control"
)
@click.option("--pattern", help="Celery destination pattern", default=None)
@click.pass_context
def celery_monitor(ctx: click.Context, timeout: float, pattern: Optional[str]) -> None:
    """Monitoring of Celery"""
    from swh.scheduler.celery_backend.config import app

    ctx.obj["timeout"] = timeout
    ctx.obj["inspect"] = app.control.inspect(timeout=timeout)

    destination_from_pattern(ctx, pattern)


@celery_monitor.command("ping-workers")
@click.pass_context
def ping_workers(ctx: click.Context) -> None:
    """Check which workers respond to the celery remote control"""

    response_times = {}

    def ping_callback(response):
        rtt = time.monotonic() - ping_time
        for destination in response:
            logger.debug("Got ping response from %s: %r", destination, response)
            response_times[destination] = rtt

    ctx.obj["inspect"].callback = ping_callback

    ping_time = time.monotonic()
    ret = ctx.obj["inspect"].ping()

    if not ret:
        logger.info("No response in %f seconds", time.monotonic() - ping_time)
        ctx.exit(1)

    for destination in ret:
        logger.info(
            "Got response from %s in %f seconds",
            destination,
            response_times[destination],
        )

    ctx.exit(0)


@celery_monitor.command("list-running")
@click.option(
    "--format",
    help="Output format",
    default="pretty",
    type=click.Choice(["pretty", "csv"]),
)
@click.pass_context
def list_running(ctx: click.Context, format: str):
    """List running tasks on the lister workers"""
    from ast import literal_eval
    import csv

    response_times = {}

    def active_callback(response):
        rtt = time.monotonic() - active_time
        for destination in response:
            response_times[destination] = rtt

    ctx.obj["inspect"].callback = active_callback

    active_time = time.monotonic()
    ret = ctx.obj["inspect"].active()

    if not ret:
        logger.info("No response in %f seconds", time.monotonic() - active_time)
        ctx.exit(1)

    def pretty_task_arguments(task: Dict[str, Any]) -> str:
        arg_list = []
        for arg in task["args"]:
            arg_list.append(repr(arg))
        for k, v in task["kwargs"].items():
            arg_list.append(f"{k}={v!r}")

        return f'{task["name"]}({", ".join(arg_list)})'

    def get_task_data(worker: str, task: Dict[str, Any]) -> Dict[str, Any]:
        duration = time.time() - task["time_start"]
        return {
            "worker": worker,
            "name": task["name"],
            "args": literal_eval(task["args"]),
            "kwargs": literal_eval(task["kwargs"]),
            "duration": duration,
            "worker_pid": task["worker_pid"],
        }

    if format == "csv":
        writer = csv.DictWriter(
            sys.stdout, ["worker", "name", "args", "kwargs", "duration", "worker_pid"]
        )
        writer.writeheader()

        def output(data: Dict[str, Any]):
            writer.writerow(data)

    elif format == "pretty":

        def output(data: Dict[str, Any]):
            print(
                f"{data['worker']}: {pretty_task_arguments(data)} "
                f"[for {data['duration']:f}s, pid={data['worker_pid']}]"
            )

    else:
        logger.error("Unknown format %s", format)
        ctx.exit(127)

    for worker, active in sorted(ret.items()):
        if not active:
            logger.info("%s: no active tasks", worker)
            continue

        for task in sorted(active, key=lambda t: t["time_start"]):
            output(get_task_data(worker, task))

    ctx.exit(0)
