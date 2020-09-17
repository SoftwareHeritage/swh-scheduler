# Copyright (C) 2016-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import logging
import time

import click

from . import cli


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
@click.pass_context
def runner(ctx, period):
    """Starts a swh-scheduler runner service.

    This process is responsible for checking for ready-to-run tasks and
    schedule them."""
    from swh.scheduler.celery_backend.config import build_app
    from swh.scheduler.celery_backend.runner import run_ready_tasks

    app = build_app(ctx.obj["config"].get("celery"))
    app.set_current()

    logger = logging.getLogger(__name__ + ".runner")
    scheduler = ctx.obj["scheduler"]
    logger.debug("Scheduler %s" % scheduler)
    try:
        while True:
            logger.debug("Run ready tasks")
            try:
                ntasks = len(run_ready_tasks(scheduler, app))
                if ntasks:
                    logger.info("Scheduled %s tasks", ntasks)
            except Exception:
                logger.exception("Unexpected error in run_ready_tasks()")
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
    handle their workflow status in the database."""
    scheduler_backend = ctx.obj["scheduler"]
    if not scheduler_backend:
        raise ValueError("Scheduler class (local/remote) must be instantiated")

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
    """
    if ctx.obj["config"]["scheduler"]["cls"] == "remote":
        click.echo(
            "The API server can only be started with a 'local' " "configuration",
            err=True,
        )
        ctx.exit(1)

    from swh.scheduler.api import server

    server.app.config.update(ctx.obj["config"])
    if debug is None:
        debug = ctx.obj["log_level"] <= logging.DEBUG
    server.app.run(host, port=port, debug=bool(debug))
