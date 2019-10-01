# Copyright (C) 2016-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import time

import click

from . import cli


@cli.command('start-runner')
@click.option('--period', '-p', default=0,
              help=('Period (in s) at witch pending tasks are checked and '
                    'executed. Set to 0 (default) for a one shot.'))
@click.pass_context
def runner(ctx, period):
    """Starts a swh-scheduler runner service.

    This process is responsible for checking for ready-to-run tasks and
    schedule them."""
    from swh.scheduler.celery_backend.runner import run_ready_tasks
    from swh.scheduler.celery_backend.config import build_app

    app = build_app(ctx.obj['config'].get('celery'))
    app.set_current()

    logger = logging.getLogger(__name__ + '.runner')
    scheduler = ctx.obj['scheduler']
    logger.debug('Scheduler %s' % scheduler)
    try:
        while True:
            logger.debug('Run ready tasks')
            try:
                ntasks = len(run_ready_tasks(scheduler, app))
                if ntasks:
                    logger.info('Scheduled %s tasks', ntasks)
            except Exception:
                logger.exception('Unexpected error in run_ready_tasks()')
            if not period:
                break
            time.sleep(period)
    except KeyboardInterrupt:
        ctx.exit(0)


@cli.command('start-listener')
@click.pass_context
def listener(ctx):
    """Starts a swh-scheduler listener service.

    This service is responsible for listening at task lifecycle events and
    handle their workflow status in the database."""
    scheduler = ctx.obj['scheduler']
    if not scheduler:
        raise ValueError('Scheduler class (local/remote) must be instantiated')

    from swh.scheduler.celery_backend.config import build_app
    app = build_app(ctx.obj['config'].get('celery'))
    app.set_current()

    from swh.scheduler.celery_backend.listener import event_monitor
    event_monitor(app, backend=scheduler)


@cli.command('rpc-serve')
@click.option('--host', default='0.0.0.0',
              help="Host to run the scheduler server api")
@click.option('--port', default=5008, type=click.INT,
              help="Binding port of the server")
@click.option('--debug/--nodebug', default=None,
              help=("Indicates if the server should run in debug mode. "
                    "Defaults to True if log-level is DEBUG, False otherwise.")
              )
@click.pass_context
def rpc_server(ctx, host, port, debug):
    """Starts a swh-scheduler API HTTP server.
    """
    if ctx.obj['config']['scheduler']['cls'] == 'remote':
        click.echo("The API server can only be started with a 'local' "
                   "configuration", err=True)
        ctx.exit(1)

    from swh.scheduler.api import server
    server.app.config.update(ctx.obj['config'])
    if debug is None:
        debug = ctx.obj['log_level'] <= logging.DEBUG
    server.app.run(host, port=port, debug=bool(debug))


@cli.command('start-updater')
@click.option('--verbose/--no-verbose', '-v', default=False,
              help='Verbose mode')
@click.pass_context
def updater(ctx, verbose):
    """Starts a scheduler-updater service.

    Insert tasks in the scheduler from the scheduler-updater's events read from
    the db cache (filled e.g. by the ghtorrent consumer service) .

    """
    from swh.scheduler.updater.writer import UpdaterWriter
    UpdaterWriter(**ctx.obj['config']).run()


@cli.command('start-ghtorrent')
@click.option('--verbose/--no-verbose', '-v', default=False,
              help='Verbose mode')
@click.pass_context
def ghtorrent(ctx, verbose):
    """Starts a ghtorrent consumer service.

    Consumes events from ghtorrent and write them to a cache.

    """
    from swh.scheduler.updater.ghtorrent import GHTorrentConsumer
    from swh.scheduler.updater.backend import SchedulerUpdaterBackend

    ght_config = ctx.obj['config'].get('ghtorrent', {})
    back_config = ctx.obj['config'].get('scheduler_updater', {})
    backend = SchedulerUpdaterBackend(**back_config)
    GHTorrentConsumer(backend, **ght_config).run()
