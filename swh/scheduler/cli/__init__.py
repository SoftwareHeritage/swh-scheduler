# Copyright (C) 2016-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

import click

from swh.core.cli import CONTEXT_SETTINGS, AliasedGroup


@click.group(name='scheduler', context_settings=CONTEXT_SETTINGS,
             cls=AliasedGroup)
@click.option('--config-file', '-C', default=None,
              type=click.Path(exists=True, dir_okay=False,),
              help="Configuration file.")
@click.option('--database', '-d', default=None,
              help="Scheduling database DSN (imply cls is 'local')")
@click.option('--url', '-u', default=None,
              help="Scheduler's url access (imply cls is 'remote')")
@click.option('--no-stdout', is_flag=True, default=False,
              help="Do NOT output logs on the console")
@click.pass_context
def cli(ctx, config_file, database, url, no_stdout):
    """Software Heritage Scheduler tools.

    Use a local scheduler instance by default (plugged to the
    main scheduler db).
    """
    from swh.core import config
    from swh.scheduler.celery_backend.config import setup_log_handler
    from swh.scheduler import get_scheduler, DEFAULT_CONFIG

    ctx.ensure_object(dict)
    log_level = ctx.obj.get('log_level', logging.INFO)

    setup_log_handler(
        loglevel=log_level, colorize=False,
        format='[%(levelname)s] %(name)s -- %(message)s',
        log_console=not no_stdout)

    logger = logging.getLogger(__name__)
    scheduler = None
    conf = config.read(config_file, DEFAULT_CONFIG)
    if 'scheduler' not in conf:
        raise ValueError("missing 'scheduler' configuration")

    if database:
        conf['scheduler']['cls'] = 'local'
        conf['scheduler']['args']['db'] = database
    elif url:
        conf['scheduler']['cls'] = 'remote'
        conf['scheduler']['args'] = {'url': url}
    sched_conf = conf['scheduler']
    try:
        logger.debug('Instanciating scheduler with %s' % (
            sched_conf))
        scheduler = get_scheduler(**sched_conf)
    except ValueError:
        # it's the subcommand to decide whether not having a proper
        # scheduler instance is a problem.
        pass

    ctx.obj['scheduler'] = scheduler
    ctx.obj['config'] = conf

from . import admin, task, task_type  # noqa


def main():
    import click.core
    click.core.DEPRECATED_HELP_NOTICE = '''

DEPRECATED! Please use the command 'swh scheduler'.'''
    cli.deprecated = True
    return cli(auto_envvar_prefix='SWH_SCHEDULER')


if __name__ == '__main__':
    main()
