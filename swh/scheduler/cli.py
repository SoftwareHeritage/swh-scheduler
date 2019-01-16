# Copyright (C) 2016-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import arrow
import click
import csv
import itertools
import json
import locale
import logging
import time

from swh.core import utils, config
from . import compute_nb_tasks_from
from .backend_es import SWHElasticSearchClient
from . import get_scheduler


locale.setlocale(locale.LC_ALL, '')
ARROW_LOCALE = locale.getlocale(locale.LC_TIME)[0]


class DateTimeType(click.ParamType):
    name = 'time and date'

    def convert(self, value, param, ctx):
        if not isinstance(value, arrow.Arrow):
            value = arrow.get(value)

        return value


DATETIME = DateTimeType()
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


def pretty_print_list(list, indent):
    """Pretty-print a list"""
    return ''.join('%s%s\n' % (' ' * indent, item) for item in list)


def pretty_print_dict(dict, indent):
    """Pretty-print a list"""
    return ''.join('%s%s: %s\n' %
                   (' ' * indent, click.style(key, bold=True), value)
                   for key, value in dict.items())


def pretty_print_task(task, full=False):
    """Pretty-print a task

    If 'full' is True, also print the status and priority fields.
    """
    next_run = arrow.get(task['next_run'])
    lines = [
        '%s %s\n' % (click.style('Task', bold=True), task['id']),
        click.style('  Next run: ', bold=True),
        "%s (%s)" % (next_run.humanize(locale=ARROW_LOCALE),
                     next_run.format()),
        '\n',
        click.style('  Interval: ', bold=True),
        str(task['current_interval']), '\n',
        click.style('  Type: ', bold=True), task['type'] or '', '\n',
        click.style('  Policy: ', bold=True), task['policy'] or '', '\n',
        ]
    if full:
        lines += [
            click.style('  Status: ', bold=True),
            task['status'] or '', '\n',
            click.style('  Priority: ', bold=True),
            task['priority'] or '', '\n',
        ]
    lines += [
        click.style('  Args:\n', bold=True),
        pretty_print_list(task['arguments']['args'], indent=4),
        click.style('  Keyword args:\n', bold=True),
        pretty_print_dict(task['arguments']['kwargs'], indent=4),
    ]

    return ''.join(lines)


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--cls', '-c', default='local',
              type=click.Choice(['local', 'remote']),
              help="Scheduler's class, default to 'local'")
@click.option('--database', '-d',
              help="Scheduling database DSN (if cls is 'local')")
@click.option('--url', '-u',
              help="Scheduler's url access (if cls is 'remote')")
@click.option('--log-level', '-l', default='INFO',
              type=click.Choice(logging._nameToLevel.keys()),
              help="Log level (default to INFO)")
@click.pass_context
def cli(ctx, cls, database, url, log_level):
    """Software Heritage Scheduler CLI interface

    Default to use the the local scheduler instance (plugged to the
    main scheduler db).

    """
    from swh.scheduler.celery_backend.config import setup_log_handler
    log_level = setup_log_handler(
        loglevel=log_level, colorize=False,
        format='[%(levelname)s] %(name)s -- %(message)s')

    ctx.ensure_object(dict)

    logger = logging.getLogger(__name__)
    scheduler = None
    override_config = {}
    try:
        if cls == 'local' and database:
            override_config = {'scheduling_db': database}
        elif cls == 'remote' and url:
            override_config = {'url': url}
        logger.debug('Instanciating scheduler %s with %s' % (
            cls, override_config))
        scheduler = get_scheduler(cls, args=override_config)
    except Exception:
        # it's the subcommand to decide whether not having a proper
        # scheduler instance is a problem.
        pass

    ctx.obj['scheduler'] = scheduler
    ctx.obj['config'] = {'cls': cls, 'args': override_config}
    ctx.obj['loglevel'] = log_level


@cli.group('task')
@click.pass_context
def task(ctx):
    """Manipulate tasks."""
    pass


@task.command('schedule')
@click.option('--columns', '-c', multiple=True,
              default=['type', 'args', 'kwargs', 'next_run'],
              type=click.Choice([
                  'type', 'args', 'kwargs', 'policy', 'next_run']),
              help='columns present in the CSV file')
@click.option('--delimiter', '-d', default=',')
@click.argument('file', type=click.File(encoding='utf-8'))
@click.pass_context
def schedule_tasks(ctx, columns, delimiter, file):
    """Schedule tasks from a CSV input file.

    The following columns are expected, and can be set through the -c option:

     - type: the type of the task to be scheduled (mandatory)

     - args: the arguments passed to the task (JSON list, defaults to an empty
       list)

     - kwargs: the keyword arguments passed to the task (JSON object, defaults
       to an empty dict)

     - next_run: the date at which the task should run (datetime, defaults to
       now)

    The CSV can be read either from a named file, or from stdin (use - as
    filename).

    Use sample:

    cat scheduling-task.txt | \
        python3 -m swh.scheduler.cli \
            --database 'service=swh-scheduler-dev' \
            task schedule \
                --columns type --columns kwargs --columns policy \
                --delimiter ';' -

    """
    tasks = []
    now = arrow.utcnow()
    scheduler = ctx.obj['scheduler']
    if not scheduler:
        raise ValueError('Scheduler class (local/remote) must be instantiated')

    reader = csv.reader(file, delimiter=delimiter)
    for line in reader:
        task = dict(zip(columns, line))
        args = json.loads(task.pop('args', '[]'))
        kwargs = json.loads(task.pop('kwargs', '{}'))
        task['arguments'] = {
            'args': args,
            'kwargs': kwargs,
        }
        task['next_run'] = DATETIME.convert(task.get('next_run', now),
                                            None, None)
        tasks.append(task)

    created = scheduler.create_tasks(tasks)

    output = [
        'Created %d tasks\n' % len(created),
    ]
    for task in created:
        output.append(pretty_print_task(task))

    click.echo_via_pager('\n'.join(output))


@task.command('add')
@click.argument('type', nargs=1, required=True)
@click.argument('options', nargs=-1)
@click.option('--policy', '-p', default='recurring',
              type=click.Choice(['recurring', 'oneshot']))
@click.option('--priority', '-P', default=None,
              type=click.Choice(['low', 'normal', 'high']))
@click.option('--next-run', '-n', default=None)
@click.pass_context
def schedule_task(ctx, type, options, policy, priority, next_run):
    """Schedule one task from arguments.

    Usage sample:

    swh-scheduler --database 'service=swh-scheduler' \
        task add swh-lister-pypi

    swh-scheduler --database 'service=swh-scheduler' \
        task add swh-lister-debian --policy=oneshot distribution=stretch

    Note: if the priority is not given, the task won't have the priority set,
    which is considered as the lowest priority level.
    """
    scheduler = ctx.obj['scheduler']
    if not scheduler:
        raise ValueError('Scheduler class (local/remote) must be instantiated')

    now = arrow.utcnow()

    args = [x for x in options if '=' not in x]
    kw = dict(x.split('=', 1) for x in options if '=' in x)
    task = {'type': type,
            'policy': policy,
            'priority': priority,
            'arguments': {
                'args': args,
                'kwargs': kw,
                },
            'next_run': DATETIME.convert(next_run or now,
                                         None, None),
            }
    created = scheduler.create_tasks([task])

    output = [
        'Created %d tasks\n' % len(created),
    ]
    for task in created:
        output.append(pretty_print_task(task))

    click.echo('\n'.join(output))


@task.command('list-pending')
@click.argument('task-types', required=True, nargs=-1)
@click.option('--limit', '-l', required=False, type=click.INT,
              help='The maximum number of tasks to fetch')
@click.option('--before', '-b', required=False, type=DATETIME,
              help='List all jobs supposed to run before the given date')
@click.pass_context
def list_pending_tasks(ctx, task_types, limit, before):
    """List the tasks that are going to be run.

    You can override the number of tasks to fetch

    """
    scheduler = ctx.obj['scheduler']
    if not scheduler:
        raise ValueError('Scheduler class (local/remote) must be instantiated')

    num_tasks, num_tasks_priority = compute_nb_tasks_from(limit)

    output = []
    for task_type in task_types:
        pending = scheduler.peek_ready_tasks(
            task_type, timestamp=before,
            num_tasks=num_tasks, num_tasks_priority=num_tasks_priority)
        output.append('Found %d %s tasks\n' % (
            len(pending), task_type))

        for task in pending:
            output.append(pretty_print_task(task))

    click.echo('\n'.join(output))


@task.command('list')
@click.option('--task-id', '-i', default=None, multiple=True, metavar='ID',
              help='List only tasks whose id is ID.')
@click.option('--task-type', '-t', default=None, multiple=True, metavar='TYPE',
              help='List only tasks of type TYPE')
@click.option('--limit', '-l', required=False, type=click.INT,
              help='The maximum number of tasks to fetch.')
@click.option('--status', '-s', multiple=True, metavar='STATUS',
              default=None,
              help='List tasks whose status is STATUS.')
@click.option('--policy', '-p', default=None,
              type=click.Choice(['recurring', 'oneshot']),
              help='List tasks whose policy is POLICY.')
@click.option('--priority', '-P', default=None, multiple=True,
              type=click.Choice(['all', 'low', 'normal', 'high']),
              help='List tasks whose priority is PRIORITY.')
@click.option('--before', '-b', required=False, type=DATETIME,
              metavar='DATETIME',
              help='Limit to tasks supposed to run before the given date.')
@click.option('--after', '-a', required=False, type=DATETIME,
              metavar='DATETIME',
              help='Limit to tasks supposed to run after the given date.')
@click.pass_context
def list_tasks(ctx, task_id, task_type, limit, status, policy, priority,
               before, after):
    """List tasks.
    """
    scheduler = ctx.obj['scheduler']
    if not scheduler:
        raise ValueError('Scheduler class (local/remote) must be instantiated')

    if not task_type:
        task_type = [x['type'] for x in scheduler.get_task_types()]

    # if task_id is not given, default value for status is
    #  'next_run_not_scheduled'
    # if task_id is given, default status is 'all'
    if task_id is None and status is None:
        status = ['next_run_not_scheduled']
    if status and 'all' in status:
        status = None

    if priority and 'all' in priority:
        priority = None

    output = []
    tasks = scheduler.search_tasks(
        task_id=task_id,
        task_type=task_type,
        status=status, priority=priority, policy=policy,
        before=before, after=after,
        limit=limit)

    output.append('Found %d tasks\n' % (
        len(tasks)))
    for task in tasks:
        output.append(pretty_print_task(task, full=True))

    click.echo('\n'.join(output))


@task.command('respawn')
@click.argument('task-ids', required=True, nargs=-1)
@click.option('--next-run', '-n', required=False, type=DATETIME,
              metavar='DATETIME', default=None,
              help='Re spawn the selected tasks at this date')
@click.pass_context
def respawn_tasks(ctx, task_ids, next_run):
    """Respawn tasks.

    Respawn tasks given by their ids (see the 'task list' command to
    find task ids) at the given date (immediately by default).

    Eg.

       swh-scheduler task respawn 1 3 12
    """
    scheduler = ctx.obj['scheduler']
    if not scheduler:
        raise ValueError('Scheduler class (local/remote) must be instantiated')
    if next_run is None:
        next_run = arrow.utcnow()
    output = []

    scheduler.set_status_tasks(
        task_ids, status='next_run_not_scheduled', next_run=next_run)
    output.append('Respawn tasks %s\n' % (
        task_ids))

    click.echo('\n'.join(output))


@task.command('archive')
@click.option('--before', '-b', default=None,
              help='''Task whose ended date is anterior will be archived.
                      Default to current month's first day.''')
@click.option('--after', '-a', default=None,
              help='''Task whose ended date is after the specified date will
                      be archived. Default to prior month's first day.''')
@click.option('--batch-index', default=1000, type=click.INT,
              help='Batch size of tasks to read from db to archive')
@click.option('--bulk-index', default=200, type=click.INT,
              help='Batch size of tasks to bulk index')
@click.option('--batch-clean', default=1000, type=click.INT,
              help='Batch size of task to clean after archival')
@click.option('--dry-run/--no-dry-run', is_flag=True, default=False,
              help='Default to list only what would be archived.')
@click.option('--verbose', is_flag=True, default=False,
              help='Default to list only what would be archived.')
@click.option('--cleanup/--no-cleanup', is_flag=True, default=True,
              help='Clean up archived tasks (default)')
@click.option('--start-from', type=click.INT, default=-1,
              help='(Optional) default task id to start from. Default is -1.')
@click.pass_context
def archive_tasks(ctx, before, after, batch_index, bulk_index, batch_clean,
                  dry_run, verbose, cleanup, start_from):
    """Archive task/task_run whose (task_type is 'oneshot' and task_status
       is 'completed') or (task_type is 'recurring' and task_status is
       'disabled').

       With --dry-run flag set (default), only list those.

    """
    scheduler = ctx.obj['scheduler']
    if not scheduler:
        raise ValueError('Scheduler class (local/remote) must be instantiated')

    es_client = SWHElasticSearchClient()
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO)
    log = logging.getLogger('swh.scheduler.cli.archive')
    logging.getLogger('urllib3').setLevel(logging.WARN)
    logging.getLogger('elasticsearch').setLevel(logging.WARN)
    if dry_run:
        log.info('**DRY-RUN** (only reading db)')
    if not cleanup:
        log.info('**NO CLEANUP**')

    now = arrow.utcnow()

    # Default to archive tasks from a rolling month starting the week
    # prior to the current one
    if not before:
        before = now.shift(weeks=-1).format('YYYY-MM-DD')

    if not after:
        after = now.shift(weeks=-1).shift(months=-1).format('YYYY-MM-DD')

    log.debug('index: %s; cleanup: %s; period: [%s ; %s]' % (
        not dry_run, not dry_run and cleanup, after, before))

    def group_by_index_name(data, es_client=es_client):
        """Given a data record, determine the index's name through its ending
           date. This varies greatly depending on the task_run's
           status.

        """
        date = data.get('started')
        if not date:
            date = data['scheduled']
        return es_client.compute_index_name(date.year, date.month)

    def index_data(before, last_id, batch_index):
        tasks_in = scheduler.filter_task_to_archive(
            after, before, last_id=last_id, limit=batch_index)
        for index_name, tasks_group in itertools.groupby(
                tasks_in, key=group_by_index_name):
            log.debug('Index tasks to %s' % index_name)
            if dry_run:
                for task in tasks_group:
                    yield task
                continue

            yield from es_client.streaming_bulk(
                index_name, tasks_group, source=['task_id', 'task_run_id'],
                chunk_size=bulk_index, log=log)

    gen = index_data(before, last_id=start_from, batch_index=batch_index)
    if cleanup:
        for task_ids in utils.grouper(gen, n=batch_clean):
            task_ids = list(task_ids)
            log.info('Clean up %s tasks: [%s, ...]' % (
                len(task_ids), task_ids[0]))
            if dry_run:  # no clean up
                continue
            ctx.obj['scheduler'].delete_archived_tasks(task_ids)
    else:
        for task_ids in utils.grouper(gen, n=batch_index):
            task_ids = list(task_ids)
            log.info('Indexed %s tasks: [%s, ...]' % (
                len(task_ids), task_ids[0]))


@cli.command('runner')
@click.option('--period', '-p', default=0,
              help=('Period (in s) at witch pending tasks are checked and '
                    'executed. Set to 0 (default) for a one shot.'))
@click.pass_context
def runner(ctx, period):
    """Starts a swh-scheduler runner service.

    This process is responsible for checking for ready-to-run tasks and
    schedule them."""
    from swh.scheduler.celery_backend.runner import run_ready_tasks
    from swh.scheduler.celery_backend.config import app

    logger = logging.getLogger(__name__ + '.runner')
    scheduler = ctx.obj['scheduler']
    logger.debug('Scheduler %s' % scheduler)
    try:
        while True:
            logger.info('Run ready tasks')
            try:
                run_ready_tasks(scheduler, app)
            except Exception:
                scheduler.rollback()
                logger.exception('Unexpected error in run_ready_tasks()')
            if not period:
                break
            time.sleep(period)
    except KeyboardInterrupt:
        ctx.exit(0)


@cli.command('listener')
@click.pass_context
def listener(ctx):
    """Starts a swh-scheduler listener service.

    This service is responsible for listening at task lifecycle events and
    handle their workflow status in the database."""
    scheduler = ctx.obj['scheduler']
    if not scheduler:
        raise ValueError('Scheduler class (local/remote) must be instantiated')

    from swh.scheduler.celery_backend.listener import (
        event_monitor, main_app)
    event_monitor(main_app, backend=scheduler)


@cli.command('api-server')
@click.argument('config-path', required=1)
@click.option('--host', default='0.0.0.0',
              help="Host to run the scheduler server api")
@click.option('--port', default=5008, type=click.INT,
              help="Binding port of the server")
@click.option('--debug/--nodebug', default=None,
              help=("Indicates if the server should run in debug mode. "
                    "Defaults to True if log-level is DEBUG, False otherwise.")
              )
@click.pass_context
def api_server(ctx, config_path, host, port, debug):
    """Starts a swh-scheduler API HTTP server.
    """
    if ctx.obj['config']['cls'] == 'remote':
        click.echo("The API server can only be started with a 'local' "
                   "configuration", err=True)
        ctx.exit(1)

    from swh.scheduler.api.server import app, DEFAULT_CONFIG
    conf = config.read(config_path, DEFAULT_CONFIG)
    if ctx.obj['config']['args']:
        conf['scheduler']['args'].update(ctx.obj['config']['args'])
    app.config.update(conf)
    if debug is None:
        debug = ctx.obj['loglevel'] <= logging.DEBUG

    app.run(host, port=port, debug=bool(debug))


@cli.group('task-type')
@click.pass_context
def task_type(ctx):
    """Manipulate task types."""
    pass


@task_type.command('list')
@click.option('--verbose', '-v', is_flag=True, default=False)
@click.option('--task_type', '-t', multiple=True, default=None,
              help='List task types of given type')
@click.option('--task_name', '-n', multiple=True, default=None,
              help='List task types of given backend task name')
@click.pass_context
def list_task_types(ctx, verbose, task_type, task_name):
    click.echo("Known task types:")
    if verbose:
        tmpl = click.style('{type}: ', bold=True) + '''{backend_name}
  {description}
  interval: {default_interval} [{min_interval}, {max_interval}]
  backoff_factor: {backoff_factor}
  max_queue_length: {max_queue_length}
  num_retries: {num_retries}
  retry_delay: {retry_delay}
'''
    else:
        tmpl = '{type}:\n  {description}'
    for tasktype in ctx.obj['scheduler'].get_task_types():
        if task_type and tasktype['type'] not in task_type:
            continue
        if task_name and tasktype['backend_name'] not in task_name:
            continue
        click.echo(tmpl.format(**tasktype))


@task_type.command('add')
@click.argument('type', required=1)
@click.argument('task-name', required=1)
@click.argument('description', required=1)
@click.option('--default-interval', '-i', default='90 days',
              help='Default interval ("90 days" by default)')
@click.option('--min-interval', default=None,
              help='Minimum interval (default interval if not set)')
@click.option('--max-interval', '-i', default=None,
              help='Maximal interval (default interval if not set)')
@click.option('--backoff-factor', '-f', type=float, default=1,
              help='Backoff factor')
@click.pass_context
def add_task_type(ctx, type, task_name, description,
                  default_interval, min_interval, max_interval,
                  backoff_factor):
    """Create a new task type
    """
    scheduler = ctx.obj['scheduler']
    if not scheduler:
        raise ValueError('Scheduler class (local/remote) must be instantiated')
    task_type = dict(
        type=type,
        backend_name=task_name,
        description=description,
        default_interval=default_interval,
        min_interval=min_interval,
        max_interval=max_interval,
        backoff_factor=backoff_factor,
        max_queue_length=None,
        num_retries=None,
        retry_delay=None,
        )
    scheduler.create_task_type(task_type)
    click.echo('OK')


if __name__ == '__main__':
    cli()
