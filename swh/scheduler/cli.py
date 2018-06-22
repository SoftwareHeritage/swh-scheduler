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

from swh.core import utils
from . import compute_nb_tasks_from
from .backend_es import SWHElasticSearchClient


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


def pretty_print_task(task):
    """Pretty-print a task"""
    next_run = arrow.get(task['next_run'])
    lines = [
        '%s %s\n' % (click.style('Task', bold=True), task['id']),
        click.style('  Next run: ', bold=True),
        "%s (%s)" % (next_run.humanize(locale=ARROW_LOCALE),
                     next_run.format()),
        '\n',
        click.style('  Interval: ', bold=True),
        str(task['current_interval']), '\n',
        click.style('  Type: ', bold=True), task['type'], '\n',
        click.style('  Policy: ', bold=True), task['policy'], '\n',
        click.style('  Args:\n', bold=True),
        pretty_print_list(task['arguments']['args'], indent=4),
        click.style('  Keyword args:\n', bold=True),
        pretty_print_dict(task['arguments']['kwargs'], indent=4),
    ]

    return ''.join(lines)


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--cls', '-c', default='local',
              help="Scheduler's class, default to 'local'")
@click.option('--database', '-d',
              help='Scheduling database DSN')
@click.option('--url', '-u',
              help="(Optional) Scheduler's url access")
@click.pass_context
def cli(ctx, cls, database, url):
    """Software Heritage Scheduler CLI interface

    Default to use the the local scheduler instance (plugged to the
    main scheduler db).

    """
    scheduler = None
    override_config = {}
    from . import get_scheduler
    if cls == 'local':
        if database:
            override_config = {'scheduling_db': database}
        scheduler = get_scheduler(cls, args=override_config)
    elif cls == 'remote':
        if url:
            override_config = {'url': url}
        scheduler = get_scheduler(cls, args=override_config)

    if not scheduler:
        raise ValueError('Scheduler class (local/remote) must be instantiated')

    ctx.obj = scheduler


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

    created = ctx.obj.create_tasks(tasks)

    output = [
        'Created %d tasks\n' % len(created),
    ]
    for task in created:
        output.append(pretty_print_task(task))

    click.echo_via_pager('\n'.join(output))


@task.command('list-pending')
@click.option('--task-type', '-t', required=True,
              help='The tasks\' type concerned by the listing')
@click.option('--limit', '-l', required=False, type=click.INT,
              help='The maximum number of tasks to fetch')
@click.option('--before', '-b', required=False, type=DATETIME,
              help='List all jobs supposed to run before the given date')
@click.pass_context
def list_pending_tasks(ctx, task_type, limit, before):
    """List the tasks that are going to be run.

    You can override the number of tasks to fetch

    """
    num_tasks, num_tasks_priority = compute_nb_tasks_from(limit)

    pending = ctx.obj.peek_ready_tasks(
        task_type, timestamp=before,
        num_tasks=num_tasks, num_tasks_priority=num_tasks_priority)
    output = [
        'Found %d tasks\n' % len(pending)
    ]
    for task in pending:
        output.append(pretty_print_task(task))

    click.echo_via_pager('\n'.join(output))


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

    def index_data(before, last_id, batch_index, backend=ctx.obj):
        tasks_in = backend.filter_task_to_archive(
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
            ctx.obj.delete_archived_tasks(task_ids)
    else:
        for task_ids in utils.grouper(gen, n=batch_index):
            task_ids = list(task_ids)
            log.info('Indexed %s tasks: [%s, ...]' % (
                len(task_ids), task_ids[0]))


@cli.group('task-run')
@click.pass_context
def task_run(ctx):
    """Manipulate task runs."""
    pass


if __name__ == '__main__':
    cli()
