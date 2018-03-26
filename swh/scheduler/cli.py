# Copyright (C) 2016-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import arrow
import click
import csv
import json
import locale
import logging

from swh.core import utils
from .backend import SchedulerBackend
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
        click.style('  Args:\n', bold=True),
        pretty_print_list(task['arguments']['args'], indent=4),
        click.style('  Keyword args:\n', bold=True),
        pretty_print_dict(task['arguments']['kwargs'], indent=4),
    ]

    return ''.join(lines)


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option(
    '--database', '-d', help='Scheduling database DSN',
    default='host=db.internal.softwareheritage.org '
            'dbname=softwareheritage-scheduler user=guest')
@click.pass_context
def cli(ctx, database):
    """Software Heritage Scheduler CLI interface"""
    override_config = {}
    if database:
        override_config['scheduling_db'] = database

    ctx.obj = SchedulerBackend(**override_config)


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
    pending = ctx.obj.peek_ready_tasks(task_type,
                                       timestamp=before, num_tasks=limit)
    output = [
        'Found %d tasks\n' % len(pending)
    ]
    for task in pending:
        output.append(pretty_print_task(task))

    click.echo_via_pager('\n'.join(output))


@task.command('archive')
@click.option('--before', '-b', default='2016-02-22',
              help='Task whose ended date is anterior will be archived.')
@click.option('--batch-index', default=1000, type=click.INT,
              help='Batch size of tasks to archive')
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
def archive_tasks(ctx, before, batch_index, batch_clean,
                  dry_run, verbose, cleanup, start_from):
    """Archive task/task_run whose (task_type is 'oneshot' and task_status
       is 'completed') or (task_type is 'recurring' and task_status is
       'disabled').

       With --dry-run flag set (default), only list those.

    """
    es = SWHElasticSearchClient()
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO)
    log = logging.getLogger('swh.scheduler.cli.archive')
    logging.getLogger('urllib3').setLevel(logging.WARN)
    logging.getLogger('elasticsearch').setLevel(logging.WARN)

    def index_data(before, last_id, batch_index, backend=ctx.obj):
        for task in backend.filter_task_to_archive(
                before, last_id=last_id, limit=batch_index):
            log.debug('task: %s' % task)
            if dry_run:
                yield task
                continue
            result = es.index(task)
            if not result:
                log.error('Error during indexation: %s, skipping', result)
                continue
            log.debug('result: %s' % result)
            yield task

    gen = index_data(before, last_id=start_from, batch_index=batch_index)
    if cleanup:
        for task_ids in utils.grouper(gen, n=batch_clean):
            _task_ids = [t['task_id'] for t in task_ids]
            log.debug('Cleanup %s tasks' % (len(_task_ids, )))
            if dry_run:
                continue
            ctx.obj.delete_archive_tasks(_task_ids)
    else:
        for task_id in gen:
            log.info('Indexing: %s' % task_id)


@cli.group('task-run')
@click.pass_context
def task_run(ctx):
    """Manipulate task runs."""
    pass


if __name__ == '__main__':
    cli()
