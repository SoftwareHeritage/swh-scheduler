# Copyright (C) 2016-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click

from . import cli


@cli.group('task-type')
@click.pass_context
def task_type(ctx):
    """Manipulate task types."""
    pass


@task_type.command('list')
@click.option('--verbose', '-v', is_flag=True, default=False,
              help='Verbose mode')
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
    for tasktype in sorted(ctx.obj['scheduler'].get_task_types(),
                           key=lambda x: x['type']):
        if task_type and tasktype['type'] not in task_type:
            continue
        if task_name and tasktype['backend_name'] not in task_name:
            continue
        click.echo(tmpl.format(**tasktype))


@task_type.command('add')
@click.argument('type', required=True)
@click.argument('task-name', required=True)
@click.argument('description', required=True)
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
