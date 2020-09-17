# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# WARNING: do not import unnecessary things here to keep cli startup time under
# control

import click

TASK_BATCH_SIZE = 1000  # Number of tasks per query to the scheduler


def schedule_origin_batches(scheduler, task_type, origins, origin_batch_size, kwargs):
    from itertools import islice

    from swh.scheduler.utils import create_task_dict

    nb_origins = 0
    nb_tasks = 0

    while True:
        task_batch = []
        for _ in range(TASK_BATCH_SIZE):
            # Group origins
            origin_batch = []
            for origin in islice(origins, origin_batch_size):
                origin_batch.append(origin)
            nb_origins += len(origin_batch)
            if not origin_batch:
                break

            # Create a task for these origins
            args = [origin_batch]
            task_dict = create_task_dict(task_type, "oneshot", *args, **kwargs)
            task_batch.append(task_dict)

        # Schedule a batch of tasks
        if not task_batch:
            break
        nb_tasks += len(task_batch)
        if scheduler:
            scheduler.create_tasks(task_batch)
        click.echo("Scheduled %d tasks (%d origins)." % (nb_tasks, nb_origins))

    # Print final status.
    if nb_tasks:
        click.echo("Done.")
    else:
        click.echo("Nothing to do (no origin metadata matched the criteria).")


def parse_argument(option):
    import yaml

    try:
        return yaml.safe_load(option)
    except Exception:
        raise click.ClickException("Invalid argument: {}".format(option))


def parse_options(options):
    """Parses options from a CLI as YAML and turns it into Python
    args and kwargs.

    >>> parse_options([])
    ([], {})
    >>> parse_options(['foo', 'bar'])
    (['foo', 'bar'], {})
    >>> parse_options(['[foo, bar]'])
    ([['foo', 'bar']], {})
    >>> parse_options(['"foo"', '"bar"'])
    (['foo', 'bar'], {})
    >>> parse_options(['foo="bar"'])
    ([], {'foo': 'bar'})
    >>> parse_options(['"foo"', 'bar="baz"'])
    (['foo'], {'bar': 'baz'})
    >>> parse_options(['42', 'bar=False'])
    ([42], {'bar': False})
    >>> parse_options(['42', 'bar=false'])
    ([42], {'bar': False})
    >>> parse_options(['42', '"foo'])
    Traceback (most recent call last):
      ...
    click.exceptions.ClickException: Invalid argument: "foo
    """
    kw_pairs = [x.split("=", 1) for x in options if "=" in x]
    args = [parse_argument(x) for x in options if "=" not in x]
    kw = {k: parse_argument(v) for (k, v) in kw_pairs}
    return (args, kw)
