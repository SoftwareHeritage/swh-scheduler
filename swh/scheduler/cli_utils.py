# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import yaml


def parse_argument(option):
    try:
        return yaml.load(option)
    except Exception:
        raise click.ClickException('Invalid argument: {}'.format(option))


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
    kw_pairs = [x.split('=', 1) for x in options if '=' in x]
    args = [parse_argument(x) for x in options if '=' not in x]
    kw = {k: parse_argument(v) for (k, v) in kw_pairs}
    return (args, kw)
