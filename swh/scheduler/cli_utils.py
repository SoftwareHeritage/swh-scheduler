# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


def parse_options(options):
    """Parses options from a CLI and turns it into Python args and kwargs.

    >>> parse_options([])
    ([], {})
    >>> parse_options(['foo', 'bar'])
    (['foo', 'bar'], {})
    >>> parse_options(['foo=bar'])
    ([], {'foo': 'bar'})
    >>> parse_options(['foo', 'bar=baz'])
    (['foo'], {'bar': 'baz'})
    """
    args = [x for x in options if '=' not in x]
    kw = dict(x.split('=', 1) for x in options if '=' in x)
    return (args, kw)
