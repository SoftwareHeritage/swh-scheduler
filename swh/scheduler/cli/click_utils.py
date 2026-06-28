# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Optional

import click
from dateparser import parse as dateparser
from dateutil.parser import ParserError as DateUtilParserError
from dateutil.parser import parse as dateutil


class DateConvert:
    def __init__(self, name: str, converter: Callable[[str], Optional[datetime]]):
        self.convert = converter
        self.name = name

    def __repr__(self) -> str:
        return self.name


class DateTimeMoreParsers(click.types.DateTime):
    def __init__(self):
        super().__init__()
        self.formats += [
            DateConvert("ISO 8601", datetime.fromisoformat),
            DateConvert("RFC 822/2822/5322", parsedate_to_datetime),
            DateConvert("dateutil", lambda value: dateutil(value, fuzzy=True)),
            DateConvert("dateparser", dateparser),
        ]

    def _try_to_convert_date(self, value: Any, parser: Any) -> Optional[datetime]:
        if isinstance(parser, str):
            return super()._try_to_convert_date(value, parser)
        elif isinstance(parser, DateConvert):
            try:
                return parser.convert(value)
            except (ValueError, DateUtilParserError):
                return None
        elif parser is not None:
            raise ValueError(f"Unknown date parser {parser}")
        else:
            return None


DATETIME = DateTimeMoreParsers()

if __name__ == "__main__":
    import sys

    for arg in sys.argv[1:]:
        print(DATETIME.convert(arg, None, None))
